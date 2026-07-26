package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type channelControlClient interface {
	daemonHealthClient
	CreateChannel(context.Context, localapi.ChannelCreateRequest) (localapi.ChannelCreateResponse, *localapi.APIError)
	JoinChannel(context.Context, localapi.ChannelJoinRequest) (localapi.ChannelJoinResponse, *localapi.APIError)
	CreateChannelInvite(context.Context, localapi.ChannelInviteRequest) (localapi.ChannelInviteResponse, *localapi.APIError)
	CloseChannelInvite(context.Context, localapi.ChannelInviteCloseRequest) (localapi.ChannelInviteCloseResponse, *localapi.APIError)
	RemoveChannelMember(context.Context, localapi.ChannelRemoveRequest) (localapi.ChannelRemoveResponse, *localapi.APIError)
	LeaveChannel(context.Context, localapi.ChannelLeaveRequest) (localapi.ChannelLeaveResponse, *localapi.APIError)
	AbandonChannel(context.Context, localapi.ChannelAbandonRequest) (localapi.ChannelAbandonResponse, *localapi.APIError)
	ReadChannelStatus(context.Context) (localapi.ChannelStatusResponse, *localapi.APIError)
}

type channelDependencies struct {
	workingDirectory func() (string, error)
	newClient        func(string) (channelControlClient, error)
	ensureDaemon     func(context.Context, string, string, daemonHealthClient) *localapi.APIError
}

type channelApp struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	deps   channelDependencies
}

func productionChannelDependencies() channelDependencies {
	return channelDependencies{workingDirectory: os.Getwd,
		newClient:    func(nodeState string) (channelControlClient, error) { return localapi.NewClient(nodeState) },
		ensureDaemon: ensureAgentDaemon}
}

func RunChannel(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer,
	_ string,
) int {
	app := &channelApp{stdin: stdin, stdout: stdout, stderr: stderr,
		deps: productionChannelDependencies()}
	return app.run(ctx, args)
}

func (app *channelApp) run(ctx context.Context, args []string) int {
	if app == nil || ctx == nil || app.stdin == nil || app.stdout == nil || app.stderr == nil ||
		app.deps.workingDirectory == nil || app.deps.newClient == nil || app.deps.ensureDaemon == nil {
		return 1
	}
	if len(args) == 0 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel requires create, join, status or invite"))
	}
	workspace, nodeState, err := resolveManagedWorkspace(app.deps.workingDirectory)
	if err != nil {
		return app.writeError(localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"Mnemon Harness is not set up in this workspace"))
	}
	client, err := app.deps.newClient(nodeState)
	if err != nil {
		code := localapi.CodeMnemondUnavailable
		if errors.Is(err, localapi.ErrUnsafeClientState) {
			code = localapi.CodeAuthenticationFailed
		}
		return app.writeError(localapi.NewAPIError(code, "mnemond local control is unavailable"))
	}
	if apiErr := app.deps.ensureDaemon(ctx, workspace, nodeState, client); apiErr != nil {
		return app.writeError(apiErr)
	}
	return app.dispatch(ctx, client, args[0], args[1:])
}

func (app *channelApp) dispatch(ctx context.Context, client channelControlClient,
	command string, args []string,
) int {
	switch command {
	case "create":
		return app.create(ctx, client, args)
	case "join":
		return app.join(ctx, client, args)
	case "status":
		return app.status(ctx, client, args)
	case "invite":
		return app.invite(ctx, client, args)
	case "remove":
		return app.remove(ctx, client, args)
	case "leave":
		return app.leave(ctx, client, args)
	case "abandon":
		return app.abandon(ctx, client, args)
	default:
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"unknown channel subcommand"))
	}
}

func (app *channelApp) abandon(ctx context.Context, client channelControlClient, args []string) int {
	args, jsonOutput := takeJSONFlag(args)
	flags := flag.NewFlagSet("channel abandon", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	force := flags.Bool("force", false, "confirm forensic abandon")
	confirmation := flags.String("confirm-channel", "", "exact local Channel alias")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || !*force || *confirmation == "" {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel abandon requires --force --confirm-channel <local-channel-alias>"))
	}
	response, apiErr := client.AbandonChannel(ctx, localapi.ChannelAbandonRequest{
		Channel: *confirmation, ConfirmChannel: *confirmation, Force: *force})
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	if jsonOutput {
		return app.writeJSON(response)
	}
	_, err := fmt.Fprintf(app.stdout, "Forensically abandoned Channel %s at %s\n",
		response.Channel, response.TransitionedAt)
	return writeExit(err)
}

func (app *channelApp) remove(ctx context.Context, client channelControlClient, args []string) int {
	args, jsonOutput := takeJSONFlag(args)
	flags := flag.NewFlagSet("channel remove", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	channel := flags.String("channel", "", "Channel alias")
	if err := flags.Parse(args); err != nil || flags.NArg() != 1 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel remove requires one member alias"))
	}
	response, apiErr := client.RemoveChannelMember(ctx, localapi.ChannelRemoveRequest{
		Channel: *channel, Member: flags.Arg(0)})
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	if jsonOutput {
		return app.writeJSON(response)
	}
	_, err := fmt.Fprintf(app.stdout, "Removed member from Channel %s\n", response.Channel.Alias)
	return writeExit(err)
}

func (app *channelApp) leave(ctx context.Context, client channelControlClient, args []string) int {
	args, jsonOutput := takeJSONFlag(args)
	if len(args) > 1 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel leave accepts one optional Channel alias"))
	}
	channel := ""
	if len(args) == 1 {
		channel = args[0]
	}
	response, apiErr := client.LeaveChannel(ctx, localapi.ChannelLeaveRequest{Channel: channel})
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	if jsonOutput {
		return app.writeJSON(response)
	}
	if response.Status == "leaving" {
		_, err := fmt.Fprintf(app.stdout, "Leaving Channel %s (owner acknowledgement queued)\n",
			response.Channel.Alias)
		return writeExit(err)
	}
	_, err := fmt.Fprintf(app.stdout, "Left Channel %s\n", response.Channel.Alias)
	return writeExit(err)
}

func (app *channelApp) create(ctx context.Context, client channelControlClient, args []string) int {
	args, jsonOutput := takeJSONFlag(args)
	if len(args) > 1 || len(args) == 1 && len(args[0]) > model.MaxLabelBytes {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel create accepts one optional name"))
	}
	name := ""
	if len(args) == 1 {
		name = args[0]
	}
	response, apiErr := client.CreateChannel(ctx, localapi.ChannelCreateRequest{Name: name})
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	if jsonOutput {
		return app.writeJSON(response)
	}
	_, err := fmt.Fprintf(app.stdout, "Created Channel %s (%s)\nInvite token (shown once):\n%s\n",
		response.Channel.Alias, response.Channel.Topic.Status, response.InviteToken)
	if err != nil {
		return 1
	}
	return 0
}

func (app *channelApp) join(ctx context.Context, client channelControlClient, args []string) int {
	args, jsonOutput := takeJSONFlag(args)
	flags := flag.NewFlagSet("channel join", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	filePath := flags.String("file", "", "invite file")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel join accepts only --file"))
	}
	token, err := readChannelJoinToken(app.stdin, app.stderr, *filePath)
	if err != nil {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidToken, err.Error()))
	}
	response, apiErr := client.JoinChannel(ctx, localapi.ChannelJoinRequest{Token: token})
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	if jsonOutput {
		return app.writeJSON(response)
	}
	switch response.Status {
	case "member_revoked":
		_, err = fmt.Fprintf(app.stdout, "Channel %s membership is terminal (member revoked)\n",
			response.Channel.Alias)
	case "channel_closed":
		_, err = fmt.Fprintf(app.stdout, "Channel %s is terminal (closed)\n",
			response.Channel.Alias)
	case "replayed":
		_, err = fmt.Fprintf(app.stdout, "Replayed Channel %s join (%s)\n",
			response.Channel.Alias, response.Channel.Topic.Status)
	default:
		_, err = fmt.Fprintf(app.stdout, "Joined Channel %s (%s)\n",
			response.Channel.Alias, response.Channel.Topic.Status)
	}
	if err != nil {
		return 1
	}
	return 0
}

func (app *channelApp) status(ctx context.Context, client channelControlClient, args []string) int {
	args, jsonOutput := takeJSONFlag(args)
	if len(args) > 1 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel status accepts one optional Channel alias"))
	}
	response, apiErr := client.ReadChannelStatus(ctx)
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	if len(args) == 1 {
		response.Channels = filterChannels(response.Channels, args[0])
		if len(response.Channels) == 0 {
			return app.writeError(localapi.NewAPIError(localapi.CodeNotMember,
				"Channel is not present on this Node"))
		}
	}
	if jsonOutput {
		return app.writeJSON(response)
	}
	for _, channel := range response.Channels {
		if _, err := fmt.Fprintf(app.stdout, "%s\t%s\t%s\t%s\n", channel.Alias,
			channel.Membership, channel.Topic.Status, channel.Name); err != nil {
			return 1
		}
	}
	return 0
}

func (app *channelApp) invite(ctx context.Context, client channelControlClient, args []string) int {
	args, jsonOutput := takeJSONFlag(args)
	flags := flag.NewFlagSet("channel invite", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	channel := flags.String("channel", "", "Channel alias")
	expires := flags.Duration("expires", time.Hour, "invite lifetime")
	uses := flags.Uint("uses", 0, "invite uses")
	closeInvite := flags.Bool("close", false, "close invite")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *uses > 7 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"channel invite options are invalid"))
	}
	if *closeInvite {
		response, apiErr := client.CloseChannelInvite(ctx,
			localapi.ChannelInviteCloseRequest{Channel: *channel})
		if apiErr != nil {
			return app.writeError(apiErr)
		}
		if jsonOutput {
			return app.writeJSON(response)
		}
		_, err := fmt.Fprintf(app.stdout, "Closed invite for Channel %s\n", response.Channel.Alias)
		return writeExit(err)
	}
	response, apiErr := client.CreateChannelInvite(ctx, localapi.ChannelInviteRequest{
		Channel: *channel, ExpiresSeconds: int64(*expires / time.Second), Uses: uint8(*uses)})
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	if jsonOutput {
		return app.writeJSON(response)
	}
	_, err := fmt.Fprintf(app.stdout, "Created invite for Channel %s (shown once):\n%s\n",
		response.Channel.Alias, response.InviteToken)
	return writeExit(err)
}

func takeJSONFlag(args []string) ([]string, bool) {
	result := make([]string, 0, len(args))
	jsonOutput := false
	for _, argument := range args {
		if argument == "--json" {
			jsonOutput = true
		} else {
			result = append(result, argument)
		}
	}
	return result, jsonOutput
}

func filterChannels(channels []localapi.ChannelView, alias string) []localapi.ChannelView {
	for _, channel := range channels {
		if channel.Alias == alias {
			return []localapi.ChannelView{channel}
		}
	}
	return []localapi.ChannelView{}
}

func (app *channelApp) writeJSON(value any) int {
	raw, err := model.CanonicalMarshal(value)
	if err != nil {
		return 1
	}
	_, err = app.stdout.Write(append(raw, '\n'))
	return writeExit(err)
}

func (app *channelApp) writeError(apiErr *localapi.APIError) int {
	if apiErr == nil {
		apiErr = localapi.NewAPIError(localapi.CodeInternal, "internal Channel error")
	}
	_, err := fmt.Fprintf(app.stderr, "%s: %s\n", apiErr.Code, apiErr.Message)
	if err != nil {
		return 1
	}
	return apiErr.ExitStatus()
}

func writeExit(err error) int {
	if err != nil {
		return 1
	}
	return 0
}
