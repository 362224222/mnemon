package githubbackend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	pathpkg "path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
)

const githubAPIVersion = "2022-11-28"

type PublicationStoreConfig struct {
	Repo          string
	Token         string
	BaseURL       string
	UserAgent     string
	HTTPClient    *http.Client
	MutativeDelay time.Duration
}

type GitHubPublicationStore struct {
	owner         string
	repo          string
	token         string
	baseURL       string
	userAgent     string
	client        *http.Client
	mutativeDelay time.Duration
	writeMu       sync.Mutex
	lastWrite     time.Time
}

func NewPublicationStore(cfg PublicationStoreConfig) (*GitHubPublicationStore, error) {
	repo, err := exchange.NormalizeGitHubRepo(cfg.Repo)
	if err != nil {
		return nil, err
	}
	owner, name, _ := strings.Cut(repo, "/")
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	userAgent := strings.TrimSpace(cfg.UserAgent)
	if userAgent == "" {
		userAgent = "mnemon-harness"
	}
	return &GitHubPublicationStore{
		owner:         owner,
		repo:          name,
		token:         strings.TrimSpace(cfg.Token),
		baseURL:       baseURL,
		userAgent:     userAgent,
		client:        client,
		mutativeDelay: cfg.MutativeDelay,
	}, nil
}

func (s *GitHubPublicationStore) PutEvent(ctx context.Context, branch string, path string, body []byte) (exchange.PublicationPutResult, error) {
	branch, path, err := normalizeGitHubPublicationEventRef(branch, path)
	if err != nil {
		return exchange.PublicationPutResult{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	existing, found, err := s.readFileWithSHA(ctx, branch, path)
	if err != nil {
		return exchange.PublicationPutResult{}, err
	}
	if found {
		if bytes.Equal(existing.body, body) {
			return exchange.PublicationPutResult{ExistsSame: true}, nil
		}
		return exchange.PublicationPutResult{Conflict: true}, nil
	}
	if err := s.pauseBeforeMutation(ctx); err != nil {
		return exchange.PublicationPutResult{}, err
	}
	if err := s.putFile(ctx, branch, path, body, ""); err != nil {
		if apiErr, ok := err.(*githubAPIError); ok && apiErr.Status == http.StatusConflict {
			return exchange.PublicationPutResult{Conflict: true}, nil
		}
		return exchange.PublicationPutResult{}, err
	}
	s.lastWrite = time.Now()
	return exchange.PublicationPutResult{Created: true}, nil
}

func (s *GitHubPublicationStore) ListEvents(ctx context.Context, branch string, prefix string, cursor string) (exchange.PublicationListResult, error) {
	branch, err := exchange.NormalizePublicationBranch(branch)
	if err != nil {
		return exchange.PublicationListResult{}, err
	}
	prefix, err = normalizeGitHubPublicationEventPrefix(prefix)
	if err != nil {
		return exchange.PublicationListResult{}, err
	}
	head, err := s.branchHead(ctx, branch)
	if err != nil {
		return exchange.PublicationListResult{}, err
	}
	if strings.TrimSpace(cursor) == head {
		return exchange.PublicationListResult{NextCursor: head}, nil
	}
	paths, err := s.listEventPaths(ctx, head, prefix)
	if err != nil {
		return exchange.PublicationListResult{}, err
	}
	events := make([]exchange.PublicationStoredEvent, 0, len(paths))
	for _, path := range paths {
		file, found, err := s.readFileWithSHA(ctx, head, path)
		if err != nil {
			return exchange.PublicationListResult{}, err
		}
		if !found {
			continue
		}
		events = append(events, exchange.PublicationStoredEvent{Path: path, Body: file.body, Cursor: head})
	}
	return exchange.PublicationListResult{Events: events, NextCursor: head}, nil
}

func (s *GitHubPublicationStore) ReadFile(ctx context.Context, branch string, path string) ([]byte, error) {
	branch, path, err := normalizeGitHubPublicationFileRef(branch, path)
	if err != nil {
		return nil, err
	}
	file, found, err := s.readFileWithSHA(ctx, branch, path)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("publication file %s:%s not found", branch, path)
	}
	return append([]byte(nil), file.body...), nil
}

func (s *GitHubPublicationStore) WriteFile(ctx context.Context, branch string, path string, body []byte) error {
	branch, path, err := normalizeGitHubPublicationFileRef(branch, path)
	if err != nil {
		return err
	}
	if strings.HasPrefix(path, exchange.PublicationEventRoot+"/") {
		return fmt.Errorf("publication event path %q must be written with PutEvent", path)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	existing, found, err := s.readFileWithSHA(ctx, branch, path)
	if err != nil {
		return err
	}
	sha := ""
	if found {
		sha = existing.sha
	}
	if err := s.pauseBeforeMutation(ctx); err != nil {
		return err
	}
	if err := s.putFile(ctx, branch, path, body, sha); err != nil {
		return err
	}
	s.lastWrite = time.Now()
	return nil
}

func (s *GitHubPublicationStore) EnsureBranch(ctx context.Context, branch string, baseBranch string) error {
	return s.EnsureBranches(ctx, []string{branch}, baseBranch)
}

func (s *GitHubPublicationStore) EnsureBranches(ctx context.Context, branches []string, baseBranch string) error {
	baseBranch, err := normalizeGitHubBranchName(baseBranch)
	if err != nil {
		return fmt.Errorf("base branch: %w", err)
	}
	if baseBranch == "" {
		baseBranch = "main"
	}
	normalized := make([]string, 0, len(branches))
	seen := map[string]bool{}
	for _, branch := range branches {
		branch, err := exchange.NormalizePublicationBranch(branch)
		if err != nil {
			return err
		}
		if seen[branch] {
			continue
		}
		seen[branch] = true
		normalized = append(normalized, branch)
	}
	if len(normalized) == 0 {
		return nil
	}
	var missing []string
	for _, branch := range normalized {
		if _, err := s.branchHead(ctx, branch); err == nil {
			continue
		} else if apiErr, ok := err.(*githubAPIError); ok && apiErr.Status == http.StatusNotFound {
			missing = append(missing, branch)
			continue
		} else {
			return err
		}
	}
	if len(missing) == 0 {
		return nil
	}
	baseSHA, err := s.branchHead(ctx, baseBranch)
	if err != nil {
		return fmt.Errorf("read base branch %q: %w", baseBranch, err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	for _, branch := range missing {
		if err := s.pauseBeforeMutation(ctx); err != nil {
			return err
		}
		req := githubCreateRefRequest{
			Ref: "refs/heads/" + branch,
			SHA: baseSHA,
		}
		status, err := s.do(ctx, http.MethodPost, "/repos/"+s.owner+"/"+s.repo+"/git/refs", nil, req, nil)
		if err != nil {
			if apiErr, ok := err.(*githubAPIError); ok && apiErr.Status == http.StatusUnprocessableEntity {
				if _, headErr := s.branchHead(ctx, branch); headErr == nil {
					continue
				}
			}
			return fmt.Errorf("create branch %q: %w", branch, err)
		}
		if status != http.StatusCreated {
			return fmt.Errorf("create branch %q returned status %d", branch, status)
		}
		s.lastWrite = time.Now()
	}
	return nil
}

type githubFile struct {
	body []byte
	sha  string
}

type githubContentResponse struct {
	Type     string `json:"type"`
	Path     string `json:"path"`
	SHA      string `json:"sha"`
	Encoding string `json:"encoding"`
	Content  string `json:"content"`
}

type githubContentEntry struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

type githubPutFileRequest struct {
	Message string `json:"message"`
	Content string `json:"content"`
	SHA     string `json:"sha,omitempty"`
	Branch  string `json:"branch"`
}

type githubCreateRefRequest struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type githubRefResponse struct {
	Object struct {
		SHA string `json:"sha"`
	} `json:"object"`
}

type githubAPIError struct {
	Status             int
	Message            string
	RetryAfter         string
	RateLimitRemaining string
	RateLimitReset     string
}

func (e *githubAPIError) Error() string {
	base := ""
	if strings.TrimSpace(e.Message) == "" {
		base = fmt.Sprintf("github api status %d", e.Status)
	} else {
		base = fmt.Sprintf("github api status %d: %s", e.Status, e.Message)
	}
	var hints []string
	if strings.TrimSpace(e.RetryAfter) != "" {
		hints = append(hints, "retry_after="+strings.TrimSpace(e.RetryAfter))
	}
	if strings.TrimSpace(e.RateLimitRemaining) != "" {
		hints = append(hints, "rate_limit_remaining="+strings.TrimSpace(e.RateLimitRemaining))
	}
	if strings.TrimSpace(e.RateLimitReset) != "" {
		hints = append(hints, "rate_limit_reset="+strings.TrimSpace(e.RateLimitReset))
	}
	if len(hints) == 0 {
		return base
	}
	return base + " (" + strings.Join(hints, ", ") + ")"
}

func (s *GitHubPublicationStore) readFileWithSHA(ctx context.Context, branch, path string) (githubFile, bool, error) {
	var out githubContentResponse
	status, err := s.do(ctx, http.MethodGet, "/repos/"+s.owner+"/"+s.repo+"/contents/"+escapeGitHubPath(path), url.Values{"ref": []string{branch}}, nil, &out)
	if err != nil {
		if apiErr, ok := err.(*githubAPIError); ok && apiErr.Status == http.StatusNotFound {
			return githubFile{}, false, nil
		}
		return githubFile{}, false, err
	}
	if status != http.StatusOK {
		return githubFile{}, false, fmt.Errorf("github content read returned status %d", status)
	}
	body, err := decodeGitHubContent(out)
	if err != nil {
		return githubFile{}, false, err
	}
	return githubFile{body: body, sha: out.SHA}, true, nil
}

func (s *GitHubPublicationStore) putFile(ctx context.Context, branch, path string, body []byte, sha string) error {
	req := githubPutFileRequest{
		Message: "mnemon publication update " + path,
		Content: base64.StdEncoding.EncodeToString(body),
		SHA:     sha,
		Branch:  branch,
	}
	status, err := s.do(ctx, http.MethodPut, "/repos/"+s.owner+"/"+s.repo+"/contents/"+escapeGitHubPath(path), nil, req, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("github content write returned status %d", status)
	}
	return nil
}

func (s *GitHubPublicationStore) branchHead(ctx context.Context, branch string) (string, error) {
	var out githubRefResponse
	status, err := s.do(ctx, http.MethodGet, "/repos/"+s.owner+"/"+s.repo+"/git/ref/heads/"+escapeGitHubPath(branch), nil, nil, &out)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK || strings.TrimSpace(out.Object.SHA) == "" {
		return "", fmt.Errorf("github branch ref %q returned status %d without sha", branch, status)
	}
	return strings.TrimSpace(out.Object.SHA), nil
}

func (s *GitHubPublicationStore) listEventPaths(ctx context.Context, branch, prefix string) ([]string, error) {
	entries, err := s.listDir(ctx, branch, prefix)
	if err != nil {
		if apiErr, ok := err.(*githubAPIError); ok && apiErr.Status == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		switch entry.Type {
		case "dir":
			nested, err := s.listEventPaths(ctx, branch, entry.Path)
			if err != nil {
				return nil, err
			}
			paths = append(paths, nested...)
		case "file":
			if strings.HasPrefix(entry.Path, prefix) {
				paths = append(paths, entry.Path)
			}
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func (s *GitHubPublicationStore) listDir(ctx context.Context, branch, path string) ([]githubContentEntry, error) {
	var entries []githubContentEntry
	status, err := s.do(ctx, http.MethodGet, "/repos/"+s.owner+"/"+s.repo+"/contents/"+escapeGitHubPath(path), url.Values{"ref": []string{branch}}, nil, &entries)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("github directory list returned status %d", status)
	}
	return entries, nil
}

func (s *GitHubPublicationStore) do(ctx context.Context, method, apiPath string, query url.Values, in any, out any) (int, error) {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(data)
	}
	u := s.baseURL + apiPath
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("User-Agent", s.userAgent)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode >= 300 {
		var apiErr struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &apiErr)
		return resp.StatusCode, &githubAPIError{
			Status:             resp.StatusCode,
			Message:            apiErr.Message,
			RetryAfter:         resp.Header.Get("Retry-After"),
			RateLimitRemaining: resp.Header.Get("X-RateLimit-Remaining"),
			RateLimitReset:     resp.Header.Get("X-RateLimit-Reset"),
		}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func (s *GitHubPublicationStore) pauseBeforeMutation(ctx context.Context) error {
	if s.mutativeDelay <= 0 || s.lastWrite.IsZero() {
		return nil
	}
	wait := s.mutativeDelay - time.Since(s.lastWrite)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func decodeGitHubContent(resp githubContentResponse) ([]byte, error) {
	if resp.Type != "" && resp.Type != "file" {
		return nil, fmt.Errorf("github content %q is not a file", resp.Path)
	}
	if resp.Encoding != "base64" {
		return nil, fmt.Errorf("github content %q has unsupported encoding %q", resp.Path, resp.Encoding)
	}
	content := strings.ReplaceAll(resp.Content, "\n", "")
	body, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func normalizeGitHubPublicationEventRef(branch, path string) (string, string, error) {
	branch, path, err := normalizeGitHubPublicationFileRef(branch, path)
	if err != nil {
		return "", "", err
	}
	if !strings.HasPrefix(path, exchange.PublicationEventRoot+"/") {
		return "", "", fmt.Errorf("publication event path %q must be under %s", path, exchange.PublicationEventRoot)
	}
	return branch, path, nil
}

func normalizeGitHubPublicationFileRef(branch, path string) (string, string, error) {
	branch, err := exchange.NormalizePublicationBranch(branch)
	if err != nil {
		return "", "", err
	}
	path, err = exchange.NormalizePublicationPath(path)
	if err != nil {
		return "", "", err
	}
	return branch, path, nil
}

func normalizeGitHubBranchName(branch string) (string, error) {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return "", nil
	}
	if strings.Contains(branch, "\\") || strings.HasPrefix(branch, "/") {
		return "", fmt.Errorf("branch %q is invalid", branch)
	}
	for _, part := range strings.Split(branch, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("branch %q is invalid", branch)
		}
	}
	if pathpkg.Clean(branch) != branch {
		return "", fmt.Errorf("branch %q is invalid", branch)
	}
	return branch, nil
}

func normalizeGitHubPublicationEventPrefix(prefix string) (string, error) {
	prefix, err := exchange.NormalizePublicationPath(prefix)
	if err != nil {
		return "", err
	}
	if prefix == exchange.PublicationEventRoot {
		return prefix, nil
	}
	if !strings.HasPrefix(prefix, exchange.PublicationEventRoot+"/") {
		return "", fmt.Errorf("publication event prefix %q must be under %s", prefix, exchange.PublicationEventRoot)
	}
	return prefix, nil
}

func escapeGitHubPath(path string) string {
	clean := pathpkg.Clean(strings.Trim(path, "/"))
	if clean == "." {
		return ""
	}
	parts := strings.Split(clean, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
