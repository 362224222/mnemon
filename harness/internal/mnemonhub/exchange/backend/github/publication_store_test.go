package githubbackend

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
)

func TestGitHubPublicationStorePutEventCreateAndIdempotent(t *testing.T) {
	fake := newFakeGitHubPublicationAPI(t)
	store, err := NewPublicationStore(PublicationStoreConfig{
		Repo:       "mnemon-dev/mnemon-teamwork-example",
		Token:      "secret-token",
		BaseURL:    fake.server.URL,
		HTTPClient: fake.server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	path := exchange.PublicationEventRoot + "/replica-a/progress_digest/project/000000000001-dec-a.json"

	first, err := store.PutEvent(context.Background(), "mnemon/agent-a", path, []byte(`{"id":"a"}`))
	if err != nil {
		t.Fatalf("put event create: %v", err)
	}
	if !first.Created || fake.puts != 1 {
		t.Fatalf("first put = %+v, puts=%d; want created with one PUT", first, fake.puts)
	}
	same, err := store.PutEvent(context.Background(), "mnemon/agent-a", path, []byte(`{"id":"a"}`))
	if err != nil {
		t.Fatalf("put event same: %v", err)
	}
	if !same.ExistsSame || same.Conflict || fake.puts != 1 {
		t.Fatalf("same put = %+v puts=%d; want idempotent without PUT", same, fake.puts)
	}
	conflict, err := store.PutEvent(context.Background(), "mnemon/agent-a", path, []byte(`{"id":"b"}`))
	if err != nil {
		t.Fatalf("put event conflict: %v", err)
	}
	if !conflict.Conflict || fake.puts != 1 {
		t.Fatalf("conflict put = %+v puts=%d; want conflict without overwrite", conflict, fake.puts)
	}
}

func TestGitHubPublicationStoreWriteFileUpdatesWithSHA(t *testing.T) {
	fake := newFakeGitHubPublicationAPI(t)
	fake.files["mnemon/team:.mnemon/team.json"] = fakeGitHubFile{body: []byte(`{"schema_version":1}`), sha: "sha-existing"}
	store, err := NewPublicationStore(PublicationStoreConfig{
		Repo:       "mnemon-dev/mnemon-teamwork-example",
		BaseURL:    fake.server.URL,
		HTTPClient: fake.server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.WriteFile(context.Background(), "mnemon/team", ".mnemon/team.json", []byte(`{"schema_version":2}`)); err != nil {
		t.Fatalf("write team manifest: %v", err)
	}
	if fake.lastSHA != "sha-existing" {
		t.Fatalf("write file must include existing sha, got %q", fake.lastSHA)
	}
	body, err := store.ReadFile(context.Background(), "mnemon/team", ".mnemon/team.json")
	if err != nil {
		t.Fatalf("read team manifest: %v", err)
	}
	if string(body) != `{"schema_version":2}` {
		t.Fatalf("read body = %s", body)
	}
}

func TestGitHubPublicationStoreListEventsUsesBranchHeadCursor(t *testing.T) {
	fake := newFakeGitHubPublicationAPI(t)
	fake.head = "head-2"
	fake.files["mnemon/agent-b:"+exchange.PublicationEventRoot+"/replica-b/progress_digest/project/000000000001-dec-b.json"] = fakeGitHubFile{body: []byte(`{"id":"b"}`), sha: "sha-b"}
	fake.files["mnemon/agent-b:"+exchange.PublicationEventRoot+"/replica-c/progress_digest/project/000000000001-dec-c.json"] = fakeGitHubFile{body: []byte(`{"id":"c"}`), sha: "sha-c"}
	store, err := NewPublicationStore(PublicationStoreConfig{
		Repo:       "mnemon-dev/mnemon-teamwork-example",
		BaseURL:    fake.server.URL,
		HTTPClient: fake.server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}

	list, err := store.ListEvents(context.Background(), "mnemon/agent-b", exchange.PublicationEventRoot, "")
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(list.Events) != 2 || list.NextCursor != "head-2" {
		t.Fatalf("list = %+v, want two events at head-2", list)
	}
	again, err := store.ListEvents(context.Background(), "mnemon/agent-b", exchange.PublicationEventRoot, list.NextCursor)
	if err != nil {
		t.Fatalf("list after head cursor: %v", err)
	}
	if len(again.Events) != 0 || again.NextCursor != "head-2" {
		t.Fatalf("list after head cursor = %+v, want empty", again)
	}
}

func TestGitHubPublicationStoreEnsureBranchCreatesMissingBranchFromMain(t *testing.T) {
	fake := newFakeGitHubPublicationAPI(t)
	fake.refs["main"] = "main-sha"
	fake.missingRefs["mnemon/acceptance/run-1/agent-a"] = true
	store, err := NewPublicationStore(PublicationStoreConfig{
		Repo:       "mnemon-dev/mnemon-teamwork-example",
		BaseURL:    fake.server.URL,
		HTTPClient: fake.server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.EnsureBranch(context.Background(), "mnemon/acceptance/run-1/agent-a", "main"); err != nil {
		t.Fatalf("ensure branch: %v", err)
	}
	if fake.creates != 1 {
		t.Fatalf("creates = %d, want one branch create", fake.creates)
	}
	if got := fake.refs["mnemon/acceptance/run-1/agent-a"]; got != "main-sha" {
		t.Fatalf("created branch sha = %q, want main-sha", got)
	}
	if err := store.EnsureBranch(context.Background(), "mnemon/acceptance/run-1/agent-a", "main"); err != nil {
		t.Fatalf("ensure branch again: %v", err)
	}
	if fake.creates != 1 {
		t.Fatalf("idempotent ensure must not recreate branch, creates=%d", fake.creates)
	}
}

func TestGitHubPublicationStoreLiveGated(t *testing.T) {
	if os.Getenv("MNEMON_GITHUB_LIVE") != "1" {
		t.Skip("set MNEMON_GITHUB_LIVE=1 to run the real GitHub publication store smoke test")
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if token == "" {
		t.Skip("GITHUB_TOKEN is required for live GitHub publication store smoke test")
	}
	repo := strings.TrimSpace(os.Getenv("MNEMON_GITHUB_REPO"))
	if repo == "" {
		repo = "mnemon-dev/mnemon-teamwork-example"
	}
	branch := strings.TrimSpace(os.Getenv("MNEMON_GITHUB_BRANCH"))
	if branch == "" {
		branch = "mnemon/agent-a"
	}
	store, err := NewPublicationStore(PublicationStoreConfig{Repo: repo, Token: token})
	if err != nil {
		t.Fatal(err)
	}
	path := exchange.PublicationEventRoot + "/live-smoke/progress_digest/project/000000000001-live-smoke.json"
	body := []byte(`{"schema_version":1,"source":"mnemon live smoke"}`)
	res, err := store.PutEvent(context.Background(), branch, path, body)
	if err != nil {
		t.Fatalf("live put event: %v", err)
	}
	if !res.Created && !res.ExistsSame {
		t.Fatalf("live put result = %+v, want created or exists_same", res)
	}
	list, err := store.ListEvents(context.Background(), branch, exchange.PublicationEventRoot, "")
	if err != nil {
		t.Fatalf("live list events: %v", err)
	}
	if list.NextCursor == "" {
		t.Fatalf("live list must return a branch-head cursor: %+v", list)
	}
}

type fakeGitHubPublicationAPI struct {
	server      *httptest.Server
	files       map[string]fakeGitHubFile
	refs        map[string]string
	missingRefs map[string]bool
	head        string
	puts        int
	creates     int
	lastSHA     string
}

type fakeGitHubFile struct {
	body []byte
	sha  string
}

func newFakeGitHubPublicationAPI(t *testing.T) *fakeGitHubPublicationAPI {
	t.Helper()
	fake := &fakeGitHubPublicationAPI{
		files:       map[string]fakeGitHubFile{},
		refs:        map[string]string{},
		missingRefs: map[string]bool{},
		head:        "head-1",
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeGitHubPublicationAPI) handle(w http.ResponseWriter, r *http.Request) {
	if token := r.Header.Get("Authorization"); strings.Contains(token, "secret-token") && token != "Bearer secret-token" {
		http.Error(w, `{"message":"bad token"}`, http.StatusUnauthorized)
		return
	}
	prefix := "/repos/mnemon-dev/mnemon-teamwork-example/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	tail := strings.TrimPrefix(r.URL.Path, prefix)
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(tail, "git/ref/heads/"):
		branch := strings.TrimPrefix(tail, "git/ref/heads/")
		if f.missingRefs[branch] {
			writeJSON(w, http.StatusNotFound, map[string]any{"message": "ref not found"})
			return
		}
		sha := f.refs[branch]
		if sha == "" {
			sha = f.head
		}
		writeJSON(w, http.StatusOK, map[string]any{"object": map[string]any{"sha": sha}})
	case r.Method == http.MethodPost && tail == "git/refs":
		var req struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
			return
		}
		branch := strings.TrimPrefix(req.Ref, "refs/heads/")
		if branch == req.Ref || branch == "" || req.SHA == "" {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "invalid ref"})
			return
		}
		delete(f.missingRefs, branch)
		f.refs[branch] = req.SHA
		f.creates++
		writeJSON(w, http.StatusCreated, map[string]any{"ref": req.Ref, "object": map[string]any{"sha": req.SHA}})
	case strings.HasPrefix(tail, "contents/"):
		path := strings.TrimPrefix(tail, "contents/")
		branch := r.URL.Query().Get("ref")
		f.handleContents(w, r, branch, path)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeGitHubPublicationAPI) handleContents(w http.ResponseWriter, r *http.Request, branch, path string) {
	switch r.Method {
	case http.MethodGet:
		key := branch + ":" + path
		if file, ok := f.files[key]; ok {
			writeJSON(w, http.StatusOK, map[string]any{
				"type":     "file",
				"path":     path,
				"sha":      file.sha,
				"encoding": "base64",
				"content":  base64.StdEncoding.EncodeToString(file.body),
			})
			return
		}
		entries := f.dirEntries(branch, path)
		if len(entries) > 0 {
			writeJSON(w, http.StatusOK, entries)
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "not found"})
	case http.MethodPut:
		var req struct {
			Content string `json:"content"`
			SHA     string `json:"sha"`
			Branch  string `json:"branch"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
			return
		}
		body, err := base64.StdEncoding.DecodeString(req.Content)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
			return
		}
		if branch == "" {
			branch = req.Branch
		}
		key := branch + ":" + path
		if existing, ok := f.files[key]; ok && req.SHA != existing.sha {
			writeJSON(w, http.StatusConflict, map[string]any{"message": "sha mismatch"})
			return
		}
		f.puts++
		f.lastSHA = req.SHA
		f.files[key] = fakeGitHubFile{body: body, sha: "sha-new"}
		status := http.StatusCreated
		if req.SHA != "" {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]any{"content": map[string]any{"sha": "sha-new"}})
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeGitHubPublicationAPI) dirEntries(branch, dir string) []map[string]any {
	prefix := branch + ":" + strings.TrimSuffix(dir, "/") + "/"
	seen := map[string]map[string]any{}
	for key := range f.files {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rel := strings.TrimPrefix(key, prefix)
		head, _, hasRest := strings.Cut(rel, "/")
		path := strings.TrimSuffix(dir, "/") + "/" + head
		typ := "file"
		if hasRest {
			typ = "dir"
		}
		seen[path] = map[string]any{"type": typ, "path": path}
	}
	out := make([]map[string]any, 0, len(seen))
	for _, entry := range seen {
		out = append(out, entry)
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
