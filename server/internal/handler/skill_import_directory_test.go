package handler

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// directoryFixture serves a two-skill repository:
//
//	ui-skills/
//	  SKILL.md            ← root skill (drives the "single" branch)
//	  README.md
//	  skills/alpha/SKILL.md          (frontmatter name: alpha-skill)
//	  skills/alpha/references/notes.md
//	  skills/beta/SKILL.md           (no frontmatter → name falls back to dir)
//	  skills/beta/scripts/run.py
//	  skills/justdocs/readme.md      (no SKILL.md → not a skill)
func directoryFixture(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("X-Test-Original-Host") {
		case "api.github.com":
			switch r.URL.Path {
			case "/repos/acme/ui-skills":
				writeJSON(w, http.StatusOK, map[string]any{"default_branch": "main"})
			case "/repos/acme/ui-skills/commits/main":
				// Ref probe used to disambiguate /tree/{ref}/{path} URLs.
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("0123456789abcdef"))
			case "/repos/acme/ui-skills/git/trees/main":
				writeJSON(w, http.StatusOK, githubTreeResponse{
					Tree: []githubTreeEntry{
						{Path: "SKILL.md", Type: "blob"},
						{Path: "README.md", Type: "blob"},
						{Path: "skills/alpha/SKILL.md", Type: "blob"},
						{Path: "skills/alpha/references/notes.md", Type: "blob"},
						{Path: "skills/beta/SKILL.md", Type: "blob"},
						{Path: "skills/beta/scripts/run.py", Type: "blob"},
						{Path: "skills/justdocs/readme.md", Type: "blob"},
					},
				})
			default:
				http.NotFound(w, r)
			}
		case "raw.githubusercontent.com":
			switch r.URL.Path {
			case "/acme/ui-skills/main/skills/alpha/SKILL.md":
				w.Write([]byte("---\nname: alpha-skill\ndescription: Alpha preview\n---\nalpha body"))
			case "/acme/ui-skills/main/skills/alpha/references/notes.md":
				w.Write([]byte("alpha notes"))
			case "/acme/ui-skills/main/skills/beta/SKILL.md":
				w.Write([]byte("beta body"))
			case "/acme/ui-skills/main/skills/beta/scripts/run.py":
				w.Write([]byte("print('beta')"))
			case "/acme/ui-skills/main/SKILL.md":
				w.Write([]byte("---\nname: root-skill\n---\nroot"))
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}
}

func withDirectoryFixtureClient(t *testing.T) {
	t.Helper()
	prev := importDirectoryHTTPClient
	client, _ := newGitHubFixtureClient(t, directoryFixture(t))
	importDirectoryHTTPClient = func() *http.Client { return client }
	t.Cleanup(func() { importDirectoryHTTPClient = prev })
}

// insertHandlerTestSkillExact inserts a skill with an exact name (the shared
// helper derives names from the test name; directory imports collide on
// upstream skill names instead).
func insertHandlerTestSkillExact(t *testing.T, name string) {
	t.Helper()
	var id string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO skill (workspace_id, name, description, content, config, created_by)
		VALUES ($1, $2, 'fixture', '# Existing', '{}'::jsonb, $3)
		RETURNING id
	`, testWorkspaceID, name, testUserID).Scan(&id); err != nil {
		t.Fatalf("insert skill %q: %v", name, err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM skill WHERE id = $1`, id)
	})
}

// cleanupDirectoryImport removes the skills and group label a directory
// import test created. Labels have no FK to skill rows, so both sides need
// explicit deletes.
func cleanupDirectoryImport(t *testing.T, labelName string, skillNames ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, name := range skillNames {
			var id string
			if err := testPool.QueryRow(ctx, `SELECT id FROM skill WHERE workspace_id=$1 AND name=$2`,
				testWorkspaceID, name).Scan(&id); err == nil {
				testPool.Exec(ctx, `DELETE FROM skill_to_label WHERE skill_id=$1`, id)
				testPool.Exec(ctx, `DELETE FROM skill WHERE id=$1`, id)
			}
		}
		testPool.Exec(ctx, `DELETE FROM issue_label WHERE workspace_id=$1 AND name=$2 AND resource_type='skill'`,
			testWorkspaceID, labelName)
	})
}

func testWorkspaceUUID() pgtype.UUID {
	return parseUUID(testWorkspaceID)
}

func TestChildSkillDirs(t *testing.T) {
	tree := []githubTreeEntry{
		{Path: "SKILL.md", Type: "blob"},
		{Path: "README.md", Type: "blob"},
		{Path: "skills/alpha/SKILL.md", Type: "blob"},
		{Path: "skills/alpha/references/notes.md", Type: "blob"},
		{Path: "skills/beta/SKILL.md", Type: "blob"},
		{Path: "skills/justdocs/readme.md", Type: "blob"},
		{Path: "skills/nested/inner/SKILL.md", Type: "blob"},
		{Path: "other/gamma/SKILL.md", Type: "blob"},
		{Path: "toplevel/SKILL.md", Type: "blob"},
	}
	got := childSkillDirs(tree, "skills")
	want := []string{"skills/alpha", "skills/beta", "skills/nested/inner"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("childSkillDirs = %v, want %v", got, want)
	}
	// Root enumeration is depth-unbounded so a bare repository URL finds
	// skills nested under skills/ too.
	root := childSkillDirs(tree, "")
	wantRoot := []string{"other/gamma", "skills/alpha", "skills/beta", "skills/nested/inner", "toplevel"}
	if strings.Join(root, ",") != strings.Join(wantRoot, ",") {
		t.Fatalf("childSkillDirs(root) = %v, want %v", root, wantRoot)
	}
}

func TestSelectDirectoryImportDirs(t *testing.T) {
	all := []string{"a", "b", "c"}
	if got := selectDirectoryImportDirs(all, nil); strings.Join(got, "") != "abc" {
		t.Fatalf("nil selection = %v, want all", got)
	}
	// Order follows enumeration, not the request order; unknown dirs drop.
	got := selectDirectoryImportDirs(all, []string{"c", "nope", "a"})
	if strings.Join(got, ",") != "a,c" {
		t.Fatalf("selection = %v", got)
	}
	if got := selectDirectoryImportDirs(all, []string{"nope"}); len(got) != 0 {
		t.Fatalf("unknown selection = %v, want empty", got)
	}
}

func TestSuggestedDirectoryLabel(t *testing.T) {
	if got := suggestedDirectoryLabel("ui-skills"); got != "ui-skills" {
		t.Fatalf("label = %q", got)
	}
	if got := suggestedDirectoryLabel("A-Very-Long-Repository-Name-That-Exceeds-32-Runes"); got != "a-very-long-repository-name-that" {
		t.Fatalf("truncated label = %q (len %d)", got, len(got))
	}
	if got := suggestedDirectoryLabel(""); got != "" {
		t.Fatalf("empty repo label = %q", got)
	}
}

func TestFetchDirectorySkillReusesSharedTree(t *testing.T) {
	client, _ := newGitHubFixtureClient(t, directoryFixture(t))
	skill, err := fetchDirectorySkill(context.Background(), client, "https://raw.githubusercontent.com/acme/ui-skills/main",
		[]githubTreeEntry{
			{Path: "skills/alpha/SKILL.md", Type: "blob"},
			{Path: "skills/alpha/references/notes.md", Type: "blob"},
		}, "skills/alpha", "https://github.com/acme/ui-skills/tree/main/skills", "acme", "ui-skills", "main")
	if err != nil {
		t.Fatalf("fetchDirectorySkill: %v", err)
	}
	if skill.name != "alpha-skill" || skill.description != "Alpha preview" {
		t.Fatalf("name/description = %q/%q", skill.name, skill.description)
	}
	if !strings.Contains(skill.content, "alpha body") {
		t.Fatalf("content = %q", skill.content)
	}
	if got := importedFilePaths(skill.files); strings.Join(got, ",") != "references/notes.md" {
		t.Fatalf("files = %v", got)
	}
	if skill.origin["path"] != "skills/alpha" || skill.origin["repo"] != "ui-skills" {
		t.Fatalf("origin = %#v", skill.origin)
	}
}

func TestPreviewSkillDirectoryReportsConflicts(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test DB not configured")
	}
	insertHandlerTestSkillExact(t, "alpha-skill")
	withDirectoryFixtureClient(t)

	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/preview", map[string]any{
		"url": "https://github.com/acme/ui-skills/tree/main/skills",
	})
	testHandler.PreviewSkillDirectoryImport(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var body SkillDirectoryPreviewResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Kind != "directory" || body.Repo != "ui-skills" || body.Label != "ui-skills" {
		t.Fatalf("body head = %#v", body)
	}
	if len(body.Skills) != 2 {
		t.Fatalf("skills = %#v", body.Skills)
	}
	first := body.Skills[0]
	if first.Dir != "skills/alpha" || first.Name != "alpha-skill" || first.Description != "Alpha preview" {
		t.Fatalf("first = %#v", first)
	}
	if !first.Conflict {
		t.Fatalf("alpha-skill should be marked conflicting, got %#v", first)
	}
	if body.Skills[1].Conflict {
		t.Fatalf("beta should not conflict, got %#v", body.Skills[1])
	}
}

func TestPreviewSkillDirectorySingleSkillFallsBack(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test DB not configured")
	}
	withDirectoryFixtureClient(t)

	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/preview", map[string]any{
		"url": "https://github.com/acme/ui-skills/tree/main",
	})
	testHandler.PreviewSkillDirectoryImport(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var body SkillDirectoryPreviewResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Kind != "single" {
		t.Fatalf("kind = %q, want single", body.Kind)
	}
}

func TestPreviewSkillDirectoryRejectsNonGitHub(t *testing.T) {
	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/preview", map[string]any{
		"url": "https://clawhub.ai/acme/some-skill",
	})
	testHandler.PreviewSkillDirectoryImport(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestImportSkillDirectoryCreatesSkillsAndGroupLabel(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test DB not configured")
	}
	cleanupDirectoryImport(t, "ui-skills", "alpha-skill", "beta")
	withDirectoryFixtureClient(t)

	// No label in the request: the repository-name default applies.
	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/batch", map[string]any{
		"url":         "https://github.com/acme/ui-skills/tree/main/skills",
		"dirs":        []string{"skills/alpha"},
		"on_conflict": "skip",
	})
	testHandler.ImportSkillDirectory(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var body SkillDirectoryImportResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Label == nil || body.Label.Name != "ui-skills" || body.Label.ResourceType != "skill" {
		t.Fatalf("label = %#v", body.Label)
	}
	if len(body.Results) != 1 {
		t.Fatalf("results = %#v", body.Results)
	}
	item := body.Results[0]
	if item.Status != "created" || item.Name != "alpha-skill" || item.SkillID == "" {
		t.Fatalf("item = %#v", item)
	}

	skill, found, err := testHandler.lookupSkillByName(context.Background(), testWorkspaceUUID(), "alpha-skill")
	if err != nil || !found {
		t.Fatalf("lookup imported skill: found=%v err=%v", found, err)
	}
	labels, err := testHandler.Queries.ListLabelsBySkill(context.Background(), db.ListLabelsBySkillParams{
		SkillID: skill.ID, WorkspaceID: testWorkspaceUUID(),
	})
	if err != nil {
		t.Fatalf("list skill labels: %v", err)
	}
	if len(labels) != 1 || labels[0].Name != "ui-skills" {
		t.Fatalf("labels = %#v", labels)
	}
}

// TestImportSkillDirectoryImportsConcurrently pins the parallel batch: each
// item's first fetch is its SKILL.md, so the fixture counts how many of those
// overlap. A regression to a serial loop answers one request at a time and
// maxInFlight stays at 1, failing fast instead of hanging.
func TestImportSkillDirectoryImportsConcurrently(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test DB not configured")
	}
	cleanupDirectoryImport(t, "ui-skills", "alpha-skill", "beta")

	var inFlight, maxInFlight atomic.Int64
	prev := importDirectoryHTTPClient
	client, _ := newGitHubFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Original-Host") == "raw.githubusercontent.com" && strings.HasSuffix(r.URL.Path, "/SKILL.md") {
			n := inFlight.Add(1)
			for {
				max := maxInFlight.Load()
				if n <= max || maxInFlight.CompareAndSwap(max, n) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond) // widen the overlap window
			defer inFlight.Add(-1)
		}
		directoryFixture(t)(w, r)
	})
	importDirectoryHTTPClient = func() *http.Client { return client }
	t.Cleanup(func() { importDirectoryHTTPClient = prev })

	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/batch", map[string]any{
		"url":         "https://github.com/acme/ui-skills/tree/main/skills",
		"on_conflict": "skip",
	})
	testHandler.ImportSkillDirectory(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}

	var body SkillDirectoryImportResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Results) != 2 {
		t.Fatalf("results = %#v", body.Results)
	}
	for _, item := range body.Results {
		if item.Status != "created" {
			t.Fatalf("item = %#v", item)
		}
	}
	// Results keep enumeration order even though the imports overlap.
	if body.Results[0].Dir != "skills/alpha" || body.Results[1].Dir != "skills/beta" {
		t.Fatalf("result order = %s, %s", body.Results[0].Dir, body.Results[1].Dir)
	}
	if maxInFlight.Load() < 2 {
		t.Fatalf("SKILL.md fetches never overlapped: maxInFlight = %d (serial import?)", maxInFlight.Load())
	}
}

func TestImportSkillDirectorySkipsExistingAndKeepsGoing(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test DB not configured")
	}
	// beta has no frontmatter name, so the import names it after its directory.
	insertHandlerTestSkillExact(t, "beta")
	cleanupDirectoryImport(t, "dir-import-group", "alpha-skill", "beta")
	withDirectoryFixtureClient(t)

	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/batch", map[string]any{
		"url":         "https://github.com/acme/ui-skills/tree/main/skills",
		"on_conflict": "skip",
		"label":       "dir-import-group",
	})
	testHandler.ImportSkillDirectory(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var body SkillDirectoryImportResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Results) != 2 {
		t.Fatalf("results = %#v", body.Results)
	}
	byDir := map[string]SkillDirectoryImportItem{}
	for _, item := range body.Results {
		byDir[item.Dir] = item
	}
	if byDir["skills/alpha"].Status != "created" {
		t.Fatalf("alpha = %#v", byDir["skills/alpha"])
	}
	if byDir["skills/beta"].Status != "skipped" {
		t.Fatalf("beta = %#v", byDir["skills/beta"])
	}
	if body.Label == nil || body.Label.Name != "dir-import-group" {
		t.Fatalf("label = %#v", body.Label)
	}
}

func TestImportSkillDirectoryRejectsSingleSkillURL(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test DB not configured")
	}
	withDirectoryFixtureClient(t)

	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/batch", map[string]any{
		"url":         "https://github.com/acme/ui-skills/tree/main",
		"on_conflict": "skip",
	})
	testHandler.ImportSkillDirectory(w, req)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
}

// rootlessFixture serves the same repo without a root SKILL.md — the bare
// repository URL (no /tree/{ref} segment) is then a pure directory import.
func rootlessFixture(t *testing.T) http.HandlerFunc {
	t.Helper()
	base := directoryFixture(t)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Original-Host") == "api.github.com" &&
			r.URL.Path == "/repos/acme/ui-skills/git/trees/main" {
			writeJSON(w, http.StatusOK, githubTreeResponse{
				Tree: []githubTreeEntry{
					{Path: "README.md", Type: "blob"},
					{Path: "skills/alpha/SKILL.md", Type: "blob"},
					{Path: "skills/beta/SKILL.md", Type: "blob"},
				},
			})
			return
		}
		base(w, r)
	}
}

func TestPreviewSkillDirectoryBareRepoURL(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test DB not configured")
	}
	prev := importDirectoryHTTPClient
	client, _ := newGitHubFixtureClient(t, rootlessFixture(t))
	importDirectoryHTTPClient = func() *http.Client { return client }
	t.Cleanup(func() { importDirectoryHTTPClient = prev })

	// No /tree/{ref} segment: the repository root itself is the directory.
	w := httptest.NewRecorder()
	req := newRequestAsUser(testUserID, http.MethodPost, "/api/skills/import/preview", map[string]any{
		"url": "https://github.com/acme/ui-skills",
	})
	testHandler.PreviewSkillDirectoryImport(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var body SkillDirectoryPreviewResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Kind != "directory" || body.Dir != "" || body.Ref != "main" {
		t.Fatalf("head = %#v", body)
	}
	if len(body.Skills) != 2 || body.Skills[0].Dir != "skills/alpha" || body.Skills[1].Dir != "skills/beta" {
		t.Fatalf("skills = %#v", body.Skills)
	}
}

func TestDoImportGetRetries5xxThenSucceeds(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := newImportHTTPClient()
	resp, err := doImportGet(t.Context(), client, func() (*http.Request, error) {
		return http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	})
	if err != nil {
		t.Fatalf("doImportGet: %v", err)
	}
	resp.Body.Close()
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (one 502 retry)", calls)
	}
}

func TestDoImportGetGivesUpAfterAttempts(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	client := newImportHTTPClient()
	if _, err := doImportGet(t.Context(), client, func() (*http.Request, error) {
		return http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	}); err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if calls != importFetchAttempts {
		t.Fatalf("calls = %d, want %d", calls, importFetchAttempts)
	}
}

func TestDoImportGetDoesNotRetry4xx(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	t.Cleanup(srv.Close)

	client := newImportHTTPClient()
	resp, err := doImportGet(t.Context(), client, func() (*http.Request, error) {
		return http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	})
	if err != nil {
		t.Fatalf("doImportGet: %v", err)
	}
	resp.Body.Close()
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (422 is authoritative)", calls)
	}
}

// tarballFixtureBytes builds a codeload-style tar.gz whose entries live under
// a "repo-sha/" top-level directory, exactly as codeload serves them.
func tarballFixtureBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	files := []struct{ name, body string }{
		{"ui-skills-abc/README.md", "# repo"},
		{"ui-skills-abc/skills/alpha/SKILL.md", "---\nname: alpha-skill\n---\nbody"},
		{"ui-skills-abc/skills/alpha/references/notes.md", "notes"},
		{"ui-skills-abc/skills/beta/SKILL.md", "beta"},
	}
	for _, f := range files {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatalf("tar header: %v", err)
		}
		if _, err := tw.Write([]byte(f.body)); err != nil {
			t.Fatalf("tar body: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestGithubTreeViaTarballStripsTopDir(t *testing.T) {
	client, requests := newGitHubFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Original-Host") == "codeload.github.com" {
			w.Write(tarballFixtureBytes(t))
			return
		}
		http.NotFound(w, r)
	})
	entries, err := githubTreeViaTarball(t.Context(), client, "acme", "ui-skills", "")
	if err != nil {
		t.Fatalf("githubTreeViaTarball: %v", err)
	}
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Type != "blob" || e.Size <= 0 {
			t.Fatalf("entry = %#v", e)
		}
		got = append(got, e.Path)
	}
	want := "README.md,skills/alpha/SKILL.md,skills/alpha/references/notes.md,skills/beta/SKILL.md"
	if strings.Join(got, ",") != want {
		t.Fatalf("entries = %v", got)
	}
	for _, req := range *requests {
		if strings.Contains(req, "codeload.github.com") && !strings.Contains(req, "/tar.gz/HEAD") {
			t.Fatalf("empty ref must request /tar.gz/HEAD, got %s", req)
		}
	}
}

func TestFetchGitHubTreeResilientFallsBackOnAPIRateLimit(t *testing.T) {
	client, _ := newGitHubFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("X-Test-Original-Host") {
		case "api.github.com":
			// Shared-IP anonymous quota exhaustion.
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"message":"API rate limit exceeded"}`))
		case "codeload.github.com":
			w.Write(tarballFixtureBytes(t))
		default:
			http.NotFound(w, r)
		}
	})
	tree, truncated, err := fetchGitHubTreeResilient(t.Context(), client, "acme", "ui-skills", "main")
	if err != nil {
		t.Fatalf("fetchGitHubTreeResilient: %v", err)
	}
	if truncated || len(tree) != 4 {
		t.Fatalf("tree = %#v truncated=%v", tree, truncated)
	}
}

func TestResolveGitHubDirectorySpecSurvivesAPIBlock(t *testing.T) {
	// The screenshot scenario end-to-end at the resolver level: bare repo
	// URL, API fully rate-limited, codeload tarball carries the tree.
	client, _ := newGitHubFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Original-Host") == "api.github.com" {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"message":"API rate limit exceeded"}`))
			return
		}
		if r.Header.Get("X-Test-Original-Host") == "codeload.github.com" {
			w.Write(tarballFixtureBytes(t))
			return
		}
		http.NotFound(w, r)
	})
	spec, rawPrefix, tree, err := resolveGitHubDirectorySpec(t.Context(), client, "https://github.com/acme/ui-skills")
	if err != nil {
		t.Fatalf("resolveGitHubDirectorySpec: %v", err)
	}
	if spec.ref != "HEAD" || !strings.HasSuffix(rawPrefix, "/HEAD") {
		t.Fatalf("ref = %q rawPrefix = %q", spec.ref, rawPrefix)
	}
	if got := childSkillDirs(tree, ""); len(got) != 2 || got[0] != "skills/alpha" || got[1] != "skills/beta" {
		t.Fatalf("childSkillDirs = %v", got)
	}
}
