package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/logger"
	skillpkg "github.com/multica-ai/multica/server/internal/skill"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Directory imports turn "a GitHub directory holding many skills" into one
// operation: enumerate each direct child that contains a SKILL.md, import the
// selected children with the same on-conflict semantics as single imports, and
// attach every imported skill to one shared label so the group stays visible
// as a unit in the workspace library and in the agent skill picker.

// maxDirectoryImportSkills bounds one directory batch. It exists so a huge
// monorepo tree cannot keep an import request open for minutes; per-skill caps
// (maxImportFileSize / maxImportTotalSize / maxImportFileCount) still apply to
// each imported skill individually.
const maxDirectoryImportSkills = 50

// directoryImportLabelColor is the palette default for the auto-created group
// label; normalizeColor("") rejects empty, and the single import flow has no
// natural color to inherit.
const directoryImportLabelColor = "#3b82f6"

// directoryImportTimeout returns the overall fetch deadline for a batch of n
// skills: the single-import budget plus per-skill slack, capped well under
// gateway timeouts so a slow source surfaces as an error, not a proxy 504.
func directoryImportTimeout(n int) time.Duration {
	budget := importFetchTimeout + time.Duration(n)*8*time.Second
	if budget > 10*time.Minute {
		budget = 10 * time.Minute
	}
	return budget
}

type PreviewSkillDirectoryRequest struct {
	URL string `json:"url"`
}

type SkillDirectorySkillPreview struct {
	Dir         string `json:"dir"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Conflict    bool   `json:"conflict"`
}

type SkillDirectoryPreviewResponse struct {
	// Kind is "directory" when the URL points at a directory of skills and
	// "single" when the target itself is one skill (its own SKILL.md) — the
	// client falls back to the plain single-skill import for the latter.
	Kind  string `json:"kind"`
	Owner string `json:"owner,omitempty"`
	Repo  string `json:"repo,omitempty"`
	Ref   string `json:"ref,omitempty"`
	Dir   string `json:"dir,omitempty"`
	// Label is the suggested group label (the repository name).
	Label  string                       `json:"label,omitempty"`
	Skills []SkillDirectorySkillPreview `json:"skills,omitempty"`
}

type ImportSkillDirectoryRequest struct {
	URL string `json:"url"`
	// Dirs limits the import to these child directories (values as returned
	// by the preview). Empty means "all children".
	Dirs []string `json:"dirs,omitempty"`
	// Label is the group label attached to every imported skill. Nil defaults
	// to the repository name; an explicit empty string opts out of labeling.
	Label      *string `json:"label"`
	OnConflict string  `json:"on_conflict"`
}

type SkillDirectoryImportItem struct {
	Dir     string `json:"dir"`
	Name    string `json:"name"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	SkillID string `json:"skill_id,omitempty"`
}

type SkillDirectoryImportResponse struct {
	Label   *LabelResponse             `json:"label"`
	Results []SkillDirectoryImportItem `json:"results"`
}

// resolveGitHubDirectorySpec parses a github.com URL and loads the repository
// tree once. Directory imports enumerate every child skill and later read
// every supporting file from the same tree, so a truncated tree is a hard
// error rather than the crawl fallback the single import tolerates.
func resolveGitHubDirectorySpec(ctx context.Context, httpClient *http.Client, rawURL string) (*githubSpec, string, []githubTreeEntry, error) {
	spec, err := parseGitHubURL(rawURL)
	if err != nil {
		return nil, "", nil, err
	}
	if len(spec.refSegments) > 0 {
		if err := resolveGitHubRefAndPath(ctx, httpClient, &spec); err != nil {
			return nil, "", nil, err
		}
	}
	if spec.ref == "" {
		spec.ref = fetchGitHubDefaultBranch(ctx, httpClient, spec.owner, spec.repo)
	}
	tree, truncated, err := fetchGitHubTreeResilient(ctx, httpClient, spec.owner, spec.repo, spec.ref)
	if err != nil {
		return nil, "", nil, err
	}
	if truncated {
		return nil, "", nil, fmt.Errorf("%w: repository tree is truncated (too large); point the import at a smaller directory", errImportSourceUnavailable)
	}
	rawPrefix := rawGitHubPrefix(spec.owner, spec.repo, spec.ref)
	return &spec, rawPrefix, tree, nil
}

func rawGitHubPrefix(owner, repo, ref string) string {
	return fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s",
		url.PathEscape(owner), url.PathEscape(repo), escapeRefPath(ref))
}

// childSkillDirs lists every directory under parentDir that contains a
// SKILL.md, sorted by path for stable preview output. Enumeration is
// depth-unbounded so the natural URLs work: a bare repository root (skills
// usually nested under skills/) as well as a pointed subdirectory. A skill
// directory that itself contains a deeper SKILL.md (e.g. bundled examples)
// shows up twice — the preview list lets the user drop the nested one.
func childSkillDirs(tree []githubTreeEntry, parentDir string) []string {
	prefix := ""
	if parentDir != "" {
		prefix = parentDir + "/"
	}
	dirs := make([]string, 0, 8)
	for _, entry := range tree {
		if entry.Type != "blob" {
			continue
		}
		rest := strings.TrimPrefix(entry.Path, prefix)
		if rest == entry.Path && prefix != "" {
			continue // not under the parent directory
		}
		if !strings.HasSuffix(rest, "/SKILL.md") || rest == "SKILL.md" {
			continue
		}
		dir := strings.TrimSuffix(rest, "/SKILL.md")
		if dir == "" {
			continue
		}
		if parentDir == "" {
			dirs = append(dirs, dir)
		} else {
			dirs = append(dirs, parentDir+"/"+dir)
		}
	}
	sort.Strings(dirs)
	return dirs
}

// dirHasOwnSkillMd reports whether the pointed directory itself is a skill.
func dirHasOwnSkillMd(tree []githubTreeEntry, dir string) bool {
	target := "SKILL.md"
	if dir != "" {
		target = dir + "/SKILL.md"
	}
	for _, entry := range tree {
		if entry.Type == "blob" && entry.Path == target {
			return true
		}
	}
	return false
}

// fetchDirectorySkill builds one importedSkill for a child directory,
// reusing the batch's shared tree for supporting files instead of re-listing.
func fetchDirectorySkill(ctx context.Context, httpClient *http.Client, rawPrefix string, tree []githubTreeEntry, dir, sourceURL, owner, repo, ref string) (*importedSkill, error) {
	skillMdBody, err := fetchRawFile(ctx, httpClient, buildRawGitHubURL(rawPrefix, dir+"/SKILL.md"))
	if err != nil {
		return nil, fmt.Errorf("SKILL.md not found at %s in %s/%s@%s: %w", dir, owner, repo, ref, err)
	}
	name, description := skillpkg.ParseSkillFrontmatter(string(skillMdBody))
	if name == "" {
		name = path.Base(dir)
	}
	result := &importedSkill{
		name:        name,
		description: description,
		content:     string(skillMdBody),
		origin: map[string]any{
			"type":       "github",
			"source_url": sourceURL,
			"owner":      owner,
			"repo":       repo,
			"ref":        ref,
			"path":       dir,
		},
	}
	if err := addSupportingFilesFromTree(ctx, httpClient, result, tree, rawPrefix, dir); err != nil {
		return nil, err
	}
	return result, nil
}

// PreviewSkillDirectoryImport lists the skills a directory URL would import.
func (h *Handler) PreviewSkillDirectoryImport(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireUserID(w, r); !ok {
		return
	}
	var req PreviewSkillDirectoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.URL) == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	source, normalized, err := detectImportSource(req.URL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if source != sourceGitHub {
		writeError(w, http.StatusBadRequest, "directory import supports github.com URLs only")
		return
	}

	httpClient := importDirectoryHTTPClient()
	ctx, cancel := context.WithTimeout(r.Context(), directoryImportTimeout(maxDirectoryImportSkills))
	defer cancel()

	spec, rawPrefix, tree, err := resolveGitHubDirectorySpec(ctx, httpClient, normalized)
	if err != nil {
		status, msg := importFetchErrorResponse(ctx, err)
		writeError(w, status, msg)
		return
	}
	if dirHasOwnSkillMd(tree, spec.skillDir) {
		writeJSON(w, http.StatusOK, SkillDirectoryPreviewResponse{Kind: "single"})
		return
	}
	dirs := childSkillDirs(tree, spec.skillDir)
	if len(dirs) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "no skill directories found under this path; each skill must be a subdirectory with its own SKILL.md")
		return
	}
	if len(dirs) > maxDirectoryImportSkills {
		writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf("directory holds %d skills; the batch limit is %d — import a smaller directory", len(dirs), maxDirectoryImportSkills))
		return
	}

	workspaceUUID := parseUUID(h.resolveWorkspaceID(r))
	skills := make([]SkillDirectorySkillPreview, 0, len(dirs))
	for _, dir := range dirs {
		preview := SkillDirectorySkillPreview{Dir: dir, Name: path.Base(dir)}
		if body, ferr := fetchRawFile(ctx, httpClient, buildRawGitHubURL(rawPrefix, dir+"/SKILL.md")); ferr == nil {
			if name, description := skillpkg.ParseSkillFrontmatter(string(body)); name != "" {
				preview.Name = name
				preview.Description = description
			}
		}
		if _, found, lerr := h.lookupSkillByName(ctx, workspaceUUID, preview.Name); lerr == nil && found {
			preview.Conflict = true
		}
		skills = append(skills, preview)
	}
	writeJSON(w, http.StatusOK, SkillDirectoryPreviewResponse{
		Kind:   "directory",
		Owner:  spec.owner,
		Repo:   spec.repo,
		Ref:    spec.ref,
		Dir:    spec.skillDir,
		Label:  suggestedDirectoryLabel(spec.repo),
		Skills: skills,
	})
}

// suggestedDirectoryLabel derives the default group label from the repository
// name: lowercased, bounded to the label name limit so oversized repo names
// degrade to a truncated label instead of failing the whole import.
func suggestedDirectoryLabel(repo string) string {
	label := strings.ToLower(strings.TrimSpace(repo))
	if len(label) > maxLabelNameLen {
		label = label[:maxLabelNameLen]
	}
	if name, err := validateLabelName(label); err == nil {
		return name
	}
	return ""
}

// importDirectoryHTTPClient builds the HTTP client the directory handlers use
// for upstream fetches. Swappable in tests to redirect GitHub traffic to a
// fixture server — the same seam pattern as clawHubAPIBase.
var importDirectoryHTTPClient = newImportHTTPClient

// ImportSkillDirectory imports the selected child skills of a directory URL
// and attaches them to one shared skill label (found or created by name).
func (h *Handler) ImportSkillDirectory(w http.ResponseWriter, r *http.Request) {
	workspaceID := h.resolveWorkspaceID(r)
	creatorID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace_id")
	if !ok {
		return
	}
	creatorUUID := parseUUID(creatorID)

	var req ImportSkillDirectoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.URL) == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	// The batch default is skip, not fail: one colliding name in a group of
	// otherwise-new skills should not sink the remaining imports.
	strategy := req.OnConflict
	if strategy == "" {
		strategy = importOnConflictSkip
	}
	if !validImportOnConflict(strategy) {
		writeError(w, http.StatusBadRequest, "on_conflict must be one of: fail, overwrite, rename, skip")
		return
	}
	source, normalized, err := detectImportSource(req.URL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if source != sourceGitHub {
		writeError(w, http.StatusBadRequest, "directory import supports github.com URLs only")
		return
	}

	httpClient := importDirectoryHTTPClient()
	ctx, cancel := context.WithTimeout(r.Context(), directoryImportTimeout(maxDirectoryImportSkills))
	defer cancel()

	spec, rawPrefix, tree, err := resolveGitHubDirectorySpec(ctx, httpClient, normalized)
	if err != nil {
		status, msg := importFetchErrorResponse(ctx, err)
		writeError(w, status, msg)
		return
	}
	if dirHasOwnSkillMd(tree, spec.skillDir) {
		writeError(w, http.StatusUnprocessableEntity, "this URL points at a single skill; use the regular import instead")
		return
	}
	dirs := selectDirectoryImportDirs(childSkillDirs(tree, spec.skillDir), req.Dirs)
	if len(dirs) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "no skill directories selected")
		return
	}

	labelName := suggestedDirectoryLabel(spec.repo)
	if req.Label != nil {
		labelName = *req.Label
		if labelName != "" {
			if name, lerr := validateLabelName(labelName); lerr != nil {
				writeError(w, http.StatusBadRequest, lerr.Error())
				return
			} else {
				labelName = name
			}
		}
	}

	results := make([]SkillDirectoryImportItem, 0, len(dirs))
	for _, dir := range dirs {
		item := h.importSkillDirectoryItem(ctx, r, httpClient, spec, rawPrefix, tree, dir, normalized, strategy, workspaceID, workspaceUUID, creatorUUID, creatorID)
		results = append(results, item)
	}

	if labelName != "" {
		// A failed label attach must not fail the response: the skills are
		// already imported, and attaching stays retryable via the label UI.
		label, lerr := h.attachDirectoryImportLabel(ctx, r, workspaceID, workspaceUUID, labelName, results)
		if lerr != nil {
			slog.Warn("directory import: attach group label failed", append(logger.RequestAttrs(r), "label", labelName, "error", lerr)...)
		} else {
			writeJSON(w, http.StatusOK, SkillDirectoryImportResponse{Label: label, Results: results})
			return
		}
	}
	writeJSON(w, http.StatusOK, SkillDirectoryImportResponse{Results: results})
}

// selectDirectoryImportDirs intersects the enumerated children with the
// requested dirs, preserving enumeration order. An explicit dir list naming
// nothing under the directory yields empty (the caller rejects it).
func selectDirectoryImportDirs(all, requested []string) []string {
	if len(requested) == 0 {
		return all
	}
	want := make(map[string]struct{}, len(requested))
	for _, dir := range requested {
		want[dir] = struct{}{}
	}
	dirs := make([]string, 0, len(requested))
	for _, dir := range all {
		if _, ok := want[dir]; ok {
			dirs = append(dirs, dir)
		}
	}
	return dirs
}

// importSkillDirectoryItem imports one child skill with the single-import
// on-conflict semantics, returning a per-item result instead of writing an
// HTTP response: one colliding or failing skill must not abort its siblings.
func (h *Handler) importSkillDirectoryItem(ctx context.Context, r *http.Request, httpClient *http.Client, spec *githubSpec, rawPrefix string, tree []githubTreeEntry, dir, sourceURL, strategy, workspaceID string, workspaceUUID, creatorUUID pgtype.UUID, creatorID string) SkillDirectoryImportItem {
	item := SkillDirectoryImportItem{Dir: dir, Name: path.Base(dir)}
	imported, err := fetchDirectorySkill(ctx, httpClient, rawPrefix, tree, dir, sourceURL, spec.owner, spec.repo, spec.ref)
	if err != nil {
		item.Status = "failed"
		item.Reason = err.Error()
		return item
	}
	item.Name = sanitizeNullBytes(imported.name)
	files := importedSkillFileRequests(imported)
	config := map[string]any{"origin": imported.origin}

	existing, found, lerr := h.lookupSkillByName(ctx, workspaceUUID, item.Name)
	if lerr != nil {
		item.Status = "failed"
		item.Reason = "failed to check for existing skill"
		return item
	}
	if !found {
		resp, cerr := h.createImportedSkillWithName(ctx, workspaceUUID, creatorUUID, item.Name, imported, config, files)
		if cerr != nil {
			item.Status = "failed"
			if isUniqueViolation(cerr) {
				item.Reason = "a skill with this name already exists"
			} else {
				item.Reason = "failed to create skill"
			}
			return item
		}
		actorType, actorID := h.resolveActor(r, creatorID, workspaceID)
		h.publish(protocol.EventSkillCreated, workspaceID, actorType, actorID, map[string]any{"skill": resp})
		item.Status = "created"
		item.SkillID = resp.ID
		return item
	}
	item.SkillID = uuidToString(existing.ID)

	switch strategy {
	case importOnConflictSkip:
		item.Status = "skipped"
		item.Reason = "a skill with this name already exists"
	case importOnConflictOverwrite:
		if !canOverwriteSkillByLocalImport(creatorID, existing) {
			item.Status = "failed"
			item.Reason = "only the skill creator can overwrite this skill"
			return item
		}
		resp, oerr := h.overwriteSkillWithFiles(ctx, skillOverwriteInput{
			WorkspaceID:   workspaceUUID,
			TargetSkillID: existing.ID,
			UserID:        creatorID,
			ExpectedName:  item.Name,
			Description:   imported.description,
			Content:       imported.content,
			Config:        config,
			Files:         files,
		})
		if oerr != nil {
			item.Status = "failed"
			_, item.Reason = skillImportOverwriteFailure(oerr)
			return item
		}
		actorType, actorID := h.resolveActor(r, creatorID, workspaceID)
		h.publish(protocol.EventSkillUpdated, workspaceID, actorType, actorID, map[string]any{"skill": resp})
		item.Status = "updated"
		item.SkillID = resp.ID
	case importOnConflictRename:
		resp, rerr := h.createRenamedImportedSkill(ctx, workspaceUUID, creatorUUID, item.Name, imported, config, files)
		if rerr != nil {
			item.Status = "failed"
			item.Reason = "failed to create renamed skill"
			return item
		}
		actorType, actorID := h.resolveActor(r, creatorID, workspaceID)
		h.publish(protocol.EventSkillCreated, workspaceID, actorType, actorID, map[string]any{"skill": resp})
		item.Status = "created"
		item.Name = resp.Name
		item.Reason = "renamed to avoid an existing skill"
		item.SkillID = resp.ID
	default: // fail
		item.Status = "failed"
		item.Reason = skillImportConflictReason()
		item.SkillID = ""
	}
	return item
}

// attachDirectoryImportLabel finds or creates the group skill label and
// attaches it to every imported skill that has an id (created/updated/renamed;
// skipped items keep whatever labels they already had).
func (h *Handler) attachDirectoryImportLabel(ctx context.Context, r *http.Request, workspaceID string, workspaceUUID pgtype.UUID, labelName string, results []SkillDirectoryImportItem) (*LabelResponse, error) {
	existing, err := h.findSkillLabelByName(ctx, workspaceUUID, labelName)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		created, cerr := h.Queries.CreateLabel(ctx, db.CreateLabelParams{
			WorkspaceID:  workspaceUUID,
			ResourceType: "skill",
			Name:         labelName,
			Color:        directoryImportLabelColor,
		})
		if cerr != nil {
			if !isUniqueViolation(cerr) {
				return nil, cerr
			}
			// Lost a create race; resolve through the list once more.
			if existing, err = h.findSkillLabelByName(ctx, workspaceUUID, labelName); err != nil || existing == nil {
				return nil, cerr
			}
		} else {
			existing = &created
			h.publish(protocol.EventLabelCreated, workspaceID, "member", requestUserID(r), map[string]any{
				"label": labelToResponse(created),
			})
		}
	}
	for _, item := range results {
		if item.SkillID == "" {
			continue
		}
		if err := h.Queries.AttachLabelToSkill(ctx, db.AttachLabelToSkillParams{
			SkillID:     parseUUID(item.SkillID),
			LabelID:     existing.ID,
			WorkspaceID: workspaceUUID,
		}); err != nil {
			return nil, err
		}
	}
	resp := labelToResponse(*existing)
	h.publish(protocol.EventLabelUpdated, workspaceID, "member", requestUserID(r), map[string]any{"label": resp})
	return &resp, nil
}

// findSkillLabelByName resolves a workspace skill label by (case-insensitive)
// name. ListLabels has no name filter, but label counts are small.
func (h *Handler) findSkillLabelByName(ctx context.Context, workspaceUUID pgtype.UUID, labelName string) (*db.IssueLabel, error) {
	labels, err := h.Queries.ListLabels(ctx, db.ListLabelsParams{WorkspaceID: workspaceUUID, ResourceType: "skill"})
	if err != nil {
		return nil, err
	}
	for i := range labels {
		if strings.EqualFold(labels[i].Name, labelName) {
			label := db.IssueLabel{
				ID:           labels[i].ID,
				WorkspaceID:  labels[i].WorkspaceID,
				Name:         labels[i].Name,
				Color:        labels[i].Color,
				CreatedAt:    labels[i].CreatedAt,
				UpdatedAt:    labels[i].UpdatedAt,
				ResourceType: labels[i].ResourceType,
				Description:  labels[i].Description,
			}
			return &label, nil
		}
	}
	return nil, nil
}
