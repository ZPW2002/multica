package handler

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// The GitHub API (api.github.com) enforces a 60 requests/hour anonymous quota
// per source IP. Self-hosted deployments whose egress rides a shared proxy
// exit exhaust that quota almost immediately, which used to hard-fail every
// skill import with 403s. Everything the import flow needs is also reachable
// without the API: the codeload tarball enumerates the repository and
// raw.githubusercontent.com serves file bodies, and neither is API-rate
// limited. fetchGitHubTreeResilient tries the API tree first (it carries the
// truncation signal and exact provenance) and falls back to the tarball;
// callers receive identical githubTreeEntry slices either way.

const (
	githubTarballMaxCompressed   = 64 << 20  // 64 MiB compressed download cap
	githubTarballMaxExpandedSize = 256 << 20 // 256 MiB uncompressed sanity cap
)

// fetchGitHubTreeResilient resolves the repository tree via the API when it
// answers and via the codeload tarball when it does not (rate limit, blocked
// egress, transient failure). A truncated API tree is NOT retried via the
// tarball: both describe the same oversized repository, so the caller's
// "point at a smaller directory" error stands.
func fetchGitHubTreeResilient(ctx context.Context, httpClient *http.Client, owner, repo, ref string) ([]githubTreeEntry, bool, error) {
	tree, truncated, apiErr := fetchGitHubTree(ctx, httpClient, owner, repo, ref)
	if apiErr == nil {
		return tree, truncated, nil
	}
	entries, tarErr := githubTreeViaTarball(ctx, httpClient, owner, repo, ref)
	if tarErr != nil {
		return nil, false, apiErr
	}
	return entries, false, nil
}

// githubTreeViaTarball enumerates a repository's file entries from the
// codeload tarball. An empty ref requests /tar.gz/HEAD (the default branch).
func githubTreeViaTarball(ctx context.Context, httpClient *http.Client, owner, repo, ref string) ([]githubTreeEntry, error) {
	refOrHead := ref
	if refOrHead == "" {
		refOrHead = "HEAD"
	}
	tarballURL := fmt.Sprintf("https://codeload.github.com/%s/%s/tar.gz/%s",
		url.PathEscape(owner), url.PathEscape(repo), escapeRefPath(refOrHead))

	resp, err := doImportGet(ctx, httpClient, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, tarballURL, nil)
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("codeload returned HTTP %d for %s/%s@%s", resp.StatusCode, owner, repo, refOrHead)
	}

	gz, err := gzip.NewReader(io.LimitReader(resp.Body, githubTarballMaxCompressed))
	if err != nil {
		return nil, fmt.Errorf("codeload response is not a gzip tarball: %w", err)
	}

	tr := tar.NewReader(gz)
	entries := make([]githubTreeEntry, 0, 64)
	var expanded int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading codeload tarball: %w", err)
		}
		expanded += hdr.Size
		if expanded > githubTarballMaxExpandedSize {
			return nil, fmt.Errorf("%w: repository tarball exceeds %d bytes", errImportSourceUnavailable, githubTarballMaxExpandedSize)
		}
		// Entries are prefixed with the top-level directory ("repo-sha/");
		// strip it so paths match the raw.githubusercontent layout the rest
		// of the import flow builds URLs against.
		_, rest, found := strings.Cut(hdr.Name, "/")
		if !found || rest == "" {
			continue
		}
		if hdr.Typeflag != tar.TypeReg {
			continue // consumers only need regular-file blobs
		}
		entries = append(entries, githubTreeEntry{Path: rest, Type: "blob", Size: hdr.Size})
	}
	return entries, nil
}
