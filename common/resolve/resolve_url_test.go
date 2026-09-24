package resolve

import (
	"context"
	"strings"
	"testing"

	"github.com/nix-community/go-nix/pkg/storepath"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getSpNameFromUrl used to take path.Base of the whole resolved URL, query included.
// Redirects to signed URLs (e.g. GitHub release assets) produced names with '&' and '%'
// (invalid in store paths) that also changed on every request.
func TestSpNameFromSignedRedirectUrl(t *testing.T) {
	u := "https://release-assets.githubusercontent.com/github-production-release-asset/123456/" +
		"0f1e2d3c-aaaa-bbbb-cccc-1234567890ab?sp=r&sv=2018-11-09&sr=b&spr=https" +
		"&se=2026-09-24T12%3A00%3A00Z&rscd=attachment%3B+filename%3Dfoo-1.0.tar.gz" +
		"&rsct=application%2Foctet-stream&sig=abcDEF%2B%2Fxyz%3D"
	name := getSpNameFromUrl(u)
	assert.Equal(t, name, storepath.NameRe.FindString(name), "name %q is not a valid store path name", name)
	assert.NotContains(t, name, "sig=", "name %q depends on the URL signature", name)

	assert.Equal(t, "foo-1.0", getSpNameFromUrl("https://example.org/dl/foo-1.0.tar.gz?token=abc"))
	assert.Equal(t, "source", getSpNameFromUrl("https://example.org/"))
}

// If a forge handler matches but resolving the ref fails, ResolveUrl used to ignore that
// error, "re-resolve" the empty Result and report "reresolve: no match" instead.
func TestResolveUrlKeepsHandlerError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // make resolveGitRef fail without touching the network
	_, err := ResolveUrl(ctx, "https://github.com/NixOS/nixpkgs/archive/nixos-unstable.tar.gz")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "context canceled"),
		"handler error was replaced: %v", err)
}
