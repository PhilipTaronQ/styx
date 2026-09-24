package resolve

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
