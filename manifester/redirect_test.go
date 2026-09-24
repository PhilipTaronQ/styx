package manifester

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common"
)

// Every hop of a redirect chain is checked against the allow-list, not just the first url
// or the last.
func TestUpstreamClientChecksEveryRedirect(t *testing.T) {
	a, b, evil := newFakeUpstream(t), newFakeUpstream(t), newFakeUpstream(t)
	client := newUpstreamClient([]string{a.host(), b.host()})
	get := func(u string) (string, error) {
		res, err := common.RetryHttpRequestWithClient(context.Background(), client, http.MethodGet, u, "", nil)
		if err != nil {
			return "", err
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		return string(body), err
	}

	// a -> b -> a -> b, all allowed
	b.set("/final", []byte("hello"))
	a.redirect("/start", b.url()+"hop1")
	b.redirect("/hop1", a.url()+"hop2")
	a.redirect("/hop2", b.url()+"final")
	body, err := get(a.url() + "start")
	require.NoError(t, err)
	assert.Equal(t, "hello", body)

	// a -> b -> evil -> a: refused at the disallowed hop, without retrying
	evil.redirect("/hop", a.url()+"x")
	a.set("/x", []byte("unreachable"))
	a.redirect("/start2", b.url()+"hop3")
	b.redirect("/hop3", evil.url()+"hop")
	_, err = get(a.url() + "start2")
	assert.Error(t, err)
	assert.Zero(t, evil.requestCount(), "followed a redirect to a disallowed host")

	// too many redirects fail without retrying too
	for i := range maxRedirects + 1 {
		a.redirect(fmt.Sprintf("/loop%d", i), a.url()+fmt.Sprintf("loop%d", i+1))
	}
	before := a.requestCount()
	_, err = get(a.url() + "loop0")
	assert.Error(t, err)
	assert.Equal(t, maxRedirects, a.requestCount()-before, "retried or followed too many redirects")
}

// The redirect check lives in the builder's own client, made from its config: making a
// server doesn't change the builder, and http.DefaultClient, which the daemon and everything
// else use, is left alone.
func TestUpstreamClientLeavesDefaultClientAlone(t *testing.T) {
	up := newFakeUpstream(t)
	cs := &mockChunkStore{data: make(map[string][]byte)}
	_, pk := upstreamKeys(t)

	mb := newTestBuilder(t, cs, pk, 0, up.host())
	assert.NotSame(t, http.DefaultClient, mb.upstreamClient)
	assert.NotNil(t, mb.upstreamClient.CheckRedirect)
	client := mb.upstreamClient
	_, err := NewManifestServer(Config{}, mb)
	require.NoError(t, err)
	assert.Same(t, client, mb.upstreamClient, "NewManifestServer changed the builder's client")
	assert.Nil(t, http.DefaultClient.CheckRedirect)

	// a builder that doesn't serve requests follows redirects as usual
	plain := newTestBuilder(t, cs, pk, 0)
	assert.Same(t, http.DefaultClient, plain.upstreamClient)
}
