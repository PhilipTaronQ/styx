package manifester

import (
	"context"
	"errors"
	"log"
	"net/http"
	"slices"
)

// Upstream requests go through common.RetryHttpRequest, which uses http.DefaultClient and
// follows redirects to any host. The server puts its upstream allow-list in the request
// context, and this CheckRedirect applies it to every redirect. Requests without an
// allow-list in their context are unaffected.

type allowedHostsKey struct{}

func withAllowedHosts(ctx context.Context, hosts []string) context.Context {
	return context.WithValue(ctx, allowedHostsKey{}, hosts)
}

func init() {
	next := http.DefaultClient.CheckRedirect
	http.DefaultClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if hosts, ok := req.Context().Value(allowedHostsKey{}).([]string); ok && !slices.Contains(hosts, req.URL.Host) {
			log.Printf("not following redirect from %s to disallowed host %q", via[len(via)-1].URL, req.URL.Host)
			// return the redirect response itself, which RetryHttpRequest turns into an
			// error without retrying
			return http.ErrUseLastResponse
		}
		if next != nil {
			return next(req, via)
		} else if len(via) >= 10 { // the default policy
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
}
