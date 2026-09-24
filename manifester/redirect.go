package manifester

import (
	"log"
	"net/http"
	"slices"
)

// maxRedirects is http.Client's default limit.
const maxRedirects = 10

// newUpstreamClient returns a client for upstream fetches (narinfos, nars and tarballs, and
// the HEAD requests that resolve tarball urls) that follows redirects only to allowedHosts.
// The first url's host is checked separately, when the request comes in.
func newUpstreamClient(allowedHosts []string) *http.Client {
	allowedHosts = slices.Clone(allowedHosts)
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// either way, return the redirect response itself, which
			// RetryHttpRequestWithClient turns into an error without retrying
			if !slices.Contains(allowedHosts, req.URL.Host) {
				log.Printf("not following redirect from %s to disallowed host %q", via[len(via)-1].URL, req.URL.Host)
				return http.ErrUseLastResponse
			} else if len(via) >= maxRedirects {
				log.Printf("not following more than %d redirects from %s", maxRedirects, via[0].URL)
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}
