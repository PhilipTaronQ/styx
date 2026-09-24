package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/avast/retry-go/v4"
)

// Timeouts for requests to the chunk store, chunk differ and manifester come in three
// layers, each bounded separately:
//
//  1. An attempt. Requests for small, bounded bodies (chunks, manifest cache entries) give
//     each attempt a timeout (manifester.ReadAttemptTimeout), so a server that stalls is
//     retried. Streaming requests (chunk diffs, manifest builds, nar and tarball downloads)
//     have none, since how long they take depends on the data; their caller bounds them.
//  2. Retrying. RetryHttpRequest and RetryHttpRequestBody retry transient failures for
//     RetryBudget and don't start an attempt after that, so a request with an attempt
//     timeout of T returns within RetryBudget + T, however the server fails.
//  3. The operation, set by the caller's ctx. The daemon's kernel reads have a deadline
//     (daemon's slabReadTimeout) that is at least RetryBudget + ReadAttemptTimeout, so it
//     never cuts a chunk read's retrying short; it's what bounds the chunk diffs and the
//     remanifests those reads wait on.
const (
	// About two minutes of retrying rides out a restart or a short outage of the chunk
	// store; a longer one fails reads rather than hanging them.
	RetryBudget = 2 * time.Minute

	// Retries back off from retryDelay to retryMaxDelay, so a request succeeds within about
	// retryMaxDelay of the server coming back.
	retryDelay    = time.Second
	retryMaxDelay = 2 * time.Second
)

// retryBudget is RetryBudget, shortened by tests.
var retryBudget = RetryBudget

var ErrTooLarge = errors.New("response too large")

// RetryHttpRequest makes an http request, retrying network errors and 502, 503 and 504
// responses for RetryBudget, or until ctx is done. Other non-200 responses are returned as
// an HttpError. The caller must close the response body.
func RetryHttpRequest(ctx context.Context, method, url, cType string, body []byte) (*http.Response, error) {
	return RetryHttpRequestWithClient(ctx, http.DefaultClient, method, url, cType, body)
}

// RetryHttpRequestWithClient is RetryHttpRequest using client, with the same retries and
// budget.
func RetryHttpRequestWithClient(ctx context.Context, client *http.Client, method, url, cType string, body []byte) (*http.Response, error) {
	return retry.DoWithData(
		func() (*http.Response, error) {
			return doHttpRequest(ctx, client, method, url, cType, body)
		},
		retryOpts(ctx)...,
	)
}

// RetryHttpRequestBody is like RetryHttpRequest but also reads the response body, retrying
// if that fails. It fails without retrying if the body is longer than maxBytes. If
// attemptTimeout is positive, each attempt, including reading the body, gets that long, and
// an attempt that times out is retried.
func RetryHttpRequestBody(
	ctx context.Context,
	method, url, cType string,
	body []byte,
	maxBytes int64,
	attemptTimeout time.Duration,
) ([]byte, http.Header, error) {
	type result struct {
		body   []byte
		header http.Header
	}
	res, err := retry.DoWithData(
		func() (result, error) {
			attemptCtx, cancel := ctx, context.CancelFunc(func() {})
			if attemptTimeout > 0 {
				attemptCtx, cancel = context.WithTimeout(ctx, attemptTimeout)
			}
			defer cancel()

			res, err := doHttpRequest(attemptCtx, http.DefaultClient, method, url, cType, body)
			if err == nil {
				defer res.Body.Close()
				var b []byte
				if b, err = ReadAllLimit(res.Body, maxBytes); err == nil {
					return result{body: b, header: res.Header}, nil
				} else if errors.Is(err, ErrTooLarge) {
					return result{}, retry.Unrecoverable(err)
				}
			}
			if ctx.Err() == nil && attemptCtx.Err() != nil {
				// only this attempt timed out: return an error that isn't a context error so
				// that it's retried
				err = fmt.Errorf("attempt timed out after %v: %v", attemptTimeout, err)
			}
			return result{}, err
		},
		retryOpts(ctx)...,
	)
	return res.body, res.header, err
}

func doHttpRequest(ctx context.Context, client *http.Client, method, url, cType string, body []byte) (*http.Response, error) {
	var bReader io.Reader
	if body != nil {
		bReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bReader)
	if err != nil {
		return nil, retry.Unrecoverable(err)
	}
	if cType != "" {
		req.Header.Set("Content-Type", cType)
	}
	res, err := client.Do(req)
	if err == nil && res.StatusCode != http.StatusOK {
		err = HttpErrorFromRes(res)
		res.Body.Close()
	}
	return ValOrErr(res, err)
}

func retryOpts(ctx context.Context) []retry.Option {
	deadline := time.Now().Add(retryBudget)
	return []retry.Option{
		retry.Context(ctx),
		// the budget runs out first: attempts are at least retryDelay apart
		retry.Attempts(uint(retryBudget/retryDelay) + 1),
		retry.Delay(retryDelay),
		retry.MaxDelay(retryMaxDelay),
		// return the last error itself, as UntilSucceeded did, not a retry.Error
		retry.LastErrorOnly(true),
		retry.RetryIf(func(err error) bool {
			// retry on err or some 50x codes
			if time.Now().After(deadline) {
				return false
			} else if !retry.IsRecoverable(err) {
				return false
			} else if status, ok := err.(HttpError); ok {
				switch status.Code() {
				case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
					return true
				default:
					return false
				}
			} else if IsContextError(err) {
				return false
			}
			return true
		}),
		retry.OnRetry(func(n uint, err error) {
			log.Printf("http error (%d): %v, retrying", n, err)
		}),
	}
}

// ReadAllLimit reads all of r, failing with ErrTooLarge if there are more than limit bytes.
func ReadAllLimit(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	} else if int64(len(b)) > limit {
		return nil, fmt.Errorf("%w: more than %d bytes", ErrTooLarge, limit)
	}
	return b, nil
}
