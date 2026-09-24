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

const (
	// Retries back off from retryDelay to retryMaxDelay, so a request succeeds within about
	// retryMaxDelay of the server coming back. retryAttempts at retryMaxDelay is about a
	// minute of retrying.
	retryDelay    = time.Second
	retryMaxDelay = 2 * time.Second
	retryAttempts = 30
)

var ErrTooLarge = errors.New("response too large")

// RetryHttpRequest makes an http request, retrying network errors and 502, 503 and 504
// responses up to retryAttempts times, or until ctx is done. Other non-200 responses are
// returned as an HttpError. The caller must close the response body.
func RetryHttpRequest(ctx context.Context, method, url, cType string, body []byte) (*http.Response, error) {
	return retry.DoWithData(
		func() (*http.Response, error) {
			return doHttpRequest(ctx, method, url, cType, body)
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

			res, err := doHttpRequest(attemptCtx, method, url, cType, body)
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

func doHttpRequest(ctx context.Context, method, url, cType string, body []byte) (*http.Response, error) {
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
	res, err := http.DefaultClient.Do(req)
	if err == nil && res.StatusCode != http.StatusOK {
		err = HttpErrorFromRes(res)
		res.Body.Close()
	}
	return ValOrErr(res, err)
}

func retryOpts(ctx context.Context) []retry.Option {
	return []retry.Option{
		retry.Context(ctx),
		retry.Attempts(retryAttempts),
		retry.Delay(retryDelay),
		retry.MaxDelay(retryMaxDelay),
		// return the last error itself, as UntilSucceeded did, not a retry.Error
		retry.LastErrorOnly(true),
		retry.RetryIf(func(err error) bool {
			// retry on err or some 50x codes
			if !retry.IsRecoverable(err) {
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
