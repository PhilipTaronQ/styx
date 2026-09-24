package common

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RetryHttpRequest used to retry with retry.Delay(1s) and no MaxDelay, so the delay doubled
// without bound. The kernel read path calls it with context.Background(), so a chunk store
// outage of T left reads blocked for up to about another T after the store came back.
func TestRetryHttpRequestRecoversPromptly(t *testing.T) {
	start := time.Now()
	const recoverAfter = 8 * time.Second

	var mu sync.Mutex
	var attempts []time.Duration
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		el := time.Since(start)
		mu.Lock()
		attempts = append(attempts, el.Round(100*time.Millisecond))
		mu.Unlock()
		if el < recoverAfter {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RetryHttpRequest(ctx, http.MethodGet, srv.URL, "", nil)
	require.NoError(t, err)
	res.Body.Close()

	lag := time.Since(start) - recoverAfter
	mu.Lock()
	defer mu.Unlock()
	require.Less(t, lag, 3*time.Second,
		"server recovered at %v but the request only succeeded %v later; attempts at %v",
		recoverAfter, lag.Round(100*time.Millisecond), attempts)
}

// Errors that aren't retried come back as themselves, not wrapped in a retry.Error.
func TestRetryHttpRequestReturnsHttpError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	_, err := RetryHttpRequest(context.Background(), http.MethodGet, srv.URL, "", nil)
	require.Error(t, err)
	require.True(t, IsNotFound(err), "got %v", err)
	_, ok := err.(HttpError)
	require.True(t, ok, "got %T", err)
}

// A stalled attempt is abandoned after attemptTimeout and retried.
func TestRetryHttpRequestBodyRetriesStalledAttempt(t *testing.T) {
	var calls atomic.Int32
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			// send headers and part of the body, then stall
			w.Write([]byte("par"))
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
			case <-done:
			}
			return
		}
		w.Write([]byte("whole body"))
	}))
	defer srv.Close()
	defer close(done) // before srv.Close, which waits for handlers

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, _, err := RetryHttpRequestBody(ctx, http.MethodGet, srv.URL, "", nil, 100, 500*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, "whole body", string(b))
	require.EqualValues(t, 2, calls.Load())
}

// A body over the limit fails at once, without reading the rest or retrying.
func TestRetryHttpRequestBodyLimit(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write(make([]byte, 1000))
	}))
	defer srv.Close()

	_, _, err := RetryHttpRequestBody(context.Background(), http.MethodGet, srv.URL, "", nil, 999, 0)
	require.ErrorIs(t, err, ErrTooLarge)
	require.EqualValues(t, 1, calls.Load())

	b, _, err := RetryHttpRequestBody(context.Background(), http.MethodGet, srv.URL, "", nil, 1000, 0)
	require.NoError(t, err)
	require.Len(t, b, 1000)
}

// Retrying stops once the budget is spent, whatever the attempt count: a server that
// always fails fast gets many attempts, but the request still returns in about the budget.
func TestRetryHttpRequestGivesUpAfterBudget(t *testing.T) {
	defer func(b time.Duration) { retryBudget = b }(retryBudget)
	retryBudget = 3 * time.Second

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	start := time.Now()
	_, err := RetryHttpRequest(context.Background(), http.MethodGet, srv.URL, "", nil)
	el := time.Since(start)
	require.Error(t, err)
	herr, ok := err.(HttpError)
	require.True(t, ok, "got %T %v", err, err)
	require.Equal(t, http.StatusServiceUnavailable, herr.Code())
	require.GreaterOrEqual(t, el, retryBudget)
	require.Less(t, el, retryBudget+retryMaxDelay+time.Second)
	require.Greater(t, calls.Load(), int32(1))
}

// With an attempt timeout, a server that keeps stalling fails the request within the
// budget plus one attempt.
func TestRetryHttpRequestBodyStalledServerIsBounded(t *testing.T) {
	defer func(b time.Duration) { retryBudget = b }(retryBudget)
	retryBudget = 2 * time.Second
	const attempt = 500 * time.Millisecond

	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-done:
		}
	}))
	defer srv.Close()
	defer close(done) // before srv.Close, which waits for handlers

	start := time.Now()
	_, _, err := RetryHttpRequestBody(context.Background(), http.MethodGet, srv.URL, "", nil, 100, attempt)
	el := time.Since(start)
	require.ErrorContains(t, err, "attempt timed out")
	require.Less(t, el, retryBudget+attempt+retryMaxDelay+time.Second)
}
