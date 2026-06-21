// Copyright (c) 2015-present Jeevanandam M (jeeva@myjeeva.com), All rights reserved.
// resty source code and usage is governed by a MIT style
// license that can be found in the LICENSE file.
// SPDX-License-Identifier: MIT

package resty

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimiterTokenBucket(t *testing.T) {
	t.Run("allow", func(t *testing.T) {
		l := NewRateLimitTokenBucket(100, 5)
		for i := range 5 {
			err := l.Allow(context.Background())
			assertNil(t, err, fmt.Sprintf("unexpected error on iteration %d", i))
		}
	})

	t.Run("burst depletes tokens", func(t *testing.T) {
		// 1 token/s, burst 1 -> after 1 call tokens are 0
		l := NewRateLimitTokenBucket(1, 1)

		// First call should succeed immediately (burst token available).
		err := l.Allow(context.Background())
		assertNil(t, err)

		// Second call: no token available, context with very short deadline should time out.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		err = l.Allow(ctx)
		assertErrorIs(t, ErrRateLimitExceeded, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		// rate=1/s, burst=1 -> drain burst first, then cancel
		l := NewRateLimitTokenBucket(1, 1)
		_ = l.Allow(context.Background()) // drain the single burst token

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled

		err := l.Allow(ctx)
		assertErrorIs(t, ErrRateLimitExceeded, err)
	})

	t.Run("refills over time", func(t *testing.T) {
		// 10 requests/s -> 1 token every 100 ms
		l := NewRateLimitTokenBucket(10, 1)

		// Drain burst.
		err := l.Allow(context.Background())
		assertNil(t, err)

		// Wait for one token to refill, then allow should succeed.
		time.Sleep(120 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err = l.Allow(ctx)
		assertNil(t, err, "expected token to be available after refill interval")
	})
}

func TestRateLimiterTokenBucketConfig(t *testing.T) {
	t.Run("rate and burst accessors", func(t *testing.T) {
		l := NewRateLimitTokenBucket(42.5, 7)
		assertEqual(t, 42.5, l.Rate(), "unexpected rate value")
		assertEqual(t, 7, l.Burst(), "unexpected burst")
	})

	t.Run("defaults on invalid rate", func(t *testing.T) {
		l := NewRateLimitTokenBucket(0, 3)
		assertEqual(t, 5.0, l.Rate(), "unexpected default rate")
		assertEqual(t, 3, l.Burst(), "unexpected burst")
	})

	t.Run("defaults on invalid burst", func(t *testing.T) {
		l := NewRateLimitTokenBucket(10, 0)
		assertEqual(t, 10.0, l.Rate(), "unexpected rate")
		assertEqual(t, 1, l.Burst(), "unexpected default burst")
	})
}

func TestClientRateLimiterTokenBucket(t *testing.T) {
	t.Run("set/get/clear rate limiter", func(t *testing.T) {
		c := dcnl()
		assertNil(t, c.RateLimiter(), "expected nil rate limiter initially")

		l := NewRateLimitTokenBucket(50, 5)
		c.SetRateLimiter(l)
		assertEqual(t, l, c.RateLimiter(), "expected rate limiter to be set")

		c.SetRateLimiter(nil)
		assertNil(t, c.RateLimiter(), "expected nil after clearing rate limiter")
	})

	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer ts.Close()

	t.Run("throttles requests", func(t *testing.T) {
		// rate=1/s, burst=1 -> only 1 instant request allowed
		l := NewRateLimitTokenBucket(1, 1)
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(l)

		// First request: burst token available, should succeed.
		resp, err := c.R().Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())

		// Second request with a short-deadline context: burst exhausted, must be rejected.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		resp2, err2 := c.R().SetContext(ctx).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err2)
		assertNil(t, resp2)
	})

	t.Run("allows after refill", func(t *testing.T) {
		// 10 req/s -> 1 token every 100 ms, burst 1
		l := NewRateLimitTokenBucket(10, 1)
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(l)

		// Drain burst.
		_, err := c.R().Get(ts.URL)
		assertNil(t, err)

		// Wait for one token to refill.
		time.Sleep(120 * time.Millisecond)

		resp, err := c.R().Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())
	})

	t.Run("custom implementation", func(t *testing.T) {
		var allowCalls atomic.Int32
		cl := &customTestLimiter{allow: true, calls: &allowCalls}
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(cl)

		_, err := c.R().Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, int32(1), allowCalls.Load(), "expected Allow to be called once")

		// Now reject all requests.
		cl.allow = false
		_, err = c.R().Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err)
		assertEqual(t, int32(2), allowCalls.Load(), "expected Allow to be called twice")
	})
}

type customTestLimiter struct {
	allow bool
	calls *atomic.Int32
}

func (l *customTestLimiter) Allow(_ context.Context) error {
	l.calls.Add(1)
	if !l.allow {
		return ErrRateLimitExceeded
	}
	return nil
}

func TestRateLimiterSlidingWindow(t *testing.T) {
	t.Run("allow", func(t *testing.T) {
		l := NewRateLimitSlidingWindow(5, time.Second)
		for i := range 5 {
			err := l.Allow(context.Background())
			assertNil(t, err, fmt.Sprintf("unexpected error on iteration %d: %v", i, err))
		}
	})

	t.Run("limit exhausted", func(t *testing.T) {
		// 2 requests per second window; drain both slots immediately.
		l := NewRateLimitSlidingWindow(2, time.Second)
		assertNil(t, l.Allow(context.Background()))
		assertNil(t, l.Allow(context.Background()))

		// Third request: window is full, short deadline must be rejected.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		err := l.Allow(ctx)
		assertErrorIs(t, ErrRateLimitExceeded, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		l := NewRateLimitSlidingWindow(1, time.Second)
		assertNil(t, l.Allow(context.Background())) // drain the single slot

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled
		err := l.Allow(ctx)
		assertErrorIs(t, ErrRateLimitExceeded, err)
	})

	t.Run("slides over time", func(t *testing.T) {
		// Window of 100 ms, limit 1: after the first request, wait >100 ms and
		// the slot should become available again.
		l := NewRateLimitSlidingWindow(1, 100*time.Millisecond)
		assertNil(t, l.Allow(context.Background()))

		time.Sleep(120 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := l.Allow(ctx)
		assertNil(t, err, "expected slot to be available after window slides")
	})

	t.Run("throttles", func(t *testing.T) {
		ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		defer ts.Close()

		// limit=1, window=1s → only 1 instant request allowed
		l := NewRateLimitSlidingWindow(1, time.Second)
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(l)

		resp, err := c.R().Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		resp2, err2 := c.R().SetContext(ctx).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err2)
		assertNil(t, resp2)
	})
}

func TestRateLimiterSlidingWindowConfig(t *testing.T) {
	t.Run("accessors", func(t *testing.T) {
		l := NewRateLimitSlidingWindow(42, 5*time.Second)
		assertEqual(t, 42, l.Limit(), "unexpected limit value")
		assertEqual(t, 5*time.Second, l.WindowSize(), "unexpected window size")
	})

	t.Run("defaults", func(t *testing.T) {
		l := NewRateLimitSlidingWindow(0, 0)
		assertEqual(t, 5, l.Limit(), "expected default limit of 5")
		assertEqual(t, time.Second, l.WindowSize(), "expected default window of 1s")
	})
}

func TestRequestRateLimiter(t *testing.T) {
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer ts.Close()

	t.Run("set/get rate limiter on request", func(t *testing.T) {
		c := dcnl()
		r := c.R()
		assertNil(t, r.RateLimiter(), "expected nil rate limiter on request initially")

		l := NewRateLimitTokenBucket(10, 1)
		r.SetRateLimiter(l)
		assertEqual(t, l, r.RateLimiter(), "expected rate limiter to be set on request")
	})

	t.Run("request level rate limiter throttles", func(t *testing.T) {
		l := NewRateLimitTokenBucket(1, 1)
		c := dcnl().SetBaseURL(ts.URL)

		resp, err := c.R().SetRateLimiter(l).Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		resp2, err2 := c.R().SetRateLimiter(l).SetContext(ctx).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err2)
		assertNil(t, resp2)
	})

	t.Run("request level limiter takes precedence when set", func(t *testing.T) {
		var clientCalls, reqCalls atomic.Int32
		clientRL := &trackingLimiter{allow: true, calls: &clientCalls}
		reqRL := &trackingLimiter{allow: true, calls: &reqCalls}
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(clientRL)

		resp, err := c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())
		assertEqual(t, int32(1), clientCalls.Load(), "client RL should be called")
		assertEqual(t, int32(1), reqCalls.Load(), "request RL should be called")
	})

	t.Run("client only when request level not set", func(t *testing.T) {
		var clientCalls atomic.Int32
		clientRL := &trackingLimiter{allow: true, calls: &clientCalls}
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(clientRL)

		resp, err := c.R().Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())
		assertEqual(t, int32(1), clientCalls.Load(), "client RL should be called")
	})

	t.Run("request limiter rejection does not consume client limiter", func(t *testing.T) {
		var clientCalls atomic.Int32
		clientRL := &trackingLimiter{allow: true, calls: &clientCalls}
		reqRL := NewRateLimitTokenBucket(1, 1)
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(clientRL)

		resp, err := c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		resp2, err2 := c.R().SetRateLimiter(reqRL).SetContext(ctx).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err2)
		assertNil(t, resp2)
		assertEqual(t, int32(1), clientCalls.Load(), "client RL should only be called once (first successful request)")
	})

	t.Run("rate limit rejection is ErrRateLimitExceeded", func(t *testing.T) {
		reqRL := &trackingLimiter{allow: false, calls: &atomic.Int32{}}
		c := dcnl().SetBaseURL(ts.URL)

		_, err := c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err)
	})

	t.Run("rate limit rejection is not ErrCircuitBreakerOpen", func(t *testing.T) {
		reqRL := &trackingLimiter{allow: false, calls: &atomic.Int32{}}
		cb := NewCircuitBreakerCount(5, 1, 30*time.Second)
		c := dcnl().SetBaseURL(ts.URL).SetCircuitBreaker(cb)

		_, err := c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err)
		assertNotNil(t, err)
		if errors.Is(err, ErrCircuitBreakerOpen) {
			t.Fatal("rate limit error must not be wrapped as circuit breaker open")
		}
	})

	t.Run("rate limit rejection uses onInvalid hooks not onError", func(t *testing.T) {
		reqRL := &trackingLimiter{allow: false, calls: &atomic.Int32{}}
		var invalidCalled, errorCalled atomic.Int32
		c := dcnl().SetBaseURL(ts.URL).
			OnInvalid(func(r *Request, err error) { invalidCalled.Add(1) }).
			OnError(func(r *Request, err error) { errorCalled.Add(1) })

		_, err := c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err)
		assertEqual(t, int32(1), invalidCalled.Load(), "onInvalid should be called")
		assertEqual(t, int32(0), errorCalled.Load(), "onError should not be called")
	})

	t.Run("rate limit rejection does not pollute circuit breaker", func(t *testing.T) {
		reqRL := &trackingLimiter{allow: false, calls: &atomic.Int32{}}
		cb := NewCircuitBreakerCount(2, 1, 30*time.Second)
		c := dcnl().SetBaseURL(ts.URL).SetCircuitBreaker(cb)

		for range 5 {
			_, _ = c.R().SetRateLimiter(reqRL).Get(ts.URL)
		}

		_, err := c.R().Get(ts.URL)
		assertNil(t, err, "circuit breaker should still be closed; rate limit rejections must not count as CB failures")
	})

	t.Run("rate limit rejection does not send load balancer feedback", func(t *testing.T) {
		reqRL := &trackingLimiter{allow: false, calls: &atomic.Int32{}}
		var feedbackCalls atomic.Int32
		lb := &testLoadBalancer{feedbackCalls: &feedbackCalls, baseURL: ts.URL}
		c := dcnl().SetBaseURL(ts.URL).SetLoadBalancer(lb)

		_, _ = c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertEqual(t, int32(0), feedbackCalls.Load(), "load balancer feedback should not be called on rate limit rejection")
	})

	t.Run("no snapshot when no rate limiter configured", func(t *testing.T) {
		c := dcnl().SetBaseURL(ts.URL)

		resp, err := c.R().Get(ts.URL)
		assertNil(t, err)
		assertNil(t, resp.RateLimiterSnapshot, "snapshot should be nil when no rate limiter is configured")
	})

	t.Run("snapshot present with client rate limiter", func(t *testing.T) {
		l := NewRateLimitTokenBucket(100, 5)
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(l)

		resp, err := c.R().Get(ts.URL)
		assertNil(t, err)
		assertNotNil(t, resp.RateLimiterSnapshot, "snapshot should not be nil")
		assertNotNil(t, resp.RateLimiterSnapshot.Client, "client stats should not be nil")
		assertNil(t, resp.RateLimiterSnapshot.Request, "request stats should be nil when only client RL is set")
		assertEqual(t, 1, resp.RateLimiterSnapshot.Client.WaitCount, "wait count should be 1")
	})

	t.Run("snapshot present with request rate limiter", func(t *testing.T) {
		l := NewRateLimitTokenBucket(100, 5)
		c := dcnl().SetBaseURL(ts.URL)

		resp, err := c.R().SetRateLimiter(l).Get(ts.URL)
		assertNil(t, err)
		assertNotNil(t, resp.RateLimiterSnapshot, "snapshot should not be nil")
		assertNil(t, resp.RateLimiterSnapshot.Client, "client stats should be nil when only request RL is set")
		assertNotNil(t, resp.RateLimiterSnapshot.Request, "request stats should not be nil")
		assertEqual(t, 1, resp.RateLimiterSnapshot.Request.WaitCount, "wait count should be 1")
	})

	t.Run("snapshot with both rate limiters", func(t *testing.T) {
		clientRL := NewRateLimitTokenBucket(100, 5)
		reqRL := NewRateLimitTokenBucket(100, 5)
		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(clientRL)

		resp, err := c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertNil(t, err)
		assertNotNil(t, resp.RateLimiterSnapshot, "snapshot should not be nil")
		assertNotNil(t, resp.RateLimiterSnapshot.Client, "client stats should not be nil")
		assertNotNil(t, resp.RateLimiterSnapshot.Request, "request stats should not be nil")
		assertEqual(t, 1, resp.RateLimiterSnapshot.Client.WaitCount, "client wait count should be 1")
		assertEqual(t, 1, resp.RateLimiterSnapshot.Request.WaitCount, "request wait count should be 1")
	})

	t.Run("snapshot tracks cumulative wait across retries", func(t *testing.T) {
		var callCount atomic.Int32
		reqRL := &trackingLimiter{
			allow:     true,
			calls:     &callCount,
			blockTime: 50 * time.Millisecond,
		}

		c := dcnl().SetBaseURL(ts.URL)
		var attemptCount atomic.Int32
		resp, _ := c.R().
			SetRateLimiter(reqRL).
			SetRetryCount(2).
			SetRetryDefaultConditions(false).
			AddRetryConditions(func(resp *Response, err error) bool {
				if attemptCount.Add(1) <= 2 {
					return true
				}
				return false
			}).
			Get(ts.URL)

		assertNotNil(t, resp, "response should not be nil")
		assertNotNil(t, resp.RateLimiterSnapshot, "snapshot should not be nil")
		assertNotNil(t, resp.RateLimiterSnapshot.Request, "request stats should not be nil")
		assertEqual(t, 3, resp.RateLimiterSnapshot.Request.WaitCount, "request RL should be called 3 times (1 initial + 2 retries)")
		assertEqual(t, int32(3), callCount.Load(), "tracking limiter should be called 3 times")
	})

	t.Run("rate limiter order: request RL then client RL then circuit breaker", func(t *testing.T) {
		var callOrder atomic.Int32
		reqRL := &orderTrackingLimiter{order: &callOrder, slot: 1}
		clientRL := &orderTrackingLimiter{order: &callOrder, slot: 2}
		cb := NewCircuitBreakerCount(5, 1, 30*time.Second)

		c := dcnl().SetBaseURL(ts.URL).SetRateLimiter(clientRL).SetCircuitBreaker(cb)

		resp, err := c.R().SetRateLimiter(reqRL).Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())
	})

	t.Run("circuit breaker open does not consume rate limiter when CB is checked after", func(t *testing.T) {
		clientRL := NewRateLimitTokenBucket(1, 1)
		cb := NewCircuitBreakerCount(1, 1, 30*time.Second)

		server500 := createTestServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})
		defer server500.Close()

		c := dcnl().SetBaseURL(server500.URL).SetRateLimiter(clientRL).SetCircuitBreaker(cb)

		_, _ = c.R().Get(server500.URL)

		_, err := c.R().Get(server500.URL)
		assertErrorIs(t, ErrCircuitBreakerOpen, err)
	})

	t.Run("context cancellation during rate limit wait", func(t *testing.T) {
		rl := NewRateLimitTokenBucket(1, 1)
		c := dcnl().SetBaseURL(ts.URL)

		resp, err := c.R().SetRateLimiter(rl).Get(ts.URL)
		assertNil(t, err)
		assertNotNil(t, resp.RateLimiterSnapshot)
		assertEqual(t, 1, resp.RateLimiterSnapshot.Request.WaitCount, "first call should have wait count 1")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err = c.R().SetRateLimiter(rl).SetContext(ctx).Get(ts.URL)
		assertErrorIs(t, ErrRateLimitExceeded, err)
	})

	t.Run("rate limiter not blocking other requests", func(t *testing.T) {
		slowRL := NewRateLimitTokenBucket(1, 1)
		fastRL := NewRateLimitTokenBucket(100, 10)
		c := dcnl().SetBaseURL(ts.URL)

		done1 := make(chan struct{})
		go func() {
			defer close(done1)
			resp, err := c.R().SetRateLimiter(slowRL).Get(ts.URL)
			assertNil(t, err)
			assertEqual(t, http.StatusOK, resp.StatusCode())
		}()

		resp, err := c.R().SetRateLimiter(fastRL).Get(ts.URL)
		assertNil(t, err)
		assertEqual(t, http.StatusOK, resp.StatusCode())

		<-done1
	})

	t.Run("clone preserves rate limiter", func(t *testing.T) {
		l := NewRateLimitTokenBucket(100, 5)
		c := dcnl().SetBaseURL(ts.URL)

		r1 := c.R().SetRateLimiter(l)
		r2 := r1.Clone(context.Background())
		assertEqual(t, l, r2.RateLimiter(), "cloned request should have the same rate limiter")
	})
}

type trackingLimiter struct {
	allow     bool
	calls     *atomic.Int32
	blockTime time.Duration
}

func (l *trackingLimiter) Allow(ctx context.Context) error {
	l.calls.Add(1)
	if l.blockTime > 0 {
		select {
		case <-time.After(l.blockTime):
		case <-ctx.Done():
			return ErrRateLimitExceeded
		}
	}
	if !l.allow {
		return ErrRateLimitExceeded
	}
	return nil
}

type orderTrackingLimiter struct {
	order *atomic.Int32
	slot  int32
}

func (l *orderTrackingLimiter) Allow(_ context.Context) error {
	l.order.Store(l.slot)
	return nil
}

type testLoadBalancer struct {
	feedbackCalls *atomic.Int32
	baseURL       string
}

func (lb *testLoadBalancer) NextWithContext(_ context.Context) (string, error) {
	return lb.baseURL, nil
}

func (lb *testLoadBalancer) Feedback(_ *RequestFeedback) {
	lb.feedbackCalls.Add(1)
}

func (lb *testLoadBalancer) Close() error { return nil }
