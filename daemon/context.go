package daemon

import (
	"context"
	"sync"
)

type (
	allocateContext struct {
		sph         Sph
		forManifest bool
	}

	mountContext struct {
		// Written by tryMount and read by handleOpenImage on the cachefiles
		// goroutine. The kernel orders those (the open only arrives after
		// mount(2)), but that's invisible to the Go memory model, so lock.
		lock      sync.Mutex
		imageSize int64
		isBare    bool
		imageData []byte
	}

	daemonCtxKey int
)

var (
	allocateCtxKey any = daemonCtxKey(1)
	mountCtxKey    any = daemonCtxKey(2)
)

func withAllocateCtx(ctx context.Context, sph Sph, forManifest bool) context.Context {
	return context.WithValue(ctx, allocateCtxKey, allocateContext{sph: sph, forManifest: forManifest})
}

func fromAllocateCtx(ctx context.Context) (Sph, bool, bool) {
	actx, ok := ctx.Value(allocateCtxKey).(allocateContext)
	return actx.sph, actx.forManifest, ok
}

func withMountContext(ctx context.Context, mctx *mountContext) context.Context {
	return context.WithValue(ctx, mountCtxKey, mctx)
}

func fromMountCtx(ctx context.Context) (*mountContext, bool) {
	mctx, ok := ctx.Value(mountCtxKey).(*mountContext)
	return mctx, ok
}
