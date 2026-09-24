package ci

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func newCiTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.SetTestTimeout(time.Minute)
	env.RegisterWorkflow(ci)
	return env
}

var testCiArgs = CiArgs{
	Channel:  "nixos-26.05",
	StyxRepo: RepoConfig{Repo: "https://github.com/dnr/styx/", Branch: "release"},
}

// mockPolls makes the pollers find a release and a commit when there's none yet, and
// otherwise nothing new. onPoll is called for each channel poll.
func mockPolls(env *testsuite.TestWorkflowEnvironment, onPoll func()) {
	env.OnActivity(new(activities).PollChannel, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *pollChannelReq) (*pollChannelRes, error) {
			onPoll()
			if req.LastRelID == "" {
				return &pollChannelRes{RelID: "nixos-26.05.1.0123456789ab"}, nil
			}
			return nil, temporal.NewApplicationError("same as before", "retry")
		})
	env.OnActivity(new(activities).PollRepo, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *pollRepoReq) (*pollRepoRes, error) {
			if req.LastCommit == "" {
				return &pollRepoRes{Commit: "0123456789abcdef0123456789abcdef01234567"}, nil
			}
			return nil, temporal.NewApplicationError("same as before", "retry")
		})
}

// Notify had no retry policy, so the default (retry forever) applied and a notification
// that could never be sent blocked the ci loop for good. Now it gives up after a few tries
// and the loop goes back to polling.
func TestCiWorkflowNotifyAlwaysFails(t *testing.T) {
	env := newCiTestEnv(t)
	var notifies, pollsAfterNotify, builds, gcs atomic.Int32
	mockPolls(env, func() {
		if notifies.Load() > 0 {
			pollsAfterNotify.Add(1)
		}
	})
	env.OnActivity(new(heavyActivities).HeavyBuild, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *buildReq) (*buildRes, error) {
			builds.Add(1)
			if !req.SkipGC {
				t.Error("HeavyBuild asked to run GC")
			}
			return &buildRes{Names: []string{"hello-2.12"}}, nil
		})
	env.OnActivity(new(heavyActivities).HeavyGC, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *gcReq) (*gcRes, error) {
			gcs.Add(1)
			return &gcRes{Time: 1, Summary: "gc done"}, nil
		})
	env.OnActivity(new(activities).Notify, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *notifyReq) error {
			notifies.Add(1)
			if req.GCSummary != "gc done" {
				t.Errorf("notify got GC summary %q", req.GCSummary)
			}
			return errors.New("smtp: 535 authentication failed")
		})
	env.RegisterDelayedCallback(env.CancelWorkflow, 6*time.Hour)

	env.ExecuteWorkflow(ci, &testCiArgs)

	require.True(t, env.IsWorkflowCompleted())
	require.True(t, temporal.IsCanceledError(env.GetWorkflowError()), "workflow error: %v", env.GetWorkflowError())
	require.Equal(t, int32(1), builds.Load())
	require.Equal(t, int32(1), gcs.Load())
	require.Equal(t, int32(notifyAttempts), notifies.Load())
	require.Greater(t, pollsAfterNotify.Load(), int32(0), "ci loop did not continue after notify failed")
}

// Cancelled activities and timers return right away, so the ci loop used to spin after a
// cancel until the deadlock detector panicked.
func TestCiWorkflowCancel(t *testing.T) {
	env := newCiTestEnv(t)
	var polls atomic.Int32
	env.OnActivity(new(activities).PollChannel, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *pollChannelReq) (*pollChannelRes, error) {
			polls.Add(1)
			return nil, temporal.NewApplicationError("same as before", "retry")
		})
	env.OnActivity(new(activities).PollRepo, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *pollRepoReq) (*pollRepoRes, error) {
			return nil, temporal.NewApplicationError("same as before", "retry")
		})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Hour)

	env.ExecuteWorkflow(ci, &testCiArgs)

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.True(t, temporal.IsCanceledError(err), "workflow error: %v", err)
	require.Greater(t, polls.Load(), int32(1))
}
