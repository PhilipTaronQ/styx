package ci

import (
	"context"
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
