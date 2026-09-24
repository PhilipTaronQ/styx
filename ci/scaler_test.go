package ci

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/stretchr/testify/require"
)

type fakeASG struct {
	fail  int // fail this many calls first
	calls []int32
}

func (f *fakeASG) SetDesiredCapacity(ctx context.Context, in *autoscaling.SetDesiredCapacityInput, _ ...func(*autoscaling.Options)) (*autoscaling.SetDesiredCapacityOutput, error) {
	f.calls = append(f.calls, aws.ToInt32(in.DesiredCapacity))
	if f.fail > 0 {
		f.fail--
		return nil, errors.New("throttled")
	}
	return &autoscaling.SetDesiredCapacityOutput{}, nil
}

// The scaler used to record a failed capacity change as done, so it never retried: a
// failed scale-down left the spot instance running until the next change of target.
func TestScalerRetriesFailedCapacityChange(t *testing.T) {
	asg := &fakeASG{fail: 1}
	s := &scaler{prev: -1, asgcli: asg, notifyCh: make(chan struct{}, 1)}

	s.update(scalerInfo{scheduled: 1})
	require.Equal(t, -1, s.prev)
	s.update(scalerInfo{scheduled: 1})
	require.Equal(t, 1, s.prev)
	s.update(scalerInfo{started: 1})
	require.Equal(t, []int32{1, 1}, asg.calls)

	// and scaling down
	asg.fail = 1
	s.update(scalerInfo{})
	s.update(scalerInfo{})
	require.Equal(t, 0, s.prev)
	s.update(scalerInfo{})
	require.Equal(t, []int32{1, 1, 0, 0}, asg.calls)
}

// poke is called from workflow code, where blocking trips the deadlock detector. It used to
// block once a poke was pending and the scaler was busy.
func TestScalerPokeDoesNotBlock(t *testing.T) {
	s := &scaler{notifyCh: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 3 {
			s.poke()
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("poke blocked")
	}
	require.Len(t, s.notifyCh, 1)
}
