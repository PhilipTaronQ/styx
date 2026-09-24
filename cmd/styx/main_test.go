package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const sigtermChildEnv = "STYX_TEST_SIGTERM_CHILD"

// If Stop hangs after the first SIGTERM, a second SIGTERM must still kill the daemon.
func TestSecondSigtermExits(t *testing.T) {
	if os.Getenv(sigtermChildEnv) != "" {
		fmt.Println("ready")
		stopOnSigterm(context.Background(), func() {
			fmt.Println("stopping")
			time.Sleep(time.Hour) // a Stop that hangs
		})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestSecondSigtermExits$")
	cmd.Env = append(os.Environ(), sigtermChildEnv+"=1")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	lines := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
		done <- cmd.Wait()
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-done
	})

	expect := func(want string) {
		t.Helper()
		timeout := time.After(10 * time.Second)
		for {
			select {
			case line, ok := <-lines:
				require.True(t, ok, "child exited before printing %q", want)
				if line == want {
					return
				}
			case <-timeout:
				t.Fatalf("child did not print %q", want)
			}
		}
	}

	expect("ready")
	time.Sleep(200 * time.Millisecond) // let stopOnSigterm register for SIGTERM
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	expect("stopping")
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	select {
	case err := <-done:
		done <- err // for the cleanup
		var exitErr *exec.ExitError
		require.True(t, errors.As(err, &exitErr), "child exit: %v", err)
		ws, ok := exitErr.Sys().(syscall.WaitStatus)
		require.True(t, ok)
		require.True(t, ws.Signaled() && ws.Signal() == syscall.SIGTERM, "child exit: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("second SIGTERM did not end the process while stop hung")
	}
}
