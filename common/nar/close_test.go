package nar_test

import (
	"bytes"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dnr/styx/common/nar"
)

// parserGoroutines counts Reader parser goroutines that are still running.
func parserGoroutines() int {
	buf := make([]byte, 4<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "common/nar.NewReader.func") {
			n++
		}
	}
	return n
}

// requireParserGoroutines waits for the number of parser goroutines to drop to want.
func requireParserGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for parserGoroutines() > want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.LessOrEqual(t, parserGoroutines(), want, "nar parser goroutines still running after Close")
}

func readAll(nr *nar.Reader) error {
	for {
		if _, err := nr.Next(); err != nil {
			return err
		}
	}
}

// Once Next had rejected an out-of-order entry, nothing could stop the parser goroutine: it
// waited for a Next call that never came, and Close only let it read on, until it blocked
// handing over the next header or its final error. Close must stop it in every state.
func TestReaderCloseStopsParser(t *testing.T) {
	for _, tc := range []struct {
		name string
		nar  []byte
		f    func(t *testing.T, nr *nar.Reader)
	}{{
		name: "out of order",
		nar:  genInvalidOrderNAR(),
		f: func(t *testing.T, nr *nar.Reader) {
			assert.ErrorContains(t, readAll(nr), "wrong order")
		},
	}, {
		name: "before next",
		nar:  genInvalidOrderNAR(),
		f:    func(t *testing.T, nr *nar.Reader) {},
	}, {
		name: "inside a directory",
		nar:  genInvalidOrderNAR(),
		f: func(t *testing.T, nr *nar.Reader) {
			for range 2 {
				_, err := nr.Next()
				require.NoError(t, err)
			}
		},
	}, {
		name: "in a regular file",
		nar:  genOneByteRegularNar(),
		f: func(t *testing.T, nr *nar.Reader) {
			_, err := nr.Next()
			require.NoError(t, err)
		},
	}, {
		name: "at the end",
		nar:  genSymlinkNar(),
		f: func(t *testing.T, nr *nar.Reader) {
			assert.ErrorIs(t, readAll(nr), io.EOF)
		},
	}, {
		name: "after a read error",
		nar:  genEmptyDirectoryNar()[:len(genEmptyDirectoryNar())-12],
		f: func(t *testing.T, nr *nar.Reader) {
			err := readAll(nr)
			assert.Error(t, err)
			assert.NotErrorIs(t, err, io.EOF)
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			// other tests may leave readers unclosed
			before := parserGoroutines()
			for range 20 {
				nr, err := nar.NewReader(bytes.NewReader(tc.nar))
				require.NoError(t, err)
				tc.f(t, nr)
				require.NoError(t, nr.Close())
				require.NoError(t, nr.Close(), "second Close")
			}
			requireParserGoroutines(t, before)
		})
	}
}

// Next after Close fails instead of blocking.
func TestReaderNextAfterClose(t *testing.T) {
	nr, err := nar.NewReader(bytes.NewReader(genSymlinkNar()))
	require.NoError(t, err)
	require.NoError(t, nr.Close())
	_, err = nr.Next()
	assert.ErrorIs(t, err, nar.ErrClosed)

	nr, err = nar.NewReader(bytes.NewReader(genEmptyDirectoryNar()))
	require.NoError(t, err)
	require.ErrorIs(t, readAll(nr), io.EOF)
	require.NoError(t, nr.Close())
	_, err = nr.Next()
	assert.ErrorIs(t, err, io.EOF, "Next keeps returning the error it returned before Close")
}
