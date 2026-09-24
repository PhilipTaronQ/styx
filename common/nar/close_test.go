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

// A nar cut short anywhere, including between two tokens, is an error, not a clean end
// (which is what callers check for with err == io.EOF).
func TestReaderTruncated(t *testing.T) {
	// contents that need no padding
	var eight bytes.Buffer
	nw, err := nar.NewWriter(&eight)
	require.NoError(t, err)
	require.NoError(t, nw.WriteHeader(&nar.Header{Path: "/", Type: nar.TypeRegular, Size: 8}))
	_, err = nw.Write([]byte("12345678"))
	require.NoError(t, err)
	require.NoError(t, nw.Close())

	for name, full := range map[string][]byte{
		"empty directory": genEmptyDirectoryNar(),
		"regular":         genOneByteRegularNar(),
		"regular 8 bytes": eight.Bytes(),
		"symlink":         genSymlinkNar(),
	} {
		for _, contents := range []bool{false, true} {
			require.ErrorIs(t, readAllOf(t, full, contents), io.EOF, name)
			for n := 24; n < len(full); n++ { // 24: just the magic
				err := readAllOf(t, full[:n], contents)
				assert.Error(t, err, "%s cut to %d bytes", name, n)
				assert.NotEqual(t, io.EOF, err, "%s cut to %d bytes", name, n)
			}
		}
	}
}

// readAllOf reads every header of a nar, and the contents of every file if contents is set,
// and returns the first error.
func readAllOf(t *testing.T, b []byte, contents bool) error {
	nr, err := nar.NewReader(bytes.NewReader(b))
	require.NoError(t, err)
	defer nr.Close()
	for {
		if _, err := nr.Next(); err != nil {
			return err
		} else if contents {
			if _, err := io.ReadAll(nr); err != nil {
				return err
			}
		}
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
