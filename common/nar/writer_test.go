// Copied from github.com/nix-community/go-nix pkg/nar at commit 4bdde671e0a1
// (v0.0.0-20250101154619-4bdde671e0a1). Licensed under the Apache License 2.0, see LICENSE.
// Modified: import path, and removed TestWriterSmoketest, which needs go-nix's test data.

package nar_test

import (
	"bytes"
	"testing"

	"github.com/PhilipTaronQ/styx/common/nar"
	"github.com/stretchr/testify/assert"
)

func TestWriterEmpty(t *testing.T) {
	var buf bytes.Buffer
	nw, err := nar.NewWriter(&buf)
	assert.NoError(t, err)

	// calling close on an empty NAR is an error, as it'd be invalid.
	assert.Error(t, nw.Close())

	assert.NotPanics(t, func() {
		nw.Close()
	}, "closing a second time, after the first one failed shouldn't panic")
}

func TestWriterEmptyDirectory(t *testing.T) {
	var buf bytes.Buffer
	nw, err := nar.NewWriter(&buf)
	assert.NoError(t, err)

	hdr := &nar.Header{
		Path: "/",
		Type: nar.TypeDirectory,
	}

	err = nw.WriteHeader(hdr)
	assert.NoError(t, err)

	err = nw.Close()
	assert.NoError(t, err)

	assert.Equal(t, genEmptyDirectoryNar(), buf.Bytes())

	assert.NotPanics(t, func() {
		nw.Close()
	}, "closing a second time shouldn't panic")
}

// TestWriterOneByteRegular writes a NAR only containing a single file at the root.
func TestWriterOneByteRegular(t *testing.T) {
	var buf bytes.Buffer
	nw, err := nar.NewWriter(&buf)
	assert.NoError(t, err)

	hdr := nar.Header{
		Path:       "/",
		Type:       nar.TypeRegular,
		Size:       1,
		Executable: false,
	}

	err = nw.WriteHeader(&hdr)
	assert.NoError(t, err)

	num, err := nw.Write([]byte{1})
	assert.Equal(t, num, 1)
	assert.NoError(t, err)

	err = nw.Close()
	assert.NoError(t, err)

	assert.Equal(t, genOneByteRegularNar(), buf.Bytes())
}

// TestWriterSymlink writes a NAR only containing a symlink.
func TestWriterSymlink(t *testing.T) {
	var buf bytes.Buffer
	nw, err := nar.NewWriter(&buf)
	assert.NoError(t, err)

	hdr := nar.Header{
		Path:       "/",
		Type:       nar.TypeSymlink,
		LinkTarget: "/nix/store/somewhereelse",
		Size:       0,
		Executable: false,
	}

	err = nw.WriteHeader(&hdr)
	assert.NoError(t, err)

	err = nw.Close()
	assert.NoError(t, err)

	assert.Equal(t, genSymlinkNar(), buf.Bytes())
}

func TestWriterErrorsTransitions(t *testing.T) {
	t.Run("missing directory in between", func(t *testing.T) {
		var buf bytes.Buffer
		nw, err := nar.NewWriter(&buf)
		assert.NoError(t, err)

		// write a directory node
		err = nw.WriteHeader(&nar.Header{
			Path: "/",
			Type: nar.TypeDirectory,
		})
		assert.NoError(t, err)

		// write a symlink "a/foo", but missing the directory node "a" in between should error
		err = nw.WriteHeader(&nar.Header{
			Path:       "/a/foo",
			Type:       nar.TypeSymlink,
			LinkTarget: "doesntmatter",
		})
		assert.Error(t, err)
	})

	t.Run("missing directory at the beginning, writing another directory", func(t *testing.T) {
		var buf bytes.Buffer
		nw, err := nar.NewWriter(&buf)
		assert.NoError(t, err)

		// write a directory node for "/a" without writing the one for "/"
		err = nw.WriteHeader(&nar.Header{
			Path: "/a",
			Type: nar.TypeDirectory,
		})
		assert.Error(t, err)
	})

	t.Run("missing directory at the beginning, writing a symlink", func(t *testing.T) {
		var buf bytes.Buffer
		nw, err := nar.NewWriter(&buf)
		assert.NoError(t, err)

		// write a symlink for "a" without writing the directory one for ""
		err = nw.WriteHeader(&nar.Header{
			Path:       "/a",
			Type:       nar.TypeSymlink,
			LinkTarget: "foo",
		})
		assert.Error(t, err)
	})

	t.Run("transition via a symlink, not directory", func(t *testing.T) {
		var buf bytes.Buffer
		nw, err := nar.NewWriter(&buf)
		assert.NoError(t, err)

		// write a directory node
		err = nw.WriteHeader(&nar.Header{
			Path: "/",
			Type: nar.TypeDirectory,
		})
		assert.NoError(t, err)

		// write a symlink node for "/a"
		err = nw.WriteHeader(&nar.Header{
			Path:       "/a",
			Type:       nar.TypeSymlink,
			LinkTarget: "doesntmatter",
		})
		assert.NoError(t, err)

		// write a symlink "/a/b", which should fail, as a was a symlink, not directory
		err = nw.WriteHeader(&nar.Header{
			Path:       "/a/b",
			Type:       nar.TypeSymlink,
			LinkTarget: "doesntmatter",
		})
		assert.Error(t, err)
	})

	t.Run("not lexicographically sorted", func(t *testing.T) {
		var buf bytes.Buffer
		nw, err := nar.NewWriter(&buf)
		assert.NoError(t, err)

		// write a directory node
		err = nw.WriteHeader(&nar.Header{
			Path: "/",
			Type: nar.TypeDirectory,
		})
		assert.NoError(t, err)

		// write a symlink for "/b"
		err = nw.WriteHeader(&nar.Header{
			Path:       "/b",
			Type:       nar.TypeSymlink,
			LinkTarget: "foo",
		})
		assert.NoError(t, err)

		// write a symlink for "/a"
		err = nw.WriteHeader(&nar.Header{
			Path:       "/a",
			Type:       nar.TypeSymlink,
			LinkTarget: "foo",
		})
		assert.Error(t, err)
	})

	t.Run("not lexicographically sorted, but the same", func(t *testing.T) {
		var buf bytes.Buffer
		nw, err := nar.NewWriter(&buf)
		assert.NoError(t, err)

		// write a directory node
		err = nw.WriteHeader(&nar.Header{
			Path: "/",
			Type: nar.TypeDirectory,
		})
		assert.NoError(t, err)

		// write a symlink for "/a"
		err = nw.WriteHeader(&nar.Header{
			Path:       "/a",
			Type:       nar.TypeSymlink,
			LinkTarget: "foo",
		})
		assert.NoError(t, err)

		// write a symlink for "/a"
		err = nw.WriteHeader(&nar.Header{
			Path:       "/a",
			Type:       nar.TypeSymlink,
			LinkTarget: "foo",
		})
		assert.Error(t, err)
	})

	t.Run("lexicographically sorted with nested directory and common prefix", func(t *testing.T) {
		var buf bytes.Buffer
		nw, err := nar.NewWriter(&buf)
		assert.NoError(t, err)

		// write a directory node
		err = nw.WriteHeader(&nar.Header{
			Path: "/",
			Type: nar.TypeDirectory,
		})
		assert.NoError(t, err)

		// write a directory node with name "/foo"
		err = nw.WriteHeader(&nar.Header{
			Path: "/foo",
			Type: nar.TypeDirectory,
		})
		assert.NoError(t, err)

		// write a symlink for "/foo/b"
		err = nw.WriteHeader(&nar.Header{
			Path:       "/foo/b",
			Type:       nar.TypeSymlink,
			LinkTarget: "foo",
		})
		assert.NoError(t, err)

		// write a symlink for "/foo-a"
		err = nw.WriteHeader(&nar.Header{
			Path:       "/foo-a",
			Type:       nar.TypeSymlink,
			LinkTarget: "foo",
		})
		assert.NoError(t, err)
	})
}
