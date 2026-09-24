// Copied from github.com/nix-community/go-nix pkg/nar at commit 4bdde671e0a1
// (v0.0.0-20250101154619-4bdde671e0a1). Licensed under the Apache License 2.0, see LICENSE.
// Modified: import path, and removed TestReaderSmoketest, which needs go-nix's test data.

package nar_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/dnr/styx/common/nar"
	"github.com/stretchr/testify/assert"
)

func TestReaderEmpty(t *testing.T) {
	nr, err := nar.NewReader(bytes.NewBuffer(genEmptyNar()))
	assert.NoError(t, err)

	hdr, err := nr.Next()
	// first Next() should return an non-nil error that's != io.EOF,
	// as an empty NAR is invalid.
	assert.Error(t, err, "first Next() on an empty NAR should return an error")
	assert.NotEqual(t, io.EOF, err, "first Next() on an empty NAR shouldn't return io.EOF")
	assert.Nil(t, hdr, "returned header should be nil")

	assert.NotPanics(t, func() {
		nr.Close()
	}, "closing the reader shouldn't panic")
}

func TestReaderEmptyDirectory(t *testing.T) {
	nr, err := nar.NewReader(bytes.NewBuffer(genEmptyDirectoryNar()))
	assert.NoError(t, err)

	// get first header
	hdr, err := nr.Next()
	assert.NoError(t, err)
	assert.Equal(t, &nar.Header{
		Path: "/",
		Type: nar.TypeDirectory,
	}, hdr)

	hdr, err = nr.Next()
	assert.Equal(t, io.EOF, err, "Next() should return io.EOF as error")
	assert.Nil(t, hdr, "returned header should be nil")

	assert.NotPanics(t, func() {
		nr.Close()
	}, "closing the reader shouldn't panic")
}

func TestReaderOneByteRegular(t *testing.T) {
	nr, err := nar.NewReader(bytes.NewBuffer(genOneByteRegularNar()))
	assert.NoError(t, err)

	// get first header
	hdr, err := nr.Next()
	assert.NoError(t, err)
	assert.Equal(t, &nar.Header{
		Path:       "/",
		Type:       nar.TypeRegular,
		Size:       1,
		Executable: false,
	}, hdr)

	// read contents
	contents, err := io.ReadAll(nr)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x1}, contents)

	hdr, err = nr.Next()
	assert.Equal(t, io.EOF, err, "Next() should return io.EOF as error")
	assert.Nil(t, hdr, "returned header should be nil")

	assert.NotPanics(t, func() {
		nr.Close()
	}, "closing the reader shouldn't panic")
}

func TestReaderSymlink(t *testing.T) {
	nr, err := nar.NewReader(bytes.NewBuffer(genSymlinkNar()))
	assert.NoError(t, err)

	// get first header
	hdr, err := nr.Next()
	assert.NoError(t, err)
	assert.Equal(t, &nar.Header{
		Path:       "/",
		Type:       nar.TypeSymlink,
		LinkTarget: "/nix/store/somewhereelse",
		Size:       0,
		Executable: false,
	}, hdr)

	// read contents should only return an empty byte slice
	contents, err := io.ReadAll(nr)
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contents)

	hdr, err = nr.Next()
	assert.Equal(t, io.EOF, err, "Next() should return io.EOF as error")
	assert.Nil(t, hdr, "returned header should be nil")

	assert.NotPanics(t, func() {
		nr.Close()
	}, "closing the reader shouldn't panic")
}

// TODO: various early close cases

func TestReaderInvalidOrder(t *testing.T) {
	nr, err := nar.NewReader(bytes.NewBuffer(genInvalidOrderNAR()))
	assert.NoError(t, err)

	// get first header (/)
	hdr, err := nr.Next()
	assert.NoError(t, err)
	assert.Equal(t, &nar.Header{
		Path: "/",
		Type: nar.TypeDirectory,
	}, hdr)

	// get first element inside / (/b)
	hdr, err = nr.Next()
	assert.NoError(t, err)
	assert.Equal(t, &nar.Header{
		Path: "/b",
		Type: nar.TypeDirectory,
	}, hdr)

	// get second element inside / (/a) should fail
	_, err = nr.Next()
	assert.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF, "should not be io.EOF")
}
