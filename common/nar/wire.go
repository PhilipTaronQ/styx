package nar

import (
	"fmt"
	"io"

	"github.com/nix-community/go-nix/pkg/wire"
)

// Readers for nar bytes fields (a length, the contents, and zero padding to a multiple of 8
// bytes) that fail if the input ends early. wire.ReadBytesFull ignores errors reading the
// padding, and wire.BytesReader treats contents that end early as complete, so with them a
// truncated nar could read as a complete one.

// contentReader reads the contents of a bytes field. Close skips what's left of the contents
// and the padding.
type contentReader struct {
	r   io.Reader
	n   uint64 // contents left
	pad int    // padding after the contents, -1 once it's been read
}

func newContentReader(r io.Reader, length uint64) *contentReader {
	return &contentReader{r: r, n: length, pad: int((8 - length%8) % 8)}
}

func (c *contentReader) Read(p []byte) (int, error) {
	if c.n == 0 {
		return 0, io.EOF
	}
	if uint64(len(p)) > c.n {
		p = p[:c.n]
	}
	n, err := c.r.Read(p)
	c.n -= uint64(n)
	if err == io.EOF {
		if c.n > 0 {
			return n, io.ErrUnexpectedEOF
		}
		err = nil
	}
	return n, err
}

func (c *contentReader) Close() error {
	if c.pad < 0 {
		return nil
	}
	if _, err := io.Copy(io.Discard, c); err != nil {
		return err
	}
	var buf [8]byte
	pad := buf[:c.pad]
	c.pad = -1
	if _, err := io.ReadFull(c.r, pad); err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	}
	for _, b := range pad {
		if b != 0 {
			return fmt.Errorf("invalid padding, should be null bytes, found %v", pad)
		}
	}
	return nil
}

// readBytesFull reads a bytes field of at most maxBytes.
func readBytesFull(r io.Reader, maxBytes uint64) ([]byte, error) {
	length, err := wire.ReadUint64(r)
	if err != nil {
		return nil, err
	} else if length > maxBytes {
		return nil, fmt.Errorf("content length of %v bytes exceeds maximum of %v bytes", length, maxBytes)
	}
	cr := newContentReader(r, length)
	buf := make([]byte, length)
	if _, err := io.ReadFull(cr, buf); err != nil {
		return nil, err
	}
	return buf, cr.Close()
}

// readString reads a bytes field of at most maxBytes as a string.
func readString(r io.Reader, maxBytes uint64) (string, error) {
	b, err := readBytesFull(r, maxBytes)
	return string(b), err
}
