package cdig

import (
	"bytes"
	"crypto/sha256"
)

// MatchesPadded reports whether b holds data with this digest followed by nothing but zeros.
// Slabs store each chunk zero-padded to a block boundary without recording its exact length, so
// this is how to check a chunk read back from one.
func (dig CDig) MatchesPadded(b []byte) bool {
	n := max(len(bytes.TrimRight(b, "\x00")), 1)
	if n > len(b) {
		return false
	}
	// same as Sum, but try each length from the last non-zero byte to the end
	h := sha256.New()
	h.Write(b[:n])
	var full [sha256.Size]byte
	for {
		if FromBytes(h.Sum(full[:0])) == dig {
			return true
		} else if n == len(b) {
			return false
		}
		h.Write(b[n : n+1])
		n++
	}
}
