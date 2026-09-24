// Copied from github.com/nix-community/go-nix pkg/nar at commit 4bdde671e0a1
// (v0.0.0-20250101154619-4bdde671e0a1). Licensed under the Apache License 2.0, see LICENSE.

package nar

const narVersionMagic1 = "nix-archive-1"

// Enum of all the node types possible.
type NodeType string

const (
	// TypeRegular represents a regular file.
	TypeRegular = NodeType("regular")
	// TypeDirectory represents a directory entry.
	TypeDirectory = NodeType("directory")
	// TypeSymlink represents a file symlink.
	TypeSymlink = NodeType("symlink")
)

func (t NodeType) String() string {
	return string(t)
}
