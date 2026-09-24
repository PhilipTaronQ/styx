// Copied from github.com/nix-community/go-nix pkg/nar at commit 4bdde671e0a1
// (v0.0.0-20250101154619-4bdde671e0a1). Licensed under the Apache License 2.0, see LICENSE.
// Modified: removed Header.FileInfo, which styx doesn't use.

package nar

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Header represents a single header in a NAR archive. Some fields may not
// be populated depending on the Type.
type Header struct {
	Path       string   // Path of the file entry, relative inside the NAR
	Type       NodeType // Typeflag is the type of header entry.
	LinkTarget string   // Target of symlink (valid for TypeSymlink)
	Size       int64    // Logical file size in bytes
	Executable bool     // Set to true for files that are executable
}

// Validate does some consistency checking of the header structure, such as
// checking for valid paths and inconsistent fields, and returns an error if it
// fails validation.
func (h *Header) Validate() error {
	// Path needs to start with a /, and must not contain null bytes
	// as we might get passed windows paths, ToSlash them first.
	if p := filepath.ToSlash(h.Path); len(h.Path) < 1 || p[0:1] != "/" {
		return fmt.Errorf("path must start with a /")
	}

	if strings.ContainsAny(h.Path, "\u0000") {
		return fmt.Errorf("path may not contain null bytes")
	}

	// Regular files and directories may not have LinkTarget set.
	if h.Type == TypeRegular || h.Type == TypeDirectory {
		if h.LinkTarget != "" {
			return fmt.Errorf("type is %v, but LinkTarget is not empty", h.Type.String())
		}
	}

	// Directories and Symlinks may not have Size and Executable set.
	if h.Type == TypeDirectory || h.Type == TypeSymlink {
		if h.Size != 0 {
			return fmt.Errorf("type is %v, but Size is not 0", h.Type.String())
		}

		if h.Executable {
			return fmt.Errorf("type is %v, but Executable is true", h.Type.String())
		}
	}

	// Symlinks need to specify a target.
	if h.Type == TypeSymlink {
		if h.LinkTarget == "" {
			return fmt.Errorf("type is symlink, but LinkTarget is empty")
		}
	}

	return nil
}
