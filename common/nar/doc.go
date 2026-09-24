// Package nar implements access to .nar files.
//
// Nix Archive (nar) is a file format for storing a directory or a single file
// in a binary reproducible format. This is the format that is being used to
// pack and distribute Nix build results. It doesn't store any timestamps or
// similar fields available in conventional filesystems. .nar files can be read
// and written in a streaming manner.
//
// This is the part of go-nix's pkg/nar that styx uses (the reader, writer and header
// types), copied from github.com/nix-community/go-nix at commit 4bdde671e0a1
// (v0.0.0-20250101154619-4bdde671e0a1) so that styx can fix it. It's licensed under the
// Apache License 2.0, see LICENSE. Files that were changed say so at the top.
package nar
