#!/usr/bin/env bash
# Fails if default.nix's vendorHash is stale.
#
# The vendored go modules are a fixed-output derivation, so their store path depends only on
# the declared hash, not on what go mod vendor produces. When a job's nix cache restores a
# store that already has that path, nix-build uses it without building anything, and a stale
# hash only shows up in a job whose cache doesn't have it. So if the path is already there,
# build it again with --check, which fails on a hash mismatch; if it isn't, building it
# checks the hash anyway. Every package shares baseArgs.vendorHash, so one check covers all.
set -euo pipefail
drv=$(nix-instantiate -A styx-test.goModules 2>/dev/null)
out=$(nix-store -q --outputs "$drv")
if nix-store --check-validity "$out" 2>/dev/null; then
  echo "checking $out, which is already in the store"
  nix-build --no-out-link --check "$drv"
else
  nix-build --no-out-link "$drv"
fi
