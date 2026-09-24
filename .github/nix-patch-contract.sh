#!/usr/bin/env bash
# Behavioural checks of the styx Nix patch, run against the patched Nix in CI.
#
# Each check states the behaviour we expect. "FAIL" means the patched Nix
# does something else. No styx daemon is needed: a missing socket stands in
# for a daemon that is down, and a small fake daemon stands in for replies the
# real daemon can give. A styx mount is simulated with a loop-mounted EROFS
# image, which is all the patch's isStyxMount looks at (statfs magic).
#
# usage: nix-patch-contract.sh <patched nix bin dir> <mkfs.erofs>
set -uo pipefail

NIXBIN=$(realpath "$1")
MKFS_EROFS=$(realpath "$2")
[ -x "$NIXBIN/nix-store" ] || { echo "no nix-store in $NIXBIN"; exit 2; }
[ -x "$MKFS_EROFS" ] || { echo "no mkfs.erofs at $MKFS_EROFS"; exit 2; }
work=$(mktemp -d)
fails=0
nfail() { echo "FAIL: $*"; fails=$((fails + 1)); }
pass() { echo "PASS: $*"; }
setup_err() { echo "SETUP ERROR: $*"; exit 2; }
section() { echo; echo "=== $* ==="; }

nixstore() { "$NIXBIN/nix-store" --option build-users-group '' "$@"; }
sudonixstore() { sudo "$NIXBIN/nix-store" "$@"; }

# A small package with a file and a symlink.
mkdir -p "$work/src/pkg/bin"
echo hello > "$work/src/pkg/data.txt"
ln -s data.txt "$work/src/pkg/link"
echo '#!/bin/sh' > "$work/src/pkg/bin/tool"
chmod +x "$work/src/pkg/bin/tool"

cache="$work/cache"
P=$(nixstore --store "$work/s1" --add "$work/src/pkg") || setup_err "nix-store --add"
name=$(basename "$P")
echo "store path: $P"
"$NIXBIN/nix" --extra-experimental-features nix-command copy \
    --from "$work/s1" --to "file://$cache?compression=none" "$P" || setup_err "nix copy to cache"

# Options that make the file:// cache a styx substituter for every path.
styxopts=(
    --option substituters "file://$cache"
    --option require-sigs false
    --option styx-substituters "file://$cache"
    --option styx-min-size 0
    --option styx-sock-path "$work/no-daemon.sock"
)

section "repair of a styx-eligible path"
# Expected: --repair-path restores the path's contents (or fails loudly).
real1="$work/s1$P"
chmod u+w "$real1" "$real1/data.txt"
echo corrupted > "$real1/data.txt"
chmod u-w "$real1/data.txt" "$real1"
nixstore --store "$work/s1" --verify-path "$P" && setup_err "corruption not detected"
if nixstore --store "$work/s1" "${styxopts[@]}" --option styx-ondemand '.*' --repair-path "$P" 2>&1 | tee "$work/repair.log"; then
    if nixstore --store "$work/s1" --verify-path "$P"; then
        pass "styx-ondemand repair restored the contents"
    else
        nfail "--repair-path exited 0 with styx-ondemand but the path is still corrupted: $(grep -i styx "$work/repair.log" | tr '\n' ' ')"
    fi
else
    pass "styx-ondemand repair failed loudly"
fi
# Control: the same repair with styx off works, so the setup is right.
nixstore --store "$work/s1" "${styxopts[@]}" --option styx-ondemand '' --repair-path "$P" \
    && nixstore --store "$work/s1" --verify-path "$P" || setup_err "control repair without styx failed"

section "invalid styx regex"
# Expected: a malformed styx-ondemand pattern disables styx (or is rejected
# when the setting is read); it must not break ordinary substitution.
if nixstore --store "$work/s2" "${styxopts[@]}" --option styx-ondemand '*' --realise "$P" 2>&1 | tee "$work/regex.log"; then
    pass "substitution still works with styx-ondemand = '*'"
else
    nfail "styx-ondemand = '*' makes substitution fail: $(tail -n 3 "$work/regex.log" | tr '\n' ' ')"
fi
nixstore --store "$work/s2b" "${styxopts[@]}" --option styx-ondemand 'nomatch' --realise "$P" >/dev/null \
    || setup_err "control substitution with a valid regex failed"

# Fake styx daemon: /mount and /materialize answer Success, /umount answers
# the way the real daemon does for a path it doesn't record as mounted.
# /materialize copies the package into DestPath like the real one does
# (mkdir -p, overwrite files, keep whatever else is there).
cat > "$work/fakedaemon.py" <<'EOF'
import http.server, json, os, shutil, socketserver, sys
sock, log, src, corrupt = sys.argv[1:5]
class H(http.server.BaseHTTPRequestHandler):
    def address_string(self):
        return "unix"
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        req = json.loads(body)
        with open(log, "a") as f:
            f.write(self.path + " " + json.dumps(req, sort_keys=True) + "\n")
        code, res = 200, {"Success": True}
        if self.path == "/materialize":
            shutil.copytree(src, req["DestPath"], symlinks=True, dirs_exist_ok=True)
            if os.path.exists(corrupt):
                with open(os.path.join(req["DestPath"], "data.txt"), "w") as f:
                    f.write("corrupted\n")
        elif self.path == "/umount":
            code, res = 404, {"Error": "not mounted"}
        out = json.dumps(res).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)
class S(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
if os.path.exists(sock):
    os.unlink(sock)
srv = S(sock, H)
os.chmod(sock, 0o777)
srv.serve_forever()
EOF
python3 "$work/fakedaemon.py" "$work/fake.sock" "$work/fake.log" "$work/src/pkg" "$work/fake-corrupt" &
fakepid=$!
trap 'kill $fakepid 2>/dev/null; sudo umount "$work/s5$P" 2>/dev/null; true' EXIT
for _ in $(seq 50); do [ -S "$work/fake.sock" ] && break; sleep 0.1; done
[ -S "$work/fake.sock" ] || setup_err "fake daemon did not start"
fakeopts=("${styxopts[@]}" --option styx-sock-path "$work/fake.sock")

section "request fields"
# Expected: the request carries the fields daemon/proto.go MountReq decodes.
nixstore --store "$work/s3" "${fakeopts[@]}" --option styx-ondemand '.*' --realise "$P" > "$work/s3.log" 2>&1
s3rc=$?
narsize=$(nixstore --store "$work/s1" --query --size "$P")
if grep -q "^/mount .*\"MountPoint\": \"$work/s3$P\".*\"NarSize\": $narsize,.*\"StorePath\": \"$name\".*\"Upstream\": \"file://$cache\"" "$work/fake.log"; then
    pass "mount request: $(grep '^/mount' "$work/fake.log")"
else
    nfail "unexpected mount request: $(cat "$work/fake.log")"
fi

section "mount reported successful but nothing mounted"
# The real daemon answers Success for a path its records say is mounted at
# that mount point, without checking the kernel (see
# TestNixContractMountSuccessMeansMounted). Expected: Nix checks that the
# mount is there before registering the path as valid.
if [ "$s3rc" != 0 ]; then
    nfail "unexpected: realise with the fake daemon failed: $(tail -n 3 "$work/s3.log" | tr '\n' ' ')"
elif nixstore --store "$work/s3" --verify-path "$P"; then
    pass "mounted path verifies"
else
    nfail "Nix registered $P as valid after a styx mount reply, but it is an empty directory: $(ls -A "$work/s3$P" | wc -l) entries"
fi

section "materialize into a leftover directory"
# A leftover invalid directory (an interrupted extraction or deletion) sits
# where the path goes. Expected: Nix clears it first, as addToStore and
# makeStyxMount do, so the registered path matches its NAR hash.
mkdir -p "$work/s4$P"
echo stale > "$work/s4$P/stale-file"
nixstore --store "$work/s4" "${fakeopts[@]}" --option styx-materialize '.*' --realise "$P" > "$work/s4.log" 2>&1
s4rc=$?
if [ "$s4rc" != 0 ]; then
    nfail "unexpected: materialize with the fake daemon failed: $(tail -n 3 "$work/s4.log" | tr '\n' ' ')"
elif nixstore --store "$work/s4" --verify-path "$P"; then
    pass "materialized path verifies"
else
    nfail "materialized path does not match its NAR hash; contents: $(ls -A "$work/s4$P" | tr '\n' ' ')"
fi

section "materialize that doesn't match the NAR hash"
# Expected: Nix checks what the daemon wrote, and falls back to a regular
# copy instead of registering it.
touch "$work/fake-corrupt"
nixstore --store "$work/s4b" "${fakeopts[@]}" --option styx-materialize '.*' --realise "$P" > "$work/s4b.log" 2>&1
s4brc=$?
rm -f "$work/fake-corrupt"
if [ "$s4brc" != 0 ]; then
    nfail "realise after a corrupt materialize failed: $(tail -n 3 "$work/s4b.log" | tr '\n' ' ')"
elif ! nixstore --store "$work/s4b" --verify-path "$P"; then
    nfail "Nix registered a materialized path that does not match its NAR hash: $(cat "$work/s4b$P/data.txt")"
elif ! grep -q "falling back to substitution" "$work/s4b.log"; then
    nfail "styx was not used, or did not fail: $(tail -n 3 "$work/s4b.log" | tr '\n' ' ')"
else
    pass "a corrupt materialize fell back to substitution"
fi

section "invalid styx-exclude regex"
# Expected: an exclusion that can't be evaluated keeps styx away from every
# path, and substitution still works.
nmount=$(grep -c '^/mount' "$work/fake.log")
if ! nixstore --store "$work/s6" "${fakeopts[@]}" --option styx-ondemand '.*' --option styx-exclude '*' --realise "$P" > "$work/s6.log" 2>&1; then
    nfail "styx-exclude = '*' makes substitution fail: $(tail -n 3 "$work/s6.log" | tr '\n' ' ')"
elif [ "$(grep -c '^/mount' "$work/fake.log")" != "$nmount" ]; then
    nfail "styx-exclude = '*' did not keep styx away: $(tail -n 1 "$work/fake.log")"
else
    pass "substitution without styx with styx-exclude = '*'"
fi

# The rest needs root and a loop-mounted EROFS image over a valid path.
sudo modprobe erofs || true
s5="$work/s5"
P5=$(sudonixstore --store "$s5" --add "$work/src/pkg") || setup_err "sudo nix-store --add"
[ "$P5" = "$P" ] || setup_err "unexpected path $P5"
"$MKFS_EROFS" "$work/pkg.erofs" "$work/src/pkg" >/dev/null || setup_err "mkfs.erofs"
sudo mount -t erofs -o loop,ro "$work/pkg.erofs" "$s5$P" || setup_err "mount erofs"
[ "$(stat -f -c %t "$s5$P")" = e0f5e1e2 ] || setup_err "not erofs: $(stat -f -c %t "$s5$P")"

section "optimise with a styx mount"
# Expected: nix-store --optimise skips styx mounts (another filesystem) and
# succeeds.
if sudonixstore --store "$s5" --optimise 2>&1 | tee "$work/optimise.log"; then
    pass "optimise succeeded"
else
    nfail "nix-store --optimise fails on a styx mount: $(grep -i error "$work/optimise.log" | tr '\n' ' ')"
fi

section "GC of a styx mount while the daemon is unreachable"
# Expected: GC either removes the path completely or leaves it valid. It must
# not invalidate the path and leave the mount behind.
sudonixstore --store "$s5" --option styx-sock-path "$work/no-daemon.sock" --gc 2>&1 | tee "$work/gc1.log"
gc1=${PIPESTATUS[0]}
valid=no; sudonixstore --store "$s5" --check-validity "$P" 2>/dev/null && valid=yes
mounted=no; sudo mountpoint -q "$s5$P" && mounted=yes
echo "gc exit $gc1, valid $valid, mounted $mounted"
if [ "$mounted" = yes ] && [ "$valid" = no ]; then
    nfail "GC (exit $gc1) invalidated $P but left it mounted: $(tail -n 2 "$work/gc1.log" | tr '\n' ' ')"
else
    pass "GC left a consistent state"
fi

section "GC again, daemon answers 'not mounted'"
# Expected: a later GC gets past the leftover mount.
sudonixstore --store "$s5" --option styx-sock-path "$work/fake.sock" --gc 2>&1 | tee "$work/gc2.log"
gc2=${PIPESTATUS[0]}
if [ "$gc2" = 0 ]; then
    pass "second GC succeeded"
else
    nfail "second GC fails too (exit $gc2): $(tail -n 2 "$work/gc2.log" | tr '\n' ' ')"
fi

section "substitute the path again"
# Expected: the path can be substituted again after GC.
if sudonixstore --store "$s5" "${styxopts[@]}" --option styx-ondemand '' --realise "$P" 2>&1 | tee "$work/resub.log"; then
    pass "re-substitution succeeded"
else
    nfail "$P can no longer be substituted: $(grep -i error "$work/resub.log" | head -n 2 | tr '\n' ' ')"
fi

echo
echo "$fails check(s) failed"
[ "$fails" = 0 ]
