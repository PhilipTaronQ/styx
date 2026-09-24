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

section "invalid styx regexes"
# Expected: a styx-ondemand, styx-materialize or styx-exclude pattern that
# isn't a valid regex is rejected when the settings are loaded or set, with
# an error naming the setting and the pattern, as for any other malformed
# setting. (--option only warns about a malformed value and keeps the
# previous one, for every setting.)
nixcmd() { "$NIXBIN/nix" --extra-experimental-features nix-command "$@"; }
oneline() { tail -n 3 "$1" | tr '\n' ' '; }
for s in styx-ondemand styx-materialize styx-exclude; do
    err="setting '$s' has invalid regex '\*'"
    log="$work/regex-$s.log"
    if NIX_CONFIG="$s = foo.* *" nixcmd config show "$s" > "$log" 2>&1; then
        nfail "nix loaded '$s = foo.* *' from NIX_CONFIG: $(oneline "$log")"
    elif ! grep -q "$err" "$log"; then
        nfail "nix rejected '$s = foo.* *' without naming the setting and pattern: $(oneline "$log")"
    else
        pass "config load rejects '$s = foo.* *': $(grep -m 1 error "$log")"
    fi
    if nixstore --store "$work/s2" "${styxopts[@]}" "--$s" '*' --realise "$P" > "$log" 2>&1; then
        nfail "nix-store --$s '*' --realise succeeded: $(oneline "$log")"
    elif ! grep -q "$err" "$log"; then
        nfail "nix-store --$s '*' failed without naming the setting and pattern: $(oneline "$log")"
    else
        pass "nix-store --$s '*' is rejected"
    fi
    if ! nixcmd --option "$s" '*' config show "$s" > "$log" 2>&1; then
        nfail "nix --option $s '*' failed: $(oneline "$log")"
    elif ! grep -q "$err" "$log" || [ "$(grep -v "$err" "$log")" != "" ]; then
        nfail "nix --option $s '*' did not warn and keep the default: $(oneline "$log")"
    else
        pass "nix --option $s '*' warns and keeps the default"
    fi
    # Control: valid patterns load and print as given.
    v=$(NIX_CONFIG="$s = foo.* bar" nixcmd config show "$s") && [ "$v" = "foo.* bar" ] \
        || setup_err "'$s = foo.* bar' does not load or show as given: $v"
done
nixstore --store "$work/s2" "${styxopts[@]}" --option styx-ondemand 'nomatch' --realise "$P" >/dev/null \
    || setup_err "control substitution with a valid regex failed"

# Fake styx daemon: /mount and /materialize answer Success, /umount answers
# the way the real daemon does for a path it doesn't record as mounted.
# /materialize copies the package into DestPath like the real one does
# (mkdir -p, overwrite files, keep whatever else is there). If the file named
# by `slow` exists, /materialize then waits until it is removed and writes
# into DestPath again, like a daemon that keeps going after its client has
# given up, and logs a "late-write" line.
cat > "$work/fakedaemon.py" <<'EOF'
import http.server, json, os, shutil, socketserver, sys, time
sock, log, src, corrupt, slow = sys.argv[1:6]
def record(line):
    with open(log, "a") as f:
        f.write(line + "\n")
class H(http.server.BaseHTTPRequestHandler):
    def address_string(self):
        return "unix"
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        req = json.loads(body)
        record(self.path + " " + json.dumps(req, sort_keys=True))
        code, res = 200, {"Success": True}
        if self.path == "/materialize":
            dest = req["DestPath"]
            shutil.copytree(src, dest, symlinks=True, dirs_exist_ok=True)
            if os.path.exists(corrupt):
                with open(os.path.join(dest, "data.txt"), "w") as f:
                    f.write("corrupted\n")
            if os.path.exists(slow):
                for _ in range(600):
                    if not os.path.exists(slow):
                        break
                    time.sleep(0.1)
                try:
                    # the real daemon runs as root, so read-only modes don't stop it
                    os.chmod(dest, 0o755)
                    os.chmod(os.path.join(dest, "data.txt"), 0o644)
                    with open(os.path.join(dest, "data.txt"), "w") as f:
                        f.write("late\n")
                    result = "wrote data.txt"
                except OSError as e:
                    result = e.strerror
                record("late-write " + dest + ": " + result)
        elif self.path == "/umount":
            code, res = 404, {"Error": "not mounted"}
        out = json.dumps(res).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)
        except OSError:
            pass  # the client gave up
class S(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
if os.path.exists(sock):
    os.unlink(sock)
srv = S(sock, H)
os.chmod(sock, 0o777)
srv.serve_forever()
EOF
python3 "$work/fakedaemon.py" "$work/fake.sock" "$work/fake.log" "$work/src/pkg" "$work/fake-corrupt" "$work/fake-slow" &
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

section "materialize that outlasts styx-timeout"
# Expected: Nix gives up on a daemon that hasn't answered after styx-timeout
# and falls back to a regular copy. The daemon may still be writing into the
# destination it was given (it stops when it sees the disconnect, see
# TestMaterializeStopsWhenCancelled), and that must not reach the registered
# path. The fake daemon writes only once Nix has finished.
touch "$work/fake-slow"
nixstore --store "$work/s8" "${fakeopts[@]}" --option styx-timeout 1 --option styx-materialize '.*' --realise "$P" > "$work/s8.log" 2>&1
s8rc=$?
rm -f "$work/fake-slow"
for _ in $(seq 100); do grep -q '^late-write ' "$work/fake.log" && break; sleep 0.1; done
late=$(grep '^late-write ' "$work/fake.log")
if [ "$s8rc" != 0 ]; then
    nfail "realise after a materialize timeout failed: $(oneline "$work/s8.log")"
elif ! grep -q "falling back to substitution:.*styx request '/materialize' failed: Timeout was reached" "$work/s8.log"; then
    nfail "Nix did not give up on the daemon after styx-timeout: $(oneline "$work/s8.log")"
elif [ -z "$late" ]; then
    nfail "the fake daemon did not write after Nix gave up, so nothing was checked"
elif ! nixstore --store "$work/s8" --verify-path "$P" || [ "$(cat "$work/s8$P/data.txt")" != hello ]; then
    nfail "the daemon's write after the timeout reached the store path ($late): data.txt is $(cat "$work/s8$P/data.txt")"
else
    pass "Nix fell back after styx-timeout and the late write missed the store path ($late)"
fi

section "path registered by another process during a styx substitution"
# A long-lived Nix process (nix-store --serve here, or a nix-daemon worker)
# can hold a negative path info cache entry for a path it then substitutes.
# If another process registers the path while this one waits for the path's
# lock, mountStyx and materializeStyx find it valid and leave it alone.
# Expected: they drop the negative entry, as addToStore does, so the process
# sees the path as valid afterwards. The driver speaks the serve protocol:
# QueryPathInfos (not valid yet), then QueryValidPaths with substitution,
# holding the path's lock until nix-store waits on it and registering the
# path meanwhile. It prints the paths nix-store reports as valid.
cat > "$work/serverace.py" <<'EOF'
import fcntl, os, re, struct, subprocess, sys, time
nixstore, store, path, src, errlog = sys.argv[1:6]
opts = sys.argv[6:]
real = store + path
base = [nixstore, "--option", "build-users-group", ""]
def u64(n):
    return struct.pack("<Q", n)
def string(s):
    b = s.encode()
    return u64(len(b)) + b + b"\0" * (-len(b) % 8)
def read_u64(f):
    b = f.read(8)
    if len(b) != 8:
        sys.exit("nix-store --serve closed its output")
    return struct.unpack("<Q", b)[0]
def read_string(f):
    n = read_u64(f)
    return f.read(n + (-n % 8))[:n].decode()
def send(p, data):
    p.stdin.write(data)
    p.stdin.flush()
lock = open(real + ".lock", "w")
fcntl.flock(lock, fcntl.LOCK_EX)
waiter = re.compile(r"->\s+FLOCK\s.*:%d\s" % os.fstat(lock.fileno()).st_ino)
serve = subprocess.Popen(base + ["--store", store] + opts + ["--serve", "--write"],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=open(errlog, "w"))
send(serve, u64(0x390c9deb) + u64(0x207))
if read_u64(serve.stdout) != 0x5452eecb:
    sys.exit("bad nix-store --serve magic")
read_u64(serve.stdout)
send(serve, u64(2) + u64(1) + string(path))  # QueryPathInfos
if read_string(serve.stdout) != "":
    sys.exit("nix-store --serve has path info for a path that isn't valid yet")
send(serve, u64(1) + u64(0) + u64(1) + u64(1) + string(path))  # QueryValidPaths, substitute
for _ in range(600):
    with open("/proc/locks") as f:
        if any(waiter.search(l) for l in f):
            break
    if serve.poll() is not None:
        sys.exit("nix-store --serve exited")
    time.sleep(0.1)
else:
    sys.exit("nix-store --serve never waited for the path's lock")
subprocess.run(["cp", "-a", src, real], check=True)
subprocess.run(base + ["--store", store, "--register-validity"], input=(path + "\n\n0\n").encode(), check=True)
fcntl.flock(lock, fcntl.LOCK_UN)
lock.close()
valid = [read_string(serve.stdout) for _ in range(read_u64(serve.stdout))]
serve.stdin.close()
serve.wait()
print(" ".join(valid) if valid else "none")
EOF
for mode in styx-ondemand styx-materialize; do
    s7="$work/s7-$mode"
    nixstore --store "$s7" --dump-db >/dev/null || setup_err "cannot create $s7"
    nreq=$(wc -l < "$work/fake.log")
    if ! valid=$(python3 "$work/serverace.py" "$NIXBIN/nix-store" "$s7" "$P" "$work/s1$P" "$s7.log" \
            "${fakeopts[@]}" --option "$mode" '.*'); then
        nfail "$mode: the serve protocol driver failed: $(oneline "$s7.log")"
    elif ! grep -q "with styx" "$s7.log"; then
        nfail "$mode: nix-store --serve did not substitute with styx: $(oneline "$s7.log")"
    elif [ "$(wc -l < "$work/fake.log")" != "$nreq" ]; then
        nfail "$mode: styx asked the daemon for a path that was already valid: $(tail -n 1 "$work/fake.log")"
    elif [ "$valid" != "$P" ]; then
        nfail "$mode: nix-store --serve kept a stale negative path info cache entry: valid paths after substituting $P: $valid"
    else
        pass "$mode: the substituting process sees the path registered by another as valid"
    fi
done

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
