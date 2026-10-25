#!/bin/sh
# Hard-float librespot for the Shanling Q2 (MIPS32r2, FP64, NaN-2008), linked against the Q2's own
# glibc 2.28, staged with its card files in OUT/.spotify.
#   contrib/shanlingq2/build.sh FIRMWARE_ZIP [OUT]
# FIRMWARE_ZIP is Shanling's "Q2 Firmware V1.32.zip"; its rootfs supplies the /lib to link against.
# Needs Rust nightly with rust-src, clang, lld, llvm-ar and llvm-strip (18 or later; set
# LLVM_SUFFIX=-18 for versioned names), unsquashfs, python3, and MIPS glibc headers for ring's C
# code (Debian/Ubuntu: libc6-dev-mipsel-cross; MIPSEL_INCLUDE names another directory).
set -eu
[ $# -ge 1 ] || { echo "usage: $0 FIRMWARE_ZIP [OUT]" >&2; exit 2; }
ZIP=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${2:-$HERE/../../target/shanlingq2}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
LLVM=${LLVM_SUFFIX:-}
MIPSEL_INCLUDE=${MIPSEL_INCLUDE:-/usr/mipsel-linux-gnu/include}
ZIP_SHA=154c17822d09be001be35c03d2d3488424dee195221790bd70864480d55b0f00 # q2-pod audits the same ZIP

# The Q2's /lib, from the firmware's rootfs.
SYSROOT=$OUT/sysroot
L=$SYSROOT/lib
if [ ! -e "$L/libc.so.6" ]; then
  python3 -I - "$ZIP" "$ZIP_SHA" "$OUT/rootfs.squashfs" <<'EOF'
import hashlib, io, sys, tarfile, zipfile
zip_path, want, out = sys.argv[1:]
raw = open(zip_path, 'rb').read()
if hashlib.sha256(raw).hexdigest() != want:
    sys.exit(f'{zip_path}: not the Q2 V1.32 firmware ZIP (SHA-256 differs)')
with zipfile.ZipFile(io.BytesIO(raw)) as z:
    tar = z.read('Q2 Firmware V1.32/update.tar')
with tarfile.open(fileobj=io.BytesIO(tar)) as t:
    open(out, 'wb').write(t.extractfile('recovery-update/rootfs.squashfs').read())
EOF
  unsquashfs -no-xattrs -f -d "$SYSROOT" "$OUT/rootfs.squashfs" lib >/dev/null
  rm "$OUT/rootfs.squashfs"
fi

# The rootfs ships no crt1.o or libc_nonshared.a: start.c and nonshared.c stand in for them, and
# libc.so is glibc's usual linker script around them (bare names, found through q2-link's -L, so
# a path with spaces still links). Cargo cannot see these inputs: after editing start.c,
# nonshared.c or q2-link, delete target/mipsel-q2-linux-gnu so librespot is linked again.
CFLAGS="--target=mipsel-linux-gnu -march=mips32r2 -mfp64 -mnan=2008 -mabs=2008 -mabicalls -O2"
clang$LLVM $CFLAGS -fno-pic -c "$HERE/start.c" -o "$OUT/start.o"
clang$LLVM $CFLAGS -fPIC -c "$HERE/nonshared.c" -o "$OUT/nonshared.o"
rm -f "$L/libq2nonshared.a"
llvm-ar$LLVM rcs "$L/libq2nonshared.a" "$OUT/nonshared.o"
for l in pthread.so.0 m.so.6 dl.so.2 rt.so.1 util.so.1 gcc_s.so.1; do ln -sf "lib$l" "$L/lib${l%.*}"; done
echo 'GROUP ( libc.so.6 libq2nonshared.a AS_NEEDED ( ld-linux-mipsn8.so.1 ) )' > "$L/libc.so"

# glibc's headers ask for gnu/stubs-o32_hard_2008.h under NaN-2008; Debian's legacy-NaN package has
# only stubs-o32_hard.h, which lists the same unimplemented functions.
INC=$OUT/include
mkdir -p "$INC/gnu"
cp "$MIPSEL_INCLUDE/gnu/stubs-o32_hard.h" "$INC/gnu/stubs-o32_hard_2008.h"

export Q2_SYSROOT="$SYSROOT" Q2_START="$OUT/start.o" Q2_INCLUDE="$INC" MIPSEL_INCLUDE LLVM
cd "$HERE/../.."
CC_mipsel_q2_linux_gnu="$HERE/q2-cc" \
AR_mipsel_q2_linux_gnu="llvm-ar$LLVM" \
CARGO_TARGET_MIPSEL_Q2_LINUX_GNU_LINKER="$HERE/q2-link" \
cargo +nightly build --release -Zbuild-std=std,panic_abort -Zjson-target-spec \
    --target "$HERE/mipsel-q2-linux-gnu.json" \
    --no-default-features --features rustls-tls-webpki-roots,with-libmdns

# std's clock is only 64-bit (past January 2038) if its weak __clock_gettime64 bound to nonshared.c's.
llvm-nm$LLVM target/mipsel-q2-linux-gnu/release/librespot | grep -q ' T __clock_gettime64$' ||
    { echo "librespot: std's __clock_gettime64 is unresolved; see nonshared.c" >&2; exit 1; }

R="$OUT/.spotify"
mkdir -p "$R"
llvm-strip$LLVM -o "$R/librespot" target/mipsel-q2-linux-gnu/release/librespot
cp "$HERE/launcher.sh" "$R/run"
cp "$HERE/aplay.sh" "$R/"
chmod +x "$R/run" "$R/aplay.sh"
echo "$R"
