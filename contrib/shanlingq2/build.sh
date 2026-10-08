#!/bin/sh
# Static soft-float librespot for the Shanling Q2, staged with its card files
# in OUT/.rockbox. Needs Rust nightly with rust-src and musl.cc's
# mipsel-linux-muslsf-cross toolchain.
#   contrib/shanlingq2/build.sh TOOLCHAIN [OUT]
set -eu
[ $# -ge 1 ] || { echo "usage: $0 TOOLCHAIN [OUT]" >&2; exit 2; }
TC=$(cd "$1" && pwd)
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${2:-$HERE/../../target/shanlingq2}
TARGET=mipsel-unknown-linux-musl
export PATH="$TC/bin:$PATH"

# The tier-3 target ships no crt objects or libunwind: link with the
# toolchain's crt files and its libgcc_eh in libunwind's place.
LIB="$OUT/lib"
mkdir -p "$LIB" "$OUT/.rockbox"
ln -sf "$(find "$TC" -name libgcc_eh.a | head -n 1)" "$LIB/libunwind.a"

cd "$HERE/../.."
CC_mipsel_unknown_linux_musl=mipsel-linux-muslsf-gcc \
AR_mipsel_unknown_linux_musl=mipsel-linux-muslsf-ar \
CARGO_TARGET_MIPSEL_UNKNOWN_LINUX_MUSL_LINKER=mipsel-linux-muslsf-gcc \
RUSTFLAGS="-C target-feature=+crt-static -C link-self-contained=no -L $LIB" \
cargo +nightly build --release -Zbuild-std=std,panic_abort --target $TARGET \
    --no-default-features --features rustls-tls-webpki-roots,with-libmdns

R="$OUT/.rockbox"
mipsel-linux-muslsf-strip -o "$R/librespot" target/$TARGET/release/librespot
mipsel-linux-muslsf-gcc -march=mips32r2 -static -Os -Wall -Wextra -Werror -s -o "$R/dacon" "$HERE/dacon.c"
python3 -I "$HERE/nan2008.py" "$R/librespot" "$R/dacon"
cp "$HERE/launcher.sh" "$R/rockbox"
cp "$HERE/onevent.sh" "$R/"
chmod +x "$R/rockbox" "$R/onevent.sh"
echo "$R"
