#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "$ROOT_DIR/bin"

# Build your hashgen (vaultx.cpp) as vaultx (we'll call it via bin/vaultx)
g++ -O3 -std=c++17 \
    -Isrc/BLAKE3/c \
    "$ROOT_DIR/src/vaultx.cpp" "$ROOT_DIR/src/BLAKE3/c/blake3.c" \
    -lpthread \
    -o "$ROOT_DIR/bin/vaultx"

# Build searchx
g++ -O3 -std=c++17 \
    "$ROOT_DIR/src/searchx.cpp" \
    -o "$ROOT_DIR/bin/searchx"

echo "Built bin/vaultx and bin/searchx"
