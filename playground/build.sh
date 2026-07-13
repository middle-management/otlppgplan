#!/usr/bin/env bash
# Builds the Go converter to WebAssembly for the playground.
# Produces playground/convert.wasm and playground/wasm_exec.js.
set -euo pipefail
cd "$(dirname "$0")/.."

GOOS=js GOARCH=wasm go build -trimpath -ldflags='-s -w' -o playground/convert.wasm ./cmd/wasm

# wasm_exec.js moved from misc/wasm to lib/wasm in Go 1.24
GOROOT="$(go env GOROOT)"
if [ -f "$GOROOT/lib/wasm/wasm_exec.js" ]; then
  cp "$GOROOT/lib/wasm/wasm_exec.js" playground/wasm_exec.js
else
  cp "$GOROOT/misc/wasm/wasm_exec.js" playground/wasm_exec.js
fi

ls -lh playground/convert.wasm
