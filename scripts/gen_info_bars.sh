#!/bin/bash
# Generate information bars (Dollar, DIB, TIB) for all pairs at multiple thresholds
set -e
cd "$(dirname "$0")/.."
BIN="./target/release/series-factory"
FROM="2024-03-01"
TO="2026-03-29"

gen() {
  local base=$1 quote=USDT mode=$2 step=$3
  local key="${base,,}-${quote,,}_binance_${FROM//-/}-${TO//-/}_${mode}-${step}"
  local out="output/${key}.bars"
  if [ -f "$out" ]; then
    echo "SKIP: $key (exists)"
    return
  fi
  echo -n "GEN: $key ... "
  RUST_LOG=warn $BIN --base "$base" --quote "$quote" --sources binance \
    --from "$FROM" --to "$TO" --agg-mode "$mode" --agg-step "$step" 2>&1 | \
    grep -oP 'Total aggregates: \K\d+' || true
}

echo "=== Generating Information Bars ==="
echo "  Date range: $FROM → $TO"
echo ""

# BTC-USDT: high volume
for step in 500000 1000000 2000000; do gen BTC dollar $step; done
for step in 2000 5000 10000; do gen BTC dib $step; done
for step in 2000 5000 10000; do gen BTC tib $step; done

# ETH-USDT: medium volume
for step in 200000 500000 1000000; do gen ETH dollar $step; done
for step in 2000 5000 10000; do gen ETH dib $step; done
for step in 2000 5000 10000; do gen ETH tib $step; done

# BNB-USDT: lower volume
for step in 50000 100000 250000; do gen BNB dollar $step; done
for step in 2000 5000 10000; do gen BNB dib $step; done
for step in 2000 5000 10000; do gen BNB tib $step; done

echo ""
echo "=== Done ==="
ls -lhS output/*dollar*.bars output/*dib*.bars output/*tib*.bars 2>/dev/null | awk '{print $5, $NF}'
