#!/usr/bin/env python3
"""Prepend backfilled idx records to a CLOSED day's live idx shard (offline, on copies).

usage: merge_backfill_day.py <backfill.idx> <live.idx> <out.idx>

Keeps every live record; adds backfill records with ts strictly before the first live
record. Both inputs are flat 56 B MITCH index records (header ts = bytes 2..8, u48 LE,
16 us ticks). Refuses unsorted input. Never touches its inputs; the operator swaps the
closed-day file in with tmp + rename after the writer has rolled to the next day, then
rebuilds .s10 (s10-from-idx) and refreshes both manifests.
"""
import os
import sys

R = 56


def recs(p):
    with open(p, "rb") as f:
        b = f.read()
    if len(b) % R:
        sys.exit(f"{p}: size not a multiple of {R}")
    ts = [int.from_bytes(b[i + 2:i + 8], "little") for i in range(0, len(b), R)]
    if ts != sorted(ts):
        sys.exit(f"{p}: timestamps not sorted")
    return b, ts


if len(sys.argv) != 4:
    sys.exit(__doc__)
src, live, out = sys.argv[1:]
if os.path.exists(out) and any(os.path.samefile(out, x) for x in (src, live)):
    sys.exit("out must differ from both inputs")


bf, bts = recs(src)
lv, lts = recs(live)
cut = lts[0] if lts else 1 << 48
n = sum(1 for t in bts if t < cut)
with open(out + ".tmp", "wb") as f:
    f.write(bf[: n * R] + lv)
os.replace(out + ".tmp", out)
print(f"backfill {n}/{len(bts)} + live {len(lts)} -> {n + len(lts)} records")
