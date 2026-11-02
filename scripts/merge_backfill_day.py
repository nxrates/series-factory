#!/usr/bin/env python3
"""Prepend backfilled idx records to a CLOSED day's live idx shard (offline, on copies).

usage: merge_backfill_day.py <backfill.idx> <live.idx> <out.idx>

Keeps every live record; adds backfill records with ts strictly before the first live
record. Both inputs are flat 56 B MITCH index records (header ts = bytes 2..8, u48 LE,
16 us ticks). Refuses unsorted input. Never touches its inputs; the operator swaps the
closed-day file in with tmp + rename after the writer has rolled to the next day, then
rebuilds .s10 (s10-from-idx) and refreshes both manifests.
"""
import sys

R = 56


def recs(p):
    b = open(p, "rb").read()
    assert len(b) % R == 0, f"{p}: size not a multiple of {R}"
    ts = [int.from_bytes(b[i + 2:i + 8], "little") for i in range(0, len(b), R)]
    assert ts == sorted(ts), f"{p}: timestamps not sorted"
    return b, ts


bf, bts = recs(sys.argv[1])
lv, lts = recs(sys.argv[2])
cut = lts[0] if lts else 1 << 48
n = sum(1 for t in bts if t < cut)
open(sys.argv[3], "wb").write(bf[: n * R] + lv)
print(f"backfill {n}/{len(bts)} + live {len(lts)} -> {n + len(lts)} records")
