#!/usr/bin/env python3
"""Synthetic memory hog for guard tests (stdlib only).

Code copied unchanged from the memory-hog script of the earlier private
research tool (see docs/provenance.md) for the internal-guard tests.

Allocates touched memory in chunks at a fixed rate, then holds it, so the
guard's budget kill, probe, and pre-flight paths can be exercised without a
solver.  Every page is written, so the RSS really grows.

    memory_hog.py --gb 2 --rate-gb-s 1 --hold-s 30
"""

from __future__ import annotations

import argparse
import sys
import time

CHUNK = 64 * 1024 * 1024


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gb", type=float, required=True,
                        help="total memory to allocate")
    parser.add_argument("--rate-gb-s", type=float, default=1.0,
                        help="allocation rate")
    parser.add_argument("--hold-s", type=float, default=10.0,
                        help="seconds to hold the memory afterwards")
    args = parser.parse_args()
    target = int(args.gb * 1024 ** 3)
    delay = CHUNK / (args.rate_gb_s * 1024 ** 3)
    blocks = []
    allocated = 0
    started = time.time()
    while allocated < target:
        size = min(CHUNK, target - allocated)
        block = bytearray(size)
        block[::4096] = b"\x01" * len(range(0, size, 4096))
        blocks.append(block)
        allocated += size
        print(f"hog {allocated / 1024 ** 3:.2f} GB at "
              f"{time.time() - started:.1f} s", flush=True)
        time.sleep(delay)
    time.sleep(args.hold_s)
    print(f"hog done: {allocated / 1024 ** 3:.2f} GB held {args.hold_s} s",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
