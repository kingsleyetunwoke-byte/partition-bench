"""Command line: python -m pbench [--impl naive|fenced|both] [--seeds N]"""

from __future__ import annotations

import argparse

from pbench.explore import search
from pbench.model import IMPLEMENTATIONS, STRATEGIES


def main() -> None:
    parser = argparse.ArgumentParser(prog="pbench", description=__doc__)
    parser.add_argument("--impl", choices=[*IMPLEMENTATIONS, "both"], default="both")
    parser.add_argument("--quiet", action="store_true", help="omit traces")
    parser.add_argument("--seeds", type=int, default=500)
    parser.add_argument("--strategy", choices=list(STRATEGIES), default="contiguous")
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--max-steps", type=int, default=300)
    args = parser.parse_args()

    impls = ["naive", "fenced"] if args.impl == "both" else [args.impl]
    config = dict(
        strategy=args.strategy, partitions=args.partitions, max_steps=args.max_steps
    )
    for impl in impls:
        report = search(impl, args.seeds, **config)
        failed = sum(report["failures"].values())
        print(f"== {impl}: {failed}/{args.seeds} schedules failed {report['failures'] or ''}")
        for kind, (seed, length, minimal) in report["examples"].items():
            print(f"   {kind}, seed {seed}: {length} choices shrunk to {len(minimal.choices)}")
            print(f"   {minimal.violation}")
            print(f"   replay: run({impl!r}, prefix={minimal.choices})")
            for line in [] if args.quiet else minimal.trace:
                print(f"     {line}")


if __name__ == "__main__":
    main()
