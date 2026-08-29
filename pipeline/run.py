"""Entry point. Run stages individually or the whole pipeline.

    python -m pipeline.run all         # extract -> transform -> test -> publish
    python -m pipeline.run extract
    python -m pipeline.run build       # transform -> test -> publish (no network)
    python -m pipeline.run backfill    # wide extract, then the full build
"""
from __future__ import annotations

import argparse
import logging
import sys

from . import extract, publish, quality, transform
from .config import BACKFILL_DAYS


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)-18s %(message)s",
        datefmt="%H:%M:%S",
    )


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    log = logging.getLogger("pipeline")

    parser = argparse.ArgumentParser(description="Egypt air quality pipeline")
    parser.add_argument(
        "stage",
        nargs="?",
        default="all",
        choices=["all", "extract", "transform", "test", "publish", "build",
                 "backfill"],
    )
    parser.add_argument(
        "--days",
        type=int,
        default=BACKFILL_DAYS,
        help=f"days of history for backfill (default {BACKFILL_DAYS}, max 92)",
    )
    args = parser.parse_args(argv)

    try:
        if args.stage == "extract":
            extract.run()
            return 0

        if args.stage == "backfill":
            log.info("backfilling %s days of history", args.days)
            extract.run(past_days=args.days)
        elif args.stage in ("all",):
            extract.run()

        con = transform.run()

        if args.stage == "transform":
            return 0

        checks = quality.run(con)

        if args.stage == "test":
            return 0

        publish.run(con, checks)
        log.info("pipeline finished successfully")
        return 0

    except Exception as exc:  # noqa: BLE001
        log.error("pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
