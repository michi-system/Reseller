#!/usr/bin/env python3
"""Run FX scheduler loop."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.env import load_dotenv
from reselling.scheduler import run_scheduler


def main() -> int:
    load_dotenv(ENV_PATH)

    parser = argparse.ArgumentParser(description="Run FX scheduler")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.getenv("FX_SCHEDULER_INTERVAL_SECONDS", "30")),
    )
    args = parser.parse_args()

    run_scheduler(interval_seconds=args.interval_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

