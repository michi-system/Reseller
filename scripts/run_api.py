#!/usr/bin/env python3
"""Run the minimal JSON API server."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.api_server import run_server
from reselling.env import load_dotenv


def main() -> int:
    load_dotenv(ENV_PATH)

    parser = argparse.ArgumentParser(description="Run API server")
    parser.add_argument("--host", default=os.getenv("API_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "8000")))
    args = parser.parse_args()

    run_server(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

