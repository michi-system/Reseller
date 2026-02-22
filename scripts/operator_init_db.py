#!/usr/bin/env python3
"""Initialize Operator DB and seed default config."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from listing_ops.config import load_operator_settings
from listing_ops.models import connect, init_db
from listing_ops.runtime_config import ensure_and_get_active_config


def main() -> int:
    settings = load_operator_settings()
    conn = connect(settings.db_path)
    init_db(conn)
    active = ensure_and_get_active_config(conn, settings)
    conn.close()
    print(
        json.dumps(
            {
                "db_path": str(settings.db_path),
                "active_config": active,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
