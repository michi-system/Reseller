#!/usr/bin/env python3
"""Legacy entrypoint kept for backward compatibility.

Canonical module is `scripts/rpa_market_research.py`.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
CANONICAL_PATH = ROOT_DIR / "scripts" / "rpa_market_research.py"
SUNSET_DATE = "2026-03-31"


def _load_canonical_module():
    spec = importlib.util.spec_from_file_location("rpa_market_research_canonical", CANONICAL_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load canonical module: {CANONICAL_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_CANONICAL = _load_canonical_module()

for _name in dir(_CANONICAL):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_CANONICAL, _name)


def main() -> int:
    if str(os.getenv("SUPPRESS_INTERNAL_DEPRECATION_WARN", "0")).strip() not in {"1", "true", "yes"}:
        sys.stderr.write(
            "[deprecated] scripts/rpa_ebay_product_research.py is legacy.\n"
            f"[deprecated] Use scripts/rpa_market_research.py (planned removal after {SUNSET_DATE}).\n"
        )
    return int(_CANONICAL.main())


if __name__ == "__main__":
    raise SystemExit(main())
