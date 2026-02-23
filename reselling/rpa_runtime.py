"""Shared RPA runtime path helpers."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


def resolve_rpa_output_path(
    project_root: Path,
    *,
    env_key: str = "LIQUIDITY_RPA_JSON_PATH",
    default_path: str = "data/liquidity_rpa_signals.jsonl",
) -> Path:
    raw = (os.getenv(env_key, "") or "").strip() or default_path
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def resolve_rpa_python(
    project_root: Path,
    *,
    env_key: str = "LIQUIDITY_RPA_PYTHON",
) -> str:
    raw = (os.getenv(env_key, "") or "").strip()
    if raw:
        if "/" in raw or raw.startswith(".") or raw.startswith("~"):
            candidate = Path(raw).expanduser()
            if not candidate.is_absolute():
                candidate = (project_root / candidate).resolve()
            if candidate.exists():
                return str(candidate)
        found = shutil.which(raw)
        if found:
            return found
    venv_python = project_root / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable or "python3"
