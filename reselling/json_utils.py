"""Shared JSON file helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


def load_json_dict(
    path: Path,
    *,
    required: bool = False,
    missing_message: Optional[str] = None,
    invalid_message: Optional[str] = None,
) -> Dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(missing_message or f"not found: {path}")
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if required:
            raise
        return {}
    if not isinstance(payload, dict):
        if required:
            raise ValueError(invalid_message or "JSON must be an object")
        return {}
    return payload


def save_json_dict(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def extract_json_path(payload: Dict[str, Any], path: str) -> Any:
    current: Any = payload
    for part in str(path or "").split("."):
        key = str(part or "").strip()
        if not key:
            continue
        if not isinstance(current, dict) or key not in current:
            return None
        current = current.get(key)
    return current
