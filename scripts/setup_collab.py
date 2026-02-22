#!/usr/bin/env python3
"""Quick setup for collaborator environments (Codex on another account/Mac)."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_LOCAL = ROOT_DIR / ".env.local"
VENV_PY = ROOT_DIR / ".venv" / "bin" / "python"


def _run(cmd: list[str], *, cwd: Path = ROOT_DIR) -> None:
    subprocess.run(cmd, cwd=str(cwd), check=True)


def _ensure_venv() -> None:
    if VENV_PY.exists():
        return
    _run([sys.executable, "-m", "venv", ".venv"])


def _parse_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def _upsert_env(path: Path, updates: dict[str, str]) -> None:
    original_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    out_lines: list[str] = []
    for raw in original_lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            out_lines.append(raw)
            continue
        key, _value = line.split("=", 1)
        key = key.strip()
        if key in updates:
            out_lines.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            out_lines.append(raw)
    for key, value in updates.items():
        if key not in seen:
            out_lines.append(f"{key}={value}")
    path.write_text("\n".join(out_lines).rstrip() + "\n", encoding="utf-8")


def _derive_supabase_url(db_url: str) -> str:
    # Example:
    # postgresql://postgres:***@db.vtspugzncsxmurzlhfyg.supabase.co:5432/postgres
    marker = "@db."
    suffix = ".supabase.co"
    if marker not in db_url:
        return ""
    tail = db_url.split(marker, 1)[1]
    host = tail.split("/", 1)[0].split(":", 1)[0]
    if not host.endswith(suffix):
        return ""
    project_ref = host[: -len(suffix)]
    return f"https://{project_ref}.supabase.co"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-shot collaborator setup")
    parser.add_argument(
        "--mode",
        choices=("postgres", "sqlite"),
        default="postgres",
        help="DB backend mode for this machine (default: postgres)",
    )
    parser.add_argument(
        "--supabase-db-url",
        default="",
        help="SUPABASE_DB_URL value. Required for --mode postgres if not already set in .env.local.",
    )
    parser.add_argument(
        "--api-host",
        default="127.0.0.1",
        help="API host to write into .env.local (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=8012,
        help="API port to write into .env.local (default: 8012)",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip pip install/update steps.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    _ensure_venv()
    if not args.skip_install:
        _run([str(VENV_PY), "-m", "pip", "install", "--upgrade", "pip"])
        if args.mode == "postgres":
            _run([str(VENV_PY), "-m", "pip", "install", "psycopg[binary]"])

    current = _parse_env(ENV_LOCAL)
    db_url = (args.supabase_db_url or current.get("SUPABASE_DB_URL", "")).strip()
    if args.mode == "postgres" and not db_url:
        raise SystemExit(
            "SUPABASE_DB_URL is required for postgres mode. "
            "Pass --supabase-db-url or set it in .env.local first."
        )

    updates = {
        "DB_BACKEND": args.mode,
        "API_HOST": args.api_host,
        "API_PORT": str(max(1, int(args.api_port))),
    }
    if args.mode == "postgres":
        updates["SUPABASE_DB_URL"] = db_url
        derived = _derive_supabase_url(db_url)
        if derived:
            updates["SUPABASE_URL"] = derived
    _upsert_env(ENV_LOCAL, updates)

    print("[ok] collaborator setup completed")
    print(f"[ok] mode={args.mode}")
    print("[next] run: python3 scripts/run_api.py")
    print(f"[next] verify: curl http://{updates['API_HOST']}:{updates['API_PORT']}/healthz")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
