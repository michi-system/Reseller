#!/usr/bin/env python3
"""Import a SQLite CSV export bundle into PostgreSQL (Supabase)."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Dict, List


ROOT_DIR = Path(__file__).resolve().parents[1]
BUNDLE_ROOT = ROOT_DIR / "backups" / "sqlite_exports"

IMPORT_ORDER = [
    "fx_rate_states",
    "review_candidates",
    "review_rejections",
    "liquidity_signals",
    "approved_listing_inbox",
    "operator_listings",
    "monitor_snapshots",
    "listing_events",
    "job_runs",
    "operator_config_versions",
]

ID_TABLES = [
    "review_candidates",
    "review_rejections",
    "operator_listings",
    "monitor_snapshots",
    "listing_events",
    "operator_config_versions",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import CSV bundle to PostgreSQL.")
    parser.add_argument(
        "--bundle-dir",
        default="",
        help="CSV bundle directory. Default: latest under backups/sqlite_exports",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="PostgreSQL connection URL. If omitted, loads SUPABASE_DB_URL.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target tables before import (recommended for big-bang).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned operations without DB writes.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute DB writes. Required unless --dry-run is set.",
    )
    return parser


def _latest_bundle_dir() -> Path:
    if not BUNDLE_ROOT.exists():
        raise FileNotFoundError(f"bundle root not found: {BUNDLE_ROOT}")
    dirs = sorted([p for p in BUNDLE_ROOT.iterdir() if p.is_dir()], key=lambda p: p.name)
    if not dirs:
        raise FileNotFoundError(f"no bundle found under: {BUNDLE_ROOT}")
    return dirs[-1]


def _resolve_bundle_dir(raw: str) -> Path:
    if raw:
        p = Path(raw).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"bundle dir not found: {p}")
        return p
    return _latest_bundle_dir()


def _parse_env_file(path: Path) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not path.exists():
        return result
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip().strip("'").strip('"')
    return result


def _resolve_database_url(cli_value: str) -> str:
    if cli_value.strip():
        return cli_value.strip()
    for key in ("SUPABASE_DB_URL",):
        value = os.getenv(key, "").strip()
        if value:
            return value
    env_map = _parse_env_file(ROOT_DIR / ".env.local")
    for key in ("SUPABASE_DB_URL",):
        value = env_map.get(key, "").strip()
        if value:
            return value
    raise RuntimeError("database url not found. set --database-url or SUPABASE_DB_URL")


def _load_manifest(bundle_dir: Path) -> Dict[str, object]:
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid manifest format")
    return payload


def _collect_table_files(manifest: Dict[str, object], bundle_dir: Path) -> Dict[str, Path]:
    table_files: Dict[str, Path] = {}
    dbs = manifest.get("databases", [])
    if not isinstance(dbs, list):
        raise ValueError("invalid manifest: databases must be list")
    for db_item in dbs:
        if not isinstance(db_item, dict):
            continue
        tables = db_item.get("tables", [])
        if not isinstance(tables, list):
            continue
        for table_item in tables:
            if not isinstance(table_item, dict):
                continue
            table = str(table_item.get("table", "") or "").strip()
            csv_path = str(table_item.get("csv_path", "") or "").strip()
            if not table or not csv_path:
                continue
            csv_abs = (bundle_dir / csv_path).resolve()
            if csv_abs.exists():
                table_files[table] = csv_abs
    return table_files


def _csv_headers(csv_path: Path) -> List[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return []
    return [h.strip() for h in headers if h.strip()]


def _run_import(
    *,
    database_url: str,
    table_files: Dict[str, Path],
    truncate: bool,
    dry_run: bool,
) -> int:
    if dry_run:
        for table in IMPORT_ORDER:
            csv_path = table_files.get(table)
            if not csv_path:
                continue
            headers = _csv_headers(csv_path)
            print(f"[dry-run] import {table} <- {csv_path} columns={len(headers)}")
        return 0

    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is required. install with: python3 -m pip install 'psycopg[binary]'"
        ) from exc

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            if truncate:
                truncate_list = sql.SQL(", ").join(sql.Identifier(t) for t in IMPORT_ORDER)
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(truncate_list))
                print("[ok] truncated target tables")

            for table in IMPORT_ORDER:
                csv_path = table_files.get(table)
                if not csv_path:
                    continue
                headers = _csv_headers(csv_path)
                if not headers:
                    print(f"[skip] header empty: {table}")
                    continue

                column_list = sql.SQL(", ").join(sql.Identifier(c) for c in headers)
                # Keep empty CSV fields as empty strings; only '\N' is treated as NULL.
                query = sql.SQL(
                    "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '\\\\N')"
                ).format(
                    sql.Identifier(table),
                    column_list,
                )
                with csv_path.open("r", encoding="utf-8", newline="") as f:
                    with cur.copy(query) as copy:
                        while True:
                            chunk = f.read(65536)
                            if not chunk:
                                break
                            copy.write(chunk)
                print(f"[ok] imported: {table} <- {csv_path}")

            for table in ID_TABLES:
                if table not in table_files:
                    continue
                cur.execute(
                    sql.SQL(
                        """
                        SELECT setval(
                            pg_get_serial_sequence(%s, 'id'),
                            COALESCE((SELECT MAX(id) FROM {table_name}), 1),
                            COALESCE((SELECT MAX(id) FROM {table_name}), 0) > 0
                        )
                        """
                    ).format(table_name=sql.Identifier(table)),
                    (table,),
                )
            print("[ok] serial sequence synchronized")
        conn.commit()
    return 0


def main() -> int:
    args = build_parser().parse_args()
    dry_run = bool(args.dry_run)
    if not dry_run and not args.apply:
        raise SystemExit("import requires --apply (or use --dry-run)")

    bundle_dir = _resolve_bundle_dir(args.bundle_dir)
    manifest = _load_manifest(bundle_dir)
    table_files = _collect_table_files(manifest, bundle_dir)
    if not table_files:
        raise SystemExit(f"no importable csv found in: {bundle_dir}")

    database_url = ""
    if not dry_run:
        database_url = _resolve_database_url(args.database_url)
    print(f"[info] bundle: {bundle_dir}")
    print(f"[info] tables: {len(table_files)}")
    if dry_run:
        print("[info] mode: dry-run")
    elif args.truncate:
        print("[info] mode: apply + truncate")
    else:
        print("[info] mode: apply (append)")

    return _run_import(
        database_url=database_url,
        table_files=table_files,
        truncate=bool(args.truncate),
        dry_run=dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
