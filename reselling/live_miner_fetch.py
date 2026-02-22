"""Live marketplace fetch for miner candidates."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .config import Settings, load_settings
from .liquidity import estimate_ev90, evaluate_liquidity_gate, get_liquidity_signal
from .models import connect, init_db
from .profit import ProfitInput, calculate_profit
from .miner import create_miner_candidate


_EBAY_TOKEN_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0.0}
_DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def _promote_legacy_data_path(
    new_name: str,
    legacy_name: str,
    *,
    is_dir: bool = False,
) -> Path:
    new_path = _DATA_DIR / new_name
    legacy_path = _DATA_DIR / legacy_name
    if new_path.exists() or not legacy_path.exists():
        return new_path
    new_path.parent.mkdir(parents=True, exist_ok=True)
    if is_dir:
        shutil.copytree(legacy_path, new_path, dirs_exist_ok=True)
    else:
        shutil.copy2(legacy_path, new_path)
    return new_path


_BLOCKLIST_PATH = _promote_legacy_data_path("miner_blocklist.json", "review_blocklist.json")
_FETCH_CURSOR_PATH = _promote_legacy_data_path("miner_fetch_cursor.json", "review_fetch_cursor.json")
_FETCH_TUNER_PATH = _promote_legacy_data_path("miner_fetch_tuner.json", "review_fetch_tuner.json")
_API_CACHE_DIR = _promote_legacy_data_path("miner_api_cache", "review_api_cache", is_dir=True)
_API_USAGE_PATH = _promote_legacy_data_path("miner_api_usage.json", "review_api_usage.json")
_QUERY_SKIP_PATH = _promote_legacy_data_path("miner_query_skip.json", "review_query_skip.json")
_RPA_FETCH_STATE_PATH = Path(__file__).resolve().parents[1] / "data" / "liquidity_rpa_fetch_state.json"
_RPA_PROGRESS_PATH = Path(__file__).resolve().parents[1] / "data" / "liquidity_rpa_progress.json"
_CATEGORY_KNOWLEDGE_PATH = Path(__file__).resolve().parents[1] / "data" / "category_knowledge_seeds_v1.json"
_CATEGORY_KNOWLEDGE_CACHE: Dict[str, Any] = {"mtime": 0.0, "payload": {}}
_CATEGORY_BRAND_CACHE: Dict[str, Any] = {"mtime": 0.0, "brands": {}}
_RPA_DAILY_LIMIT_EXIT_CODE = 75
_RPA_DAILY_LIMIT_PATTERNS = (
    re.compile(r"exceeded\s+the\s+number\s+of\s+requests\s+allowed\s+in\s+one\s+day", re.IGNORECASE),
    re.compile(r"please\s+try\s+again\s+tomorrow", re.IGNORECASE),
    re.compile(r"number\s+of\s+requests\s+allowed\s+in\s+one\s+day", re.IGNORECASE),
)

_HEADER_ERROR_KEY = "x-reseller-error"
_HEADER_CACHE_HIT_KEY = "x-reseller-cache-hit"
_HEADER_CACHE_AGE_SEC_KEY = "x-reseller-cache-age-sec"
_HEADER_BUDGET_REMAINING_KEY = "x-reseller-budget-remaining"

_HEADER_LEGACY_ERROR_KEY = "x-ebayminer-error"
_HEADER_LEGACY_CACHE_HIT_KEY = "x-ebayminer-cache-hit"
_HEADER_LEGACY_CACHE_AGE_SEC_KEY = "x-ebayminer-cache-age-sec"
_HEADER_LEGACY_BUDGET_REMAINING_KEY = "x-ebayminer-budget-remaining"

_HEADER_ERROR_KEYS = (_HEADER_ERROR_KEY, _HEADER_LEGACY_ERROR_KEY)
_HEADER_CACHE_HIT_KEYS = (_HEADER_CACHE_HIT_KEY, _HEADER_LEGACY_CACHE_HIT_KEY)
_HEADER_CACHE_AGE_SEC_KEYS = (_HEADER_CACHE_AGE_SEC_KEY, _HEADER_LEGACY_CACHE_AGE_SEC_KEY)
_HEADER_BUDGET_REMAINING_KEYS = (_HEADER_BUDGET_REMAINING_KEY, _HEADER_LEGACY_BUDGET_REMAINING_KEY)

_STOPWORDS = {
    "THE",
    "WITH",
    "FOR",
    "FROM",
    "AND",
    "NEW",
    "NIB",
    "WATCH",
    "JAPAN",
    "JAPANESE",
    "UNUSED",
    "AUTHENTIC",
    "FREE",
    "SHIPPING",
    "SEIKO",
    "CASIO",
    "CITIZEN",
    "新品",
    "国内正規品",
    "送料無料",
}

_CODE_RE = re.compile(r"[A-Z0-9][A-Z0-9-]{3,}")
_PRICE_JPY_RE = re.compile(r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{4,7})\s*円", re.IGNORECASE)
_USED_TERMS = {
    "USED",
    "PRE-OWNED",
    "REFURB",
    "REFURBISHED",
    "SECOND HAND",
    "NEAR MINT",
    "MINT",
    "中古",
    "美品",
    "ジャンク",
    "訳あり",
    "展示品",
    "良品",
}
_USED_PATTERNS = (
    re.compile(r"中\s*古"),
    re.compile(r"未\s*使\s*用\s*に\s*近\s*い"),
    re.compile(r"や\s*や\s*傷"),
    re.compile(r"傷\s*や\s*汚\s*れ"),
    re.compile(r"目\s*立\s*っ\s*た\s*傷"),
    re.compile(r"全\s*体\s*的\s*に\s*状\s*態\s*が\s*悪"),
    re.compile(r"動\s*作\s*未\s*確\s*認"),
    re.compile(r"現\s*状\s*品"),
    re.compile(r"[ABCDS]\s*ラ\s*ン\s*ク"),
    re.compile(r"電\s*池\s*交\s*換\s*済"),
    re.compile(r"オ\s*ー\s*バ\s*ー\s*ホ\s*ー\s*ル\s*済"),
    re.compile(r"\bOH\s*済\b", re.IGNORECASE),
    re.compile(r"新\s*品\s*仕\s*上\s*げ\s*済"),
)
_OUT_OF_STOCK_TERMS = {
    "在庫なし",
    "在庫切れ",
    "在庫がありません",
    "売り切れ",
    "売切れ",
    "欠品",
    "入荷待ち",
    "販売終了",
    "取扱終了",
    "SOLD OUT",
    "OUT OF STOCK",
}
_OUT_OF_STOCK_PATTERNS = (
    re.compile(r"在\s*庫\s*が\s*あ\s*り\s*ま\s*せ\s*ん"),
    re.compile(r"在\s*庫\s*な\s*し"),
    re.compile(r"在\s*庫\s*切\s*れ"),
    re.compile(r"売\s*り\s*切\s*れ"),
    re.compile(r"入\s*荷\s*待\s*ち"),
    re.compile(r"欠\s*品"),
    re.compile(r"販\s*売\s*終\s*了"),
    re.compile(r"\bSOLD\s*OUT\b", re.IGNORECASE),
    re.compile(r"\bOUT\s+OF\s+STOCK\b", re.IGNORECASE),
)
_ACCESSORY_TERMS = {
    "BAND",
    "STRAP",
    "BRACELET",
    "BUCKLE",
    "LINK",
    "BELT",
    "PART",
    "PARTS",
    "ベルト",
    "バンド",
    "バックル",
    "ブレス",
    "コマ",
    "替えベルト",
    "部品",
    "パーツ",
    "アクセサリー",
    "ACCESSORY",
    "ACCESSORIES",
    "ケース",
    "CASE",
    "COVER",
    "イヤーピース",
    "イヤチップ",
    "EARTIP",
    "EAR TIP",
    "EAR TIPS",
    "EARPAD",
    "EAR PAD",
    "FILM",
    "GLASS FILM",
    "SCREEN PROTECTOR",
    "PROTECTOR",
    "保護フィルム",
    "ガラスフィルム",
    "強化ガラス",
    "液晶保護",
}
_ACCESSORY_HINTS = {
    "REPLACEMENT",
    "COMPATIBLE",
    " FOR ",
    "対応",
    "交換",
    "替え",
    "互換",
    "単品",
}
_STRONG_ACCESSORY_TERMS = {
    "WATCH BAND",
    "BAND STRAP",
    "WATCH STRAP",
    "WATCH BRACELET",
    "STRAP FOR",
    "REPLACEMENT BAND",
    "REPLACEMENT STRAP",
    "REPLACEMENT BRACELET",
    "BRACELET FOR",
    "SCREW",
    "PART",
    "PARTS",
    "BUCKLE",
    "SPARE LINK",
    "BAND LINK",
    "WATCH CASE",
    "METAL CASE",
    "MODIFIED CASE",
    "CASE MOD",
    "CASE FOR",
    "COVER FOR",
    "EAR TIPS",
    "EARPAD",
    "EAR PADS",
    "EARTIP",
    "ACCESSORY",
    "ACCESSORIES",
    "GLASS FILM",
    "SCREEN PROTECTOR",
    "PROTECTIVE FILM",
    "保護フィルム",
    "ガラスフィルム",
    "強化ガラス",
    "液晶保護",
    "ビス",
    "パーツ",
    "替えベルト",
    "バックル",
    "純正コマ",
    "コマ",
}
_FORCE_ACCESSORY_TERMS = {
    "BUCKLE",
    "BAND LINK",
    "SPARE LINK",
    "純正バンド",
    "純正 バンド",
    "純正ベルト",
    "純正 ベルト",
    "純正ブレス",
    "純正 ブレス",
    "純正コマ",
    "バックル",
    "ブレス",
    "イヤーチップ",
    "イヤーパッド",
    "イヤホンケース",
    "保護ケース",
    "ケースカバー",
    "ケース カバー",
    "シリコンケース",
    "ケース",
    "カバー",
    "レンズフィルター",
    "保護フィルム",
    "イヤーピース",
    "イヤチップ",
    "EARTIP",
    "EAR TIP",
    "EAR TIPS",
    "EARPAD",
    "EAR PAD",
    "ACCESSORY",
    "ACCESSORIES",
    "GLASS FILM",
    "SCREEN PROTECTOR",
    "保護フィルム",
    "ガラスフィルム",
    "強化ガラス",
    "液晶保護",
}
_WATCH_CORE_TERMS = {
    "WATCH",
    "腕時計",
    "時計",
    "G-SHOCK",
    "PROSPEX",
    "PROMASTER",
    "AUTOMATIC",
    "ECO-DRIVE",
    "DIVER",
    "MECHANICAL",
}
_MEASUREMENT_CODE_RE = re.compile(
    r"^\d+(?:\.\d+)?(?:MAH|WH|W|V|A|MM|CM|M|IN|GB|TB|MP|HZ)$",
    re.IGNORECASE,
)
_COLOR_ALIASES: Dict[str, Tuple[str, ...]] = {
    "black": ("BLACK", "BLK", "ブラック", "黒"),
    "white": ("WHITE", "WHT", "ホワイト", "白"),
    "silver": ("SILVER", "SLV", "シルバー", "銀"),
    "gold": ("GOLD", "GLD", "ゴールド", "金"),
    "blue": ("BLUE", "BLU", "NAVY", "ネイビー", "ブルー", "青", "紺"),
    "green": ("GREEN", "GRN", "グリーン", "緑"),
    "red": ("RED", "レッド", "赤"),
    "orange": ("ORANGE", "ORG", "オレンジ"),
    "yellow": ("YELLOW", "YLW", "イエロー", "黄"),
    "pink": ("PINK", "ピンク"),
    "purple": ("PURPLE", "パープル", "紫"),
    "gray": ("GRAY", "GREY", "GRY", "グレー", "グレー", "灰", "ガンメタ"),
    "brown": ("BROWN", "BRN", "ブラウン", "茶"),
    "beige": ("BEIGE", "ベージュ"),
    "clear": ("CLEAR", "TRANSPARENT", "クリア", "透明", "スモーキー"),
}
_VARIANT_COLOR_CODE_MAP: Dict[str, str] = {
    "B": "black",
    "K": "black",
    "S": "silver",
    "P": "pink",
    "R": "red",
    "G": "green",
    "W": "white",
}
_MOD_TERMS = {
    " MOD ",
    "MODDED",
    "MODIFIED",
    "CUSTOM",
    "CUSTOMIZED",
    "CASIOAK",
    "カスタム",
    "改造",
}
_PRIMARY_FAMILY_TERMS: Dict[str, Tuple[str, ...]] = {
    "watch": ("WATCH", "腕時計", "時計", "G-SHOCK", "PROSPEX", "OCEANUS", "PROMASTER"),
    "power_bank": ("POWER BANK", "POWERCORE", "モバイルバッテリー"),
    "charger": ("CHARGER", "充電器", "ADAPTER", "ACアダプタ", "ACアダプター", "電源アダプタ"),
    "cable": ("CABLE", "ケーブル"),
    "hub": ("HUB", "DOCK", "ハブ", "ドック"),
    "earbuds": ("EARBUD", "EARBUDS", "EARPHONE", "イヤホン", "WF-"),
    "headphone": ("HEADPHONE", "HEADPHONES", "ヘッドホン", "WH-", "MDR-"),
    "speaker": ("SPEAKER", "スピーカー"),
    "camera": ("CAMERA", "カメラ", "VLOGCAM", "ミラーレス"),
    "lens": ("LENS", "レンズ"),
}
_BUNDLE_TERMS = {
    "KIT",
    "BUNDLE",
    "SET",
    "LENS KIT",
    "レンズキット",
    "キット",
    "セット",
    "ダブルズーム",
    "トリプルレンズ",
}
_BODY_ONLY_TERMS = {
    "BODY ONLY",
    "CAMERA BODY",
    "ボディ",
    "本体のみ",
    "ボディのみ",
}

_QUERY_NOISE_TERMS = {
    "NEW",
    "新品",
    "未使用",
    "送料無料",
    "FREE",
    "SHIPPING",
}

_QUERY_STOPWORDS = {
    "THE",
    "WITH",
    "FOR",
    "FROM",
    "AND",
    "NEW",
    "NIB",
    "JAPAN",
    "JAPANESE",
    "UNUSED",
    "AUTHENTIC",
    "FREE",
    "SHIPPING",
    "新品",
    "国内正規品",
    "送料無料",
}

_GENERIC_TOKENS = {
    "WATCH",
    "CLOCK",
    "SPEAKER",
    "HEADPHONE",
    "CAMERA",
    "LENS",
    "BAG",
    "WALLET",
    "SHOES",
    "TOY",
    "FIGURE",
}

_CATEGORY_QUERY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "watch": ("watch", "watches", "腕時計", "時計"),
    "sneakers": ("sneakers", "sneaker", "スニーカー"),
    "streetwear": ("streetwear", "street wear", "ストリートウェア"),
    "trading_cards": ("trading cards", "trading card", "tcg", "トレーディングカード", "トレカ"),
    "toys_collectibles": ("toys", "collectibles", "toy", "figure", "フィギュア", "ホビー", "コレクティブル"),
    "video_game_consoles": ("video game console", "game console", "console", "ゲーム機", "ゲーム機本体"),
    "camera_lenses": ("camera lens", "camera lenses", "lens", "レンズ", "交換レンズ"),
}
_CATEGORY_NOUN_HINT: Dict[str, str] = {
    "watch": "watch",
    "sneakers": "sneakers",
    "streetwear": "streetwear",
    "trading_cards": "trading cards",
    "toys_collectibles": "figure",
    "video_game_consoles": "game console",
    "camera_lenses": "lens",
}
_CATEGORY_PLACEHOLDER_MODEL_TERMS = {
    "set_number",
    "figure_code",
    "edition_code",
    "jan specific variants",
}
_CATEGORY_QUERY_NOISE_TOKENS = {
    "new",
    "unused",
    "category",
    "in",
    "stock",
    "新品",
    "未使用",
    "カテゴリ",
    "在庫",
    "在庫あり",
}


@dataclass(frozen=True)
class MarketItem:
    site: str
    item_id: str
    title: str
    item_url: str
    image_url: str
    price: float
    shipping: float
    currency: str
    condition: str
    identifiers: Dict[str, str]
    raw: Dict[str, Any]


@dataclass(frozen=True)
class SiteFetchProfile:
    site: str
    max_calls: int
    per_call_limit: int
    target_items: int
    min_new_items: int
    max_pages_per_query: int
    sleep_sec: float


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _legacy_env_key(key: str) -> str:
    if key.startswith("AUTO_MINER_"):
        return f"AUTO_REVIEW_{key[len('AUTO_MINER_'):]}"
    if key.startswith("MINER_"):
        return f"REVIEW_{key[len('MINER_'):]}"
    return ""


def _env_raw(key: str) -> str:
    raw = (os.getenv(key, "") or "").strip()
    if raw:
        return raw
    legacy_key = _legacy_env_key(key)
    if not legacy_key:
        return ""
    return (os.getenv(legacy_key, "") or "").strip()


def _env_int(key: str, default: int) -> int:
    raw = _env_raw(key)
    if not raw:
        return default
    return _to_int(raw, default)


def _env_float(key: str, default: float) -> float:
    raw = _env_raw(key)
    if not raw:
        return default
    return _to_float(raw, default)


def _env_bool(key: str, default: bool = False) -> bool:
    raw = _env_raw(key).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json_file(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _rpa_progress_default() -> Dict[str, Any]:
    now_ts = int(time.time())
    return {
        "status": "idle",
        "phase": "idle",
        "message": "待機中",
        "progress_percent": 0.0,
        "run_id": "",
        "query": "",
        "query_index": 0,
        "total_queries": 0,
        "started_at_epoch": 0,
        "updated_at_epoch": now_ts,
        "ended_at_epoch": 0,
        "daily_limit_reached": False,
        "reason": "",
        "stdout_tail": [],
        "stderr_tail": [],
    }


def _load_rpa_progress() -> Dict[str, Any]:
    payload = _load_json_file(_RPA_PROGRESS_PATH)
    if not payload:
        return _rpa_progress_default()
    merged = _rpa_progress_default()
    merged.update(payload)
    return merged


def _save_rpa_progress(payload: Dict[str, Any]) -> None:
    base = _rpa_progress_default()
    if isinstance(payload, dict):
        base.update(payload)
    base["progress_percent"] = round(max(0.0, min(100.0, _to_float(base.get("progress_percent"), 0.0))), 2)
    base["query_index"] = max(0, _to_int(base.get("query_index"), 0))
    base["total_queries"] = max(0, _to_int(base.get("total_queries"), 0))
    base["updated_at_epoch"] = max(0, _to_int(base.get("updated_at_epoch"), int(time.time())))
    base["started_at_epoch"] = max(0, _to_int(base.get("started_at_epoch"), 0))
    base["ended_at_epoch"] = max(0, _to_int(base.get("ended_at_epoch"), 0))
    base["stdout_tail"] = [str(v) for v in (base.get("stdout_tail") if isinstance(base.get("stdout_tail"), list) else [])][-30:]
    base["stderr_tail"] = [str(v) for v in (base.get("stderr_tail") if isinstance(base.get("stderr_tail"), list) else [])][-30:]
    _save_json_file(_RPA_PROGRESS_PATH, base)


def get_rpa_progress_snapshot() -> Dict[str, Any]:
    state = _load_rpa_progress()
    now_ts = int(time.time())
    state["updated_ago_sec"] = (
        max(0, now_ts - _to_int(state.get("updated_at_epoch"), now_ts))
        if _to_int(state.get("updated_at_epoch"), 0) > 0
        else -1
    )
    return state


def _resolve_rpa_output_path() -> Path:
    raw = (os.getenv("LIQUIDITY_RPA_JSON_PATH", "") or "").strip() or "data/liquidity_rpa_signals.jsonl"
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (Path(__file__).resolve().parents[1] / path).resolve()
    return path


def _resolve_rpa_python() -> str:
    raw = (os.getenv("LIQUIDITY_RPA_PYTHON", "") or "").strip()
    project_root = Path(__file__).resolve().parents[1]
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


def _normalize_rpa_queries(queries: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in queries:
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _contains_rpa_daily_limit_message(text: str) -> bool:
    haystack = str(text or "")
    if not haystack:
        return False
    matched = 0
    for pattern in _RPA_DAILY_LIMIT_PATTERNS:
        if pattern.search(haystack):
            matched += 1
    return matched >= 2


def _rpa_daily_limit_reached(summary: Any) -> bool:
    if not isinstance(summary, dict):
        return False
    reason = str(summary.get("reason", "") or "").strip().lower()
    if reason == "daily_limit_reached":
        return True
    if bool(summary.get("daily_limit_reached")):
        return True
    return False


def _tail_append(lines: List[str], line: str, *, max_len: int = 30) -> None:
    text = str(line or "").strip()
    if not text:
        return
    lines.append(text)
    if len(lines) > max_len:
        del lines[: len(lines) - max_len]


def _parse_rpa_progress_line(line: str) -> Optional[Dict[str, Any]]:
    text = str(line or "").strip()
    if not text.startswith("[progress]"):
        return None
    raw = text[len("[progress]") :].strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _run_rpa_collect_for_fetch(queries: Sequence[str]) -> Dict[str, Any]:
    query_list = _normalize_rpa_queries(queries)
    if not query_list:
        _save_rpa_progress(
            {
                "status": "idle",
                "phase": "idle",
                "message": "RPA実行対象クエリがありません",
                "progress_percent": 0.0,
                "reason": "empty_queries",
                "updated_at_epoch": int(time.time()),
            }
        )
        return {"enabled": True, "ran": False, "reason": "empty_queries", "queries": []}

    max_queries = max(1, _env_int("LIQUIDITY_RPA_FETCH_MAX_QUERIES", 3))
    query_list = query_list[:max_queries]
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "rpa_market_research.py"
    if not script_path.exists():
        _save_rpa_progress(
            {
                "status": "failed",
                "phase": "error",
                "message": "RPAスクリプトが見つかりません",
                "progress_percent": 100.0,
                "reason": "rpa_script_missing",
                "updated_at_epoch": int(time.time()),
                "ended_at_epoch": int(time.time()),
                "total_queries": len(query_list),
            }
        )
        return {
            "enabled": True,
            "ran": False,
            "reason": "rpa_script_missing",
            "script_path": str(script_path),
            "queries": query_list,
        }

    output_path = _resolve_rpa_output_path()
    cmd: List[str] = [
        _resolve_rpa_python(),
        str(script_path),
        "--output",
        str(output_path),
        "--pause-for-login",
        str(max(0, _env_int("LIQUIDITY_RPA_PAUSE_FOR_LOGIN_SECONDS", 0))),
        "--wait-seconds",
        str(max(3, _env_int("LIQUIDITY_RPA_WAIT_SECONDS", 8))),
        "--lookback-days",
        str(max(7, _env_int("LIQUIDITY_RPA_LOOKBACK_DAYS", 90))),
        "--inter-query-sleep",
        str(max(0.0, _env_float("LIQUIDITY_RPA_INTER_QUERY_SLEEP", 0.4))),
        "--condition",
        str((os.getenv("LIQUIDITY_RPA_PRIMARY_CONDITION", "new") or "new").strip() or "new"),
        "--pass-label",
        "fetch_refresh",
    ]
    profile_dir = (os.getenv("LIQUIDITY_RPA_PROFILE_DIR", "") or "").strip()
    if profile_dir:
        cmd.extend(["--profile-dir", profile_dir])
    login_url = (os.getenv("LIQUIDITY_RPA_LOGIN_URL", "") or "").strip()
    if login_url:
        cmd.extend(["--login-url", login_url])
    force_headless = _env_bool("LIQUIDITY_RPA_FORCE_HEADLESS", True)
    requested_headless = _env_bool("LIQUIDITY_RPA_HEADLESS", True)
    if force_headless or requested_headless:
        cmd.append("--headless")
    if _env_bool("LIQUIDITY_RPA_PRIMARY_STRICT_CONDITION", True):
        cmd.append("--strict-condition")
    if _env_bool("LIQUIDITY_RPA_PRIMARY_FIXED_PRICE_ONLY", True):
        cmd.append("--fixed-price-only")
    for query in query_list:
        cmd.extend(["--query", query])

    started_at = int(time.time())
    run_id = f"fetch-{started_at}-{hashlib.sha1('|'.join(query_list).encode('utf-8')).hexdigest()[:8]}"
    progress_state = {
        "status": "running",
        "phase": "starting",
        "message": "RPAプロセスを起動しています",
        "progress_percent": 0.5,
        "run_id": run_id,
        "query": "",
        "query_index": 0,
        "total_queries": len(query_list),
        "started_at_epoch": started_at,
        "updated_at_epoch": started_at,
        "ended_at_epoch": 0,
        "daily_limit_reached": False,
        "reason": "",
        "stdout_tail": [],
        "stderr_tail": [],
    }
    _save_rpa_progress(progress_state)

    lock = threading.Lock()
    stdout_tail: List[str] = []
    stderr_tail: List[str] = []
    progress_from_child: Dict[str, Any] = {}
    daily_limit_reached_stream = False

    def _save_progress_locked() -> None:
        merged = dict(progress_state)
        merged["stdout_tail"] = list(stdout_tail)
        merged["stderr_tail"] = list(stderr_tail)
        _save_rpa_progress(merged)

    def _ingest_progress_payload(payload: Dict[str, Any]) -> None:
        phase = str(payload.get("phase", "") or "").strip() or "running"
        message = str(payload.get("message", "") or "").strip() or "RPA実行中"
        progress_percent = _to_float(payload.get("progress_percent"), _to_float(progress_state.get("progress_percent"), 0.0))
        progress_state["phase"] = phase
        progress_state["message"] = message
        progress_state["progress_percent"] = progress_percent
        progress_state["query"] = str(payload.get("query", "") or "").strip()
        progress_state["query_index"] = max(0, _to_int(payload.get("query_index"), _to_int(progress_state.get("query_index"), 0)))
        progress_state["total_queries"] = max(0, _to_int(payload.get("total_queries"), _to_int(progress_state.get("total_queries"), len(query_list))))
        progress_state["updated_at_epoch"] = int(time.time())
        progress_from_child.clear()
        progress_from_child.update(payload)

    def _handle_stdout_line(line: str) -> None:
        nonlocal daily_limit_reached_stream
        text = str(line or "").rstrip("\n")
        if not text.strip():
            return
        with lock:
            _tail_append(stdout_tail, text)
            if _contains_rpa_daily_limit_message(text):
                daily_limit_reached_stream = True
            parsed = _parse_rpa_progress_line(text)
            if parsed:
                _ingest_progress_payload(parsed)
            _save_progress_locked()

    def _handle_stderr_line(line: str) -> None:
        nonlocal daily_limit_reached_stream
        text = str(line or "").rstrip("\n")
        if not text.strip():
            return
        with lock:
            _tail_append(stderr_tail, text)
            if _contains_rpa_daily_limit_message(text):
                daily_limit_reached_stream = True
            progress_state["updated_at_epoch"] = int(time.time())
            _save_progress_locked()

    def _read_stream(stream: Any, handler: Callable[[str], None]) -> None:
        try:
            for row in iter(stream.readline, ""):
                if row == "":
                    break
                handler(row)
        except Exception:
            return
        finally:
            try:
                stream.close()
            except Exception:
                pass

    timeout_sec = max(15, _env_int("LIQUIDITY_RPA_FETCH_TIMEOUT_SECONDS", 95))
    proc: Optional[subprocess.Popen[str]] = None
    timed_out = False
    return_code = -1
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        if proc.stdout is None or proc.stderr is None:
            raise RuntimeError("failed to capture RPA process streams")
        out_thread = threading.Thread(target=_read_stream, args=(proc.stdout, _handle_stdout_line), daemon=True)
        err_thread = threading.Thread(target=_read_stream, args=(proc.stderr, _handle_stderr_line), daemon=True)
        out_thread.start()
        err_thread.start()
        try:
            return_code = int(proc.wait(timeout=timeout_sec))
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                proc.kill()
            except Exception:
                pass
            try:
                return_code = int(proc.wait(timeout=5))
            except Exception:
                return_code = -1
        out_thread.join(timeout=2.0)
        err_thread.join(timeout=2.0)
    except Exception as err:
        ended_at = int(time.time())
        with lock:
            progress_state["status"] = "failed"
            progress_state["phase"] = "error"
            progress_state["message"] = f"RPA起動に失敗しました: {err}"
            progress_state["progress_percent"] = 100.0
            progress_state["reason"] = "collector_invoke_error"
            progress_state["updated_at_epoch"] = ended_at
            progress_state["ended_at_epoch"] = ended_at
            _save_progress_locked()
        return {
            "enabled": True,
            "ran": False,
            "reason": "collector_invoke_error",
            "queries": query_list,
            "query_count": len(query_list),
            "output_path": str(output_path),
            "python_exec": str(cmd[0]),
            "error": str(err),
        }

    ended_at = int(time.time())
    stdout_text = "\n".join(stdout_tail)
    stderr_text = "\n".join(stderr_tail)
    daily_limit_reached = (
        daily_limit_reached_stream
        or int(return_code) == _RPA_DAILY_LIMIT_EXIT_CODE
        or _contains_rpa_daily_limit_message(stdout_text)
        or _contains_rpa_daily_limit_message(stderr_text)
    )

    if timed_out:
        reason = "daily_limit_reached" if daily_limit_reached else "collector_timeout"
        with lock:
            progress_state["status"] = "stopped" if daily_limit_reached else "failed"
            progress_state["phase"] = "daily_limit_reached" if daily_limit_reached else "timeout"
            progress_state["message"] = (
                "Product Researchの上限到達で停止しました"
                if daily_limit_reached
                else "RPA実行がタイムアウトしました"
            )
            progress_state["progress_percent"] = 100.0
            progress_state["reason"] = reason
            progress_state["daily_limit_reached"] = bool(daily_limit_reached)
            progress_state["updated_at_epoch"] = ended_at
            progress_state["ended_at_epoch"] = ended_at
            _save_progress_locked()
        ended_at = int(time.time())
        return {
            "enabled": True,
            "ran": False,
            "reason": reason,
            "daily_limit_reached": bool(daily_limit_reached),
            "queries": query_list,
            "query_count": len(query_list),
            "output_path": str(output_path),
            "python_exec": str(cmd[0]),
            "timeout_sec": int(timeout_sec),
            "started_at_epoch": started_at,
            "ended_at_epoch": ended_at,
            "stdout_tail": stdout_tail[-18:],
            "stderr_tail": stderr_tail[-18:],
        }

    reason = "daily_limit_reached" if daily_limit_reached else ("ok" if int(return_code) == 0 else "collector_failed")
    with lock:
        progress_state["status"] = "stopped" if daily_limit_reached else ("completed" if int(return_code) == 0 else "failed")
        progress_state["phase"] = "completed" if int(return_code) == 0 else ("daily_limit_reached" if daily_limit_reached else "failed")
        progress_state["message"] = (
            "Product Research取得が完了しました"
            if int(return_code) == 0
            else ("Product Researchの上限到達で停止しました" if daily_limit_reached else "RPA実行が失敗しました")
        )
        progress_state["progress_percent"] = 100.0
        progress_state["reason"] = reason
        progress_state["daily_limit_reached"] = bool(daily_limit_reached)
        progress_state["updated_at_epoch"] = ended_at
        progress_state["ended_at_epoch"] = ended_at
        if progress_from_child:
            progress_state["query"] = str(progress_from_child.get("query", progress_state.get("query", "")) or "")
            progress_state["query_index"] = max(0, _to_int(progress_from_child.get("query_index"), _to_int(progress_state.get("query_index"), 0)))
            progress_state["total_queries"] = max(0, _to_int(progress_from_child.get("total_queries"), _to_int(progress_state.get("total_queries"), len(query_list))))
        _save_progress_locked()

    return {
        "enabled": True,
        "ran": True,
        "reason": reason,
        "daily_limit_reached": bool(daily_limit_reached),
        "returncode": int(return_code),
        "queries": query_list,
        "query_count": len(query_list),
        "output_path": str(output_path),
        "python_exec": str(cmd[0]),
        "started_at_epoch": started_at,
        "ended_at_epoch": ended_at,
        "stdout_tail": stdout_tail[-18:],
        "stderr_tail": stderr_tail[-18:],
        "run_id": run_id,
    }


def _to_epoch_from_iso(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _iter_rpa_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(raw)
        except Exception:
            return []
        if isinstance(payload, dict):
            payload = [payload]
        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, dict):
                    out.append(row)
        return out
    for line in raw.splitlines():
        row_text = str(line or "").strip()
        if not row_text:
            continue
        try:
            row = json.loads(row_text)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def _load_recent_rpa_queries(path: Path, *, max_age_sec: int) -> set[str]:
    now_ts = int(time.time())
    max_age = max(1, int(max_age_sec))
    rows = _iter_rpa_rows(path)
    out: set[str] = set()
    require_sold_sample = _env_bool("LIQUIDITY_RPA_FETCH_REQUIRE_SOLD_SAMPLE_FOR_FRESH", False)
    require_sold_count_known = _env_bool("LIQUIDITY_RPA_FETCH_REQUIRE_SOLD_COUNT_FOR_FRESH", True)
    for row in rows:
        query = re.sub(r"\s+", " ", str(row.get("query", "") or "").strip()).lower()
        if not query:
            continue
        fetched_ts = _to_epoch_from_iso(row.get("fetched_at"))
        if fetched_ts <= 0:
            continue
        if (now_ts - fetched_ts) > max_age:
            continue
        if require_sold_count_known and _to_int(row.get("sold_90d_count"), -1) < 0:
            continue
        if require_sold_sample:
            meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            sample = meta.get("sold_sample") if isinstance(meta.get("sold_sample"), dict) else {}
            has_sample = bool(
                str(sample.get("item_url", "") or "").strip()
                or str(sample.get("image_url", "") or "").strip()
                or str(sample.get("title", "") or "").strip()
            )
            if not has_sample:
                continue
        out.add(query)
    return out


def _maybe_refresh_rpa_for_fetch(queries: Sequence[str], *, force: bool = False) -> Dict[str, Any]:
    mode = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    enabled = (
        mode in {"rpa", "rpa_json"}
        and _env_bool("LIQUIDITY_RPA_AUTO_REFRESH", True)
        and _env_bool("LIQUIDITY_RPA_RUN_ON_FETCH", True)
    )
    normalized = _normalize_rpa_queries(queries)
    if not enabled:
        _save_rpa_progress(
            {
                "status": "idle",
                "phase": "skipped",
                "message": "RPA自動更新は無効です",
                "progress_percent": 0.0,
                "reason": "disabled_or_non_rpa_mode",
                "updated_at_epoch": int(time.time()),
            }
        )
        return {
            "enabled": False,
            "mode": mode,
            "ran": False,
            "reason": "disabled_or_non_rpa_mode",
            "queries": normalized,
        }
    if not normalized:
        _save_rpa_progress(
            {
                "status": "idle",
                "phase": "skipped",
                "message": "RPA実行対象クエリがありません",
                "progress_percent": 0.0,
                "reason": "empty_queries",
                "updated_at_epoch": int(time.time()),
            }
        )
        return {"enabled": True, "mode": mode, "ran": False, "reason": "empty_queries", "queries": []}

    cooldown_sec = max(0, _env_int("LIQUIDITY_RPA_FETCH_MIN_INTERVAL_SECONDS", 300))
    key_raw = "|".join(sorted(q.lower() for q in normalized))
    key = hashlib.sha1(key_raw.encode("utf-8")).hexdigest()
    state = _load_json_file(_RPA_FETCH_STATE_PATH)
    now_ts = int(time.time())
    row = state.get(key) if isinstance(state.get(key), dict) else {}
    last_run_at = _to_int((row or {}).get("last_run_at"), 0)
    last_signal_miss_override_at = _to_int((row or {}).get("last_signal_miss_override_at"), 0)
    signal_miss_min_interval_sec = max(
        0, _env_int("LIQUIDITY_RPA_FETCH_FORCE_ON_SIGNAL_MISS_MIN_INTERVAL_SECONDS", 1800)
    )
    override_missing_queries: List[str] = []
    override_blocked_by_backoff = False
    if (not force) and cooldown_sec > 0 and last_run_at > 0 and (now_ts - last_run_at) < cooldown_sec:
        allow_override = _env_bool("LIQUIDITY_RPA_FETCH_FORCE_ON_SIGNAL_MISS", True)
        if allow_override:
            output_path = _resolve_rpa_output_path()
            max_age = max(60, _env_int("LIQUIDITY_RPA_MAX_AGE_SECONDS", 259200))
            recent_queries = _load_recent_rpa_queries(output_path, max_age_sec=max_age)
            override_missing_queries = [query for query in normalized if query.lower() not in recent_queries]
            if override_missing_queries and signal_miss_min_interval_sec > 0 and last_signal_miss_override_at > 0:
                elapsed_from_override = max(0, now_ts - last_signal_miss_override_at)
                if elapsed_from_override < signal_miss_min_interval_sec:
                    override_missing_queries = []
                    override_blocked_by_backoff = True
        if not override_missing_queries:
            last_reason = str((row or {}).get("reason", "") or "").strip().lower()
            if last_reason == "daily_limit_reached":
                _save_rpa_progress(
                    {
                        "status": "stopped",
                        "phase": "daily_limit_reached",
                        "message": "Product Researchの上限到達中のため再実行を停止しました",
                        "progress_percent": 100.0,
                        "reason": "daily_limit_reached",
                        "daily_limit_reached": True,
                        "updated_at_epoch": int(time.time()),
                        "ended_at_epoch": int(time.time()),
                        "total_queries": len(normalized),
                    }
                )
                return {
                    "enabled": True,
                    "mode": mode,
                    "ran": False,
                    "reason": "daily_limit_reached",
                    "daily_limit_reached": True,
                    "queries": normalized,
                    "cooldown_sec": cooldown_sec,
                    "retry_after_sec": max(0, cooldown_sec - (now_ts - last_run_at)),
                }
            if override_blocked_by_backoff:
                _save_rpa_progress(
                    {
                        "status": "idle",
                        "phase": "cooldown_skip",
                        "message": "signal_missing再実行は待機中のためスキップしました",
                        "progress_percent": 0.0,
                        "reason": "cooldown_skip_signal_miss_backoff",
                        "updated_at_epoch": int(time.time()),
                        "total_queries": len(normalized),
                    }
                )
                return {
                    "enabled": True,
                    "mode": mode,
                    "ran": False,
                    "reason": "cooldown_skip_signal_miss_backoff",
                    "queries": normalized,
                    "cooldown_sec": cooldown_sec,
                    "signal_miss_min_interval_sec": signal_miss_min_interval_sec,
                    "next_signal_miss_retry_sec": max(
                        0, signal_miss_min_interval_sec - max(0, now_ts - last_signal_miss_override_at)
                    ),
                    "retry_after_sec": max(0, cooldown_sec - (now_ts - last_run_at)),
                }
            _save_rpa_progress(
                {
                    "status": "idle",
                    "phase": "cooldown_skip",
                    "message": "クールダウン中のためRPAをスキップしました",
                    "progress_percent": 0.0,
                    "reason": "cooldown_skip",
                    "updated_at_epoch": int(time.time()),
                    "total_queries": len(normalized),
                }
            )
            return {
                "enabled": True,
                "mode": mode,
                "ran": False,
                "reason": "cooldown_skip",
                "queries": normalized,
                "cooldown_sec": cooldown_sec,
                "retry_after_sec": max(0, cooldown_sec - (now_ts - last_run_at)),
            }

    result = _run_rpa_collect_for_fetch(normalized)
    state_row: Dict[str, Any] = {
        "last_run_at": now_ts,
        "queries": normalized,
        "returncode": _to_int(result.get("returncode"), 0),
        "reason": str(result.get("reason", "") or ""),
    }
    if override_missing_queries:
        state_row["last_signal_miss_override_at"] = now_ts
    elif last_signal_miss_override_at > 0:
        state_row["last_signal_miss_override_at"] = last_signal_miss_override_at
    state[key] = state_row
    _save_json_file(_RPA_FETCH_STATE_PATH, state)
    result["mode"] = mode
    result["cooldown_sec"] = cooldown_sec
    if override_missing_queries:
        result["cooldown_override_reason"] = "signal_missing"
        result["missing_queries"] = override_missing_queries
    return result


def _api_cache_path(site: str, method: str, url: str) -> Path:
    site_key = re.sub(r"[^a-z0-9_-]+", "_", str(site or "generic").strip().lower())
    hash_key = hashlib.sha1(f"{method.upper()}|{url}".encode("utf-8")).hexdigest()
    return _API_CACHE_DIR / site_key / f"{hash_key}.json"


def _header_read(headers: Dict[str, str], keys: Sequence[str], default: str = "") -> str:
    if not isinstance(headers, dict):
        return default
    for key in keys:
        value = str(headers.get(key, "") or "").strip()
        if value:
            return value
    return default


def _emit_legacy_internal_headers() -> bool:
    return _env_bool("INTERNAL_EMIT_LEGACY_EBAYMINER_HEADERS", False)


def _set_internal_response_headers(
    headers: Dict[str, str],
    *,
    cache_hit: str,
    cache_age_sec: str,
    budget_remaining: str,
    error: str = "",
) -> Dict[str, str]:
    out = dict(headers or {})
    out[_HEADER_CACHE_HIT_KEY] = str(cache_hit)
    out[_HEADER_CACHE_AGE_SEC_KEY] = str(cache_age_sec)
    out[_HEADER_BUDGET_REMAINING_KEY] = str(budget_remaining)
    if error:
        out[_HEADER_ERROR_KEY] = str(error)

    if _emit_legacy_internal_headers():
        out[_HEADER_LEGACY_CACHE_HIT_KEY] = str(cache_hit)
        out[_HEADER_LEGACY_CACHE_AGE_SEC_KEY] = str(cache_age_sec)
        out[_HEADER_LEGACY_BUDGET_REMAINING_KEY] = str(budget_remaining)
        if error:
            out[_HEADER_LEGACY_ERROR_KEY] = str(error)
    return out


def _make_internal_error_headers(*, error: str, budget_remaining: str) -> Dict[str, str]:
    return _set_internal_response_headers(
        {},
        cache_hit="0",
        cache_age_sec="-1",
        budget_remaining=budget_remaining,
        error=error,
    )


def _header_cache_hit(headers: Dict[str, str]) -> bool:
    return _header_read(headers, _HEADER_CACHE_HIT_KEYS, "0").lower() in {"1", "true", "yes"}


def _header_cache_age_sec(headers: Dict[str, str]) -> int:
    return _to_int(_header_read(headers, _HEADER_CACHE_AGE_SEC_KEYS, "-1"), -1)


def _header_budget_remaining(headers: Dict[str, str]) -> int:
    return _to_int(_header_read(headers, _HEADER_BUDGET_REMAINING_KEYS, "-1"), -1)


def _load_cached_api_response(
    *,
    site: str,
    method: str,
    url: str,
    ttl_sec: int,
) -> Optional[Tuple[int, Dict[str, str], Dict[str, Any], int]]:
    if ttl_sec <= 0:
        return None
    path = _api_cache_path(site, method, url)
    payload = _load_json_file(path)
    if not payload:
        return None
    fetched_at = _to_int(payload.get("fetched_at"), 0)
    if fetched_at <= 0:
        return None
    age_sec = max(0, int(time.time()) - fetched_at)
    if age_sec > ttl_sec:
        return None
    status = _to_int(payload.get("status"), 0)
    headers = payload.get("headers", {})
    body = payload.get("payload", {})
    if status <= 0 or not isinstance(headers, dict) or not isinstance(body, dict):
        return None
    out_headers = {str(k): str(v) for k, v in headers.items()}
    out_headers = _set_internal_response_headers(
        out_headers,
        cache_hit="1",
        cache_age_sec=str(age_sec),
        budget_remaining="-1",
    )
    return status, out_headers, body, age_sec


def _save_cached_api_response(
    *,
    site: str,
    method: str,
    url: str,
    status: int,
    headers: Dict[str, str],
    payload: Dict[str, Any],
) -> None:
    if status != 200:
        return
    if method.upper() != "GET":
        return
    if not isinstance(payload, dict):
        return
    path = _api_cache_path(site, method, url)
    row = {
        "site": str(site or "").strip().lower(),
        "method": method.upper(),
        "url": url,
        "status": int(status),
        "headers": {str(k): str(v) for k, v in (headers or {}).items()},
        "payload": payload,
        "fetched_at": int(time.time()),
    }
    _save_json_file(path, row)


def _consume_daily_budget(site: str) -> Tuple[bool, int]:
    site_key = str(site or "").strip().lower()
    if not site_key:
        return True, -1
    budget = max(0, _env_int(f"MINER_FETCH_DAILY_CALL_BUDGET_{site_key.upper()}", 0))
    if budget <= 0:
        return True, -1
    payload = _load_json_file(_API_USAGE_PATH)
    days = payload.get("days", {})
    if not isinstance(days, dict):
        days = {}
    today = time.strftime("%Y-%m-%d", time.gmtime())
    day_row = days.get(today, {})
    if not isinstance(day_row, dict):
        day_row = {}
    used = _to_int(day_row.get(site_key), 0)
    if used >= budget:
        return False, 0
    day_row[site_key] = used + 1
    days[today] = day_row
    valid_dates = sorted(
        [str(key) for key in days.keys() if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(key))]
    )
    keep_dates = set(valid_dates[-45:])
    for key in list(days.keys()):
        if str(key) not in keep_dates:
            days.pop(key, None)
    payload["days"] = days
    payload["updated_at"] = int(time.time())
    _save_json_file(_API_USAGE_PATH, payload)
    return True, max(0, budget - (used + 1))


def _title_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())[:220]


def _pair_signature(source_title: str, market_title: str) -> str:
    src = _title_key(source_title)
    mkt = _title_key(market_title)
    raw = f"{src}|{mkt}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def _load_blocked_pair_signatures() -> set[str]:
    if not _BLOCKLIST_PATH.exists():
        return set()
    try:
        payload = json.loads(_BLOCKLIST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set()
    rows = payload.get("blocked_pairs", []) if isinstance(payload, dict) else []
    out: set[str] = set()
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        sig = str(row.get("signature", "") or "").strip()
        if sig:
            out.add(sig)
    return out


def _load_fetch_cursor_entries() -> Dict[str, Dict[str, Any]]:
    if not _FETCH_CURSOR_PATH.exists():
        return {}
    try:
        payload = json.loads(_FETCH_CURSOR_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    entries = payload.get("entries", {})
    return entries if isinstance(entries, dict) else {}


def _save_fetch_cursor_entries(entries: Dict[str, Dict[str, Any]]) -> None:
    _FETCH_CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"entries": entries}
    _FETCH_CURSOR_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_cursor_key(*, site: str, query: str) -> str:
    normalized_query = re.sub(r"\s+", " ", str(query or "").strip().lower())
    normalized_site = str(site or "").strip().lower()
    return f"{normalized_site}:{normalized_query}"


def _query_skip_key(
    *,
    query: str,
    market_site: str,
    source_sites: Sequence[str],
    limit_per_site: int,
    max_candidates: int,
    min_match_score: float,
    min_profit_usd: float,
    min_margin_rate: float,
    require_in_stock: bool,
) -> str:
    normalized_query = re.sub(r"\s+", " ", str(query or "").strip().lower())
    src = ",".join(sorted({str(v or "").strip().lower() for v in source_sites if str(v or "").strip()}))
    return (
        f"{market_site}|{src}|{normalized_query}|limit={int(limit_per_site)}|max={int(max_candidates)}|"
        f"score={float(min_match_score):.4f}|profit={float(min_profit_usd):.4f}|margin={float(min_margin_rate):.4f}|"
        f"in_stock={1 if bool(require_in_stock) else 0}"
    )


def _iter_fetched_site_infos(fetched: Any) -> List[Tuple[str, Dict[str, Any]]]:
    if not isinstance(fetched, dict):
        return []
    out: List[Tuple[str, Dict[str, Any]]] = []
    for site, info in fetched.items():
        if not isinstance(info, dict):
            continue
        # source_budget_filter 等の集計データは除外し、実サイトの結果のみ判定対象にする。
        if str(site or "").strip().lower() not in {"ebay", "rakuten", "yahoo"} and "stop_reason" not in info:
            continue
        out.append((str(site), info))
    return out


def _is_site_scope_done(info: Dict[str, Any]) -> bool:
    if not isinstance(info, dict):
        return False
    if not bool(info.get("ok")):
        return False
    reason = str(info.get("stop_reason", "") or "").strip().lower()
    calls_made = _to_int(info.get("calls_made"), 0)
    if reason == "query_exhausted":
        return calls_made > 0
    if reason == "skipped_by_sold_first_preselection":
        return True
    return False


def _is_short_term_no_gain_result(result: Dict[str, Any]) -> bool:
    if not isinstance(result, dict):
        return False
    if _to_int(result.get("created_count"), -1) != 0:
        return False
    fetched = result.get("fetched")
    site_infos = _iter_fetched_site_infos(fetched)
    if not site_infos:
        return False
    no_gain_reasons = {
        "low_yield_stop",
        "max_calls_reached",
        "target_reached",
        "query_exhausted",
        "search_scope_done_no_gain",
        "skipped_by_sold_first_preselection",
        "skipped_no_market_hits",
    }
    has_site_reason = False
    for _site, info in site_infos:
        if not bool(info.get("ok")):
            return False
        reason = str(info.get("stop_reason", "") or "").strip().lower()
        if not reason:
            continue
        has_site_reason = True
        if reason not in no_gain_reasons:
            return False
    return has_site_reason


def _epoch_to_iso(ts: int) -> str:
    if ts <= 0:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _load_fetch_tuner_entries() -> Dict[str, Dict[str, Any]]:
    payload = _load_json_file(_FETCH_TUNER_PATH)
    rows = payload.get("entries", {}) if isinstance(payload, dict) else {}
    return rows if isinstance(rows, dict) else {}


def _save_fetch_tuner_entries(entries: Dict[str, Dict[str, Any]]) -> None:
    _save_json_file(_FETCH_TUNER_PATH, {"entries": entries})


def _load_query_skip_entries() -> Dict[str, Dict[str, Any]]:
    payload = _load_json_file(_QUERY_SKIP_PATH)
    rows = payload.get("entries", {}) if isinstance(payload, dict) else {}
    return rows if isinstance(rows, dict) else {}


def _save_query_skip_entries(entries: Dict[str, Dict[str, Any]]) -> None:
    _save_json_file(_QUERY_SKIP_PATH, {"entries": entries})


def _first_env(*keys: str) -> str:
    for key in keys:
        val = (os.getenv(key, "") or "").strip()
        if val:
            return val
    return ""


def _request_json(
    url: str,
    *,
    method: str = "GET",
    data: bytes | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 18,
    site: str = "",
) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    normalized_site = str(site or "").strip().lower()
    use_cache = (
        method.upper() == "GET"
        and bool(normalized_site)
        and _env_bool("MINER_FETCH_CACHE_ENABLED", True)
    )
    cache_ttl_sec = max(0, _env_int("MINER_FETCH_CACHE_TTL_SECONDS", 21600))
    cache_only = _env_bool("MINER_FETCH_CACHE_ONLY", False)
    if use_cache:
        cached = _load_cached_api_response(
            site=normalized_site,
            method=method,
            url=url,
            ttl_sec=cache_ttl_sec,
        )
        if cached is not None:
            status, cached_headers, cached_payload, _age = cached
            return status, cached_headers, cached_payload
        if cache_only:
            return 0, _make_internal_error_headers(error="cache_miss", budget_remaining="-1"), {"error": "cache_miss"}

    budget_remaining = -1
    if normalized_site:
        can_call, budget_remaining = _consume_daily_budget(normalized_site)
        if not can_call:
            return 0, _make_internal_error_headers(error="daily_budget_exhausted", budget_remaining="0"), {
                "error": "daily_budget_exhausted"
            }

    req = urllib.request.Request(url=url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw) if raw else {}
            if not isinstance(payload, dict):
                payload = {"data": payload}
            out_headers = dict(resp.headers.items())
            out_headers = _set_internal_response_headers(
                out_headers,
                cache_hit="0",
                cache_age_sec="-1",
                budget_remaining=str(budget_remaining),
            )
            if use_cache:
                _save_cached_api_response(
                    site=normalized_site,
                    method=method,
                    url=url,
                    status=int(resp.status),
                    headers=out_headers,
                    payload=payload,
                )
            return int(resp.status), out_headers, payload
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        payload: Dict[str, Any]
        try:
            parsed = json.loads(body) if body else {}
            payload = parsed if isinstance(parsed, dict) else {"data": parsed}
        except json.JSONDecodeError:
            payload = {"raw": body[:600]}
        out_headers = dict(err.headers.items())
        out_headers = _set_internal_response_headers(
            out_headers,
            cache_hit="0",
            cache_age_sec="-1",
            budget_remaining=str(budget_remaining),
        )
        return int(err.code), out_headers, payload
    except urllib.error.URLError as err:
        return 0, _set_internal_response_headers(
            {},
            cache_hit="0",
            cache_age_sec="-1",
            budget_remaining=str(budget_remaining),
        ), {"error": str(err)}


def _request_text(
    url: str,
    *,
    method: str = "GET",
    data: bytes | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 18,
) -> Tuple[int, Dict[str, str], str]:
    req = urllib.request.Request(url=url, data=data, method=method)
    base_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }
    for key, value in {**base_headers, **(headers or {})}.items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return int(resp.status), dict(resp.headers.items()), raw
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        return int(err.code), dict(err.headers.items()), body
    except urllib.error.URLError:
        return 0, {}, ""


def _request_with_retry(
    url: str,
    *,
    method: str = "GET",
    data: bytes | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 18,
    retries: int = 1,
    site: str = "",
) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    status, res_headers, payload = _request_json(
        url,
        method=method,
        data=data,
        headers=headers,
        timeout=timeout,
        site=site,
    )
    if status != 429 or retries <= 0:
        return status, res_headers, payload

    retry_after = (res_headers.get("Retry-After", "") or "").strip()
    sleep_sec = 2.0
    try:
        sleep_sec = max(0.5, float(retry_after))
    except ValueError:
        sleep_sec = 2.0
    time.sleep(sleep_sec)
    return _request_with_retry(
        url,
        method=method,
        data=data,
        headers=headers,
        timeout=timeout,
        retries=retries - 1,
        site=site,
    )


def _extract_image_url(value: Any) -> str:
    def _normalize_candidate(raw: Any) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        if text.startswith("//"):
            text = f"https:{text}"
        elif text.startswith("/"):
            text = urllib.parse.urljoin("https://www.ebay.com", text)
        if not text.startswith("http"):
            return ""
        return text

    def _from_srcset(raw: Any) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        for chunk in text.split(","):
            url = chunk.strip().split(" ", 1)[0].strip()
            candidate = _normalize_candidate(url)
            if candidate:
                return candidate
        return ""

    def _looks_like_image_url(url: str) -> bool:
        lower = str(url or "").lower()
        if not lower:
            return False
        if any(ext in lower for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif")):
            return True
        if any(host in lower for host in ("ebayimg.com", "rakuten.co.jp", "yimg.jp", "yahoo.co.jp")):
            return True
        return "/images/" in lower or "/image/" in lower

    def _walk(node: Any, depth: int, seen: set[int]) -> str:
        if depth > 5:
            return ""
        ident = id(node)
        if ident in seen:
            return ""
        seen.add(ident)

        if isinstance(node, str):
            direct = _normalize_candidate(node)
            if direct and _looks_like_image_url(direct):
                return direct
            from_srcset = _from_srcset(node)
            if from_srcset and _looks_like_image_url(from_srcset):
                return from_srcset
            return ""

        if isinstance(node, dict):
            for key in (
                "imageUrl",
                "data-src",
                "dataSrc",
                "data-original",
                "dataOriginal",
                "src",
                "srcSet",
                "srcset",
                "thumbnail",
                "thumbnailUrl",
                "small",
                "medium",
                "large",
                "url",
            ):
                if key not in node:
                    continue
                raw = node.get(key)
                candidate = _from_srcset(raw) if key.lower() == "srcset" else _normalize_candidate(raw)
                if candidate and _looks_like_image_url(candidate):
                    return candidate
            for key, raw in node.items():
                key_text = str(key or "").lower()
                if any(tag in key_text for tag in ("image", "img", "thumb", "photo", "picture")):
                    candidate = _walk(raw, depth + 1, seen)
                    if candidate:
                        return candidate
            for raw in node.values():
                candidate = _walk(raw, depth + 1, seen)
                if candidate:
                    return candidate
            return ""

        if isinstance(node, list):
            for raw in node[:12]:
                candidate = _walk(raw, depth + 1, seen)
                if candidate:
                    return candidate
            return ""
        return ""

    return _walk(value, 0, set())


def _ebay_fetch_item_image(
    *,
    token: str,
    marketplace: str,
    item_id: str,
    timeout: int,
) -> str:
    item = str(item_id or "").strip()
    if not item:
        return ""
    encoded_item = urllib.parse.quote(item, safe="")
    status, _, payload = _request_with_retry(
        f"https://api.ebay.com/buy/browse/v1/item/{encoded_item}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": marketplace,
        },
        timeout=timeout,
        site="ebay",
    )
    if status != 200 or not isinstance(payload, dict):
        return ""
    return (
        _extract_image_url(payload.get("image"))
        or _extract_image_url(payload.get("additionalImages"))
        or _extract_image_url(payload.get("thumbnailImages"))
        or _extract_image_url(payload)
    )


def _ebay_item_id_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    m = re.search(r"/itm/(?:[^/?#]+/)?([0-9]{9,15})", raw)
    if not m:
        return ""
    numeric_id = str(m.group(1) or "").strip()
    if not numeric_id:
        return ""
    return f"v1|{numeric_id}|0"


def backfill_candidate_market_images(
    candidates: Sequence[Dict[str, Any]],
    *,
    timeout: int = 8,
    max_calls: int = 8,
) -> int:
    rows = [row for row in (candidates or []) if isinstance(row, dict)]
    if not rows:
        return 0
    allowed_calls = max(0, int(max_calls))
    if allowed_calls <= 0:
        return 0
    try:
        token = _ebay_access_token(timeout)
    except Exception:
        return 0
    marketplace = (_first_env("TARGET_MARKETPLACE") or "EBAY_US").strip() or "EBAY_US"
    updated = 0
    calls = 0
    for candidate in rows:
        if calls >= allowed_calls:
            break
        market_site = str(candidate.get("market_site", "") or "").strip().lower()
        if market_site != "ebay":
            continue
        metadata = candidate.get("metadata")
        if not isinstance(metadata, dict):
            continue
        existing_image = (
            str(metadata.get("ebay_sold_image_url", "") or "").strip()
            or str(metadata.get("market_image_url", "") or "").strip()
            or str(metadata.get("market_image_url_active", "") or "").strip()
        )
        if existing_image:
            continue
        item_id = str(candidate.get("market_item_id", "") or "").strip()
        if not item_id:
            item_id = _ebay_item_id_from_url(str(metadata.get("market_item_url", "") or ""))
        if not item_id:
            continue
        calls += 1
        image_url = _ebay_fetch_item_image(
            token=token,
            marketplace=marketplace,
            item_id=item_id,
            timeout=timeout,
        )
        if not image_url:
            continue
        metadata["market_image_url"] = image_url
        metadata.setdefault("market_image_url_active", image_url)
        candidate["metadata"] = metadata
        updated += 1
    return updated


def _ebay_access_token(timeout: int) -> str:
    now = time.time()
    cached = _EBAY_TOKEN_CACHE.get("token")
    if isinstance(cached, str) and cached and now < float(_EBAY_TOKEN_CACHE.get("expires_at", 0)) - 30:
        return cached

    client_id = _first_env("EBAY_CLIENT_ID")
    client_secret = _first_env("EBAY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise ValueError("EBAY_CLIENT_ID / EBAY_CLIENT_SECRET が未設定です")

    scope = "https://api.ebay.com/oauth/api_scope"
    body = urllib.parse.urlencode({"grant_type": "client_credentials", "scope": scope}).encode("utf-8")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    status, _, payload = _request_with_retry(
        "https://api.ebay.com/identity/v1/oauth2/token",
        method="POST",
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=timeout,
    )
    if status != 200:
        raise ValueError(f"eBayトークン取得失敗: http={status}")

    token = str(payload.get("access_token", "") or "")
    if not token:
        raise ValueError("eBayトークン取得失敗: access_token が空です")
    expires_in = int(payload.get("expires_in", 7200) or 7200)
    _EBAY_TOKEN_CACHE["token"] = token
    _EBAY_TOKEN_CACHE["expires_at"] = now + max(60, expires_in)
    return token


def _search_ebay(
    query: str, limit: int, timeout: int, page: int = 1, require_in_stock: bool = True
) -> Tuple[List[MarketItem], Dict[str, Any]]:
    token = _ebay_access_token(timeout)
    marketplace = (_first_env("TARGET_MARKETPLACE") or "EBAY_US").strip() or "EBAY_US"
    capped_limit = max(1, min(200, limit))
    safe_page = max(1, int(page))
    offset = (safe_page - 1) * capped_limit
    if offset > 9999:
        return [], {"http": 200, "raw_total": 0, "page": safe_page, "offset": offset, "truncated": True}
    params_dict = {
        "q": query,
        "limit": str(capped_limit),
        "filter": "conditions:{NEW}",
    }
    if offset > 0:
        params_dict["offset"] = str(offset)
    params = urllib.parse.urlencode(params_dict)
    image_fallback_enabled = _env_bool("EBAY_IMAGE_FALLBACK_ITEM_API", True)
    image_fallback_max_calls = max(0, min(40, _env_int("EBAY_IMAGE_FALLBACK_MAX_CALLS", 8)))
    image_fallback_calls = 0
    image_fallback_hits = 0
    status, res_headers, payload = _request_with_retry(
        f"https://api.ebay.com/buy/browse/v1/item_summary/search?{params}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": marketplace,
        },
        timeout=timeout,
        site="ebay",
    )
    if status != 200:
        detail = str(payload.get("error", "") or "").strip()
        suffix = f" ({detail})" if detail else ""
        raise ValueError(f"eBay検索失敗: http={status}{suffix}")

    items: List[MarketItem] = []
    for row in payload.get("itemSummaries", []) or []:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title", "") or "").strip()
        condition = str(row.get("condition", "new") or "new")
        item_id = str(row.get("itemId", "") or "")
        price_info = row.get("price") if isinstance(row.get("price"), dict) else {}
        shipping_options = row.get("shippingOptions") if isinstance(row.get("shippingOptions"), list) else []
        shipping_cost = 0.0
        if shipping_options:
            first = shipping_options[0] if isinstance(shipping_options[0], dict) else {}
            shipping_value = first.get("shippingCost") if isinstance(first.get("shippingCost"), dict) else {}
            shipping_cost = _to_float(shipping_value.get("value"), 0.0)
        if not title:
            continue
        if _contains_out_of_stock_marker(title):
            continue
        if _is_accessory_title(title):
            continue
        if not _is_new_listing(title, condition):
            continue
        image_url = (
            _extract_image_url(row.get("image"))
            or _extract_image_url(row.get("additionalImages"))
            or _extract_image_url(row.get("thumbnailImages"))
            or _extract_image_url(row.get("thumbnailImage"))
            or _extract_image_url(row.get("imageUrl"))
        )
        if (
            not image_url
            and image_fallback_enabled
            and image_fallback_calls < image_fallback_max_calls
            and item_id
        ):
            image_fallback_calls += 1
            image_url = _ebay_fetch_item_image(
                token=token,
                marketplace=marketplace,
                item_id=item_id,
                timeout=timeout,
            )
            if image_url:
                image_fallback_hits += 1
        identifiers = _with_title_identifier_hints({}, title)
        item = MarketItem(
            site="ebay",
            item_id=item_id,
            title=title,
            item_url=str(row.get("itemWebUrl", "") or ""),
            image_url=image_url,
            price=_to_float(price_info.get("value"), 0.0),
            shipping=shipping_cost,
            currency=str(price_info.get("currency", "USD") or "USD"),
            condition=condition,
            identifiers=identifiers,
            raw=row,
        )
        if item.price > 0:
            items.append(item)
    return items, {
        "http": status,
        "raw_total": payload.get("total"),
        "page": safe_page,
        "offset": offset,
        "cache_hit": _header_cache_hit(res_headers),
        "cache_age_sec": _header_cache_age_sec(res_headers),
        "budget_remaining": _header_budget_remaining(res_headers),
        "image_fallback_calls": image_fallback_calls,
        "image_fallback_hits": image_fallback_hits,
    }


def _search_rakuten(
    query: str, limit: int, timeout: int, page: int = 1, require_in_stock: bool = True
) -> Tuple[List[MarketItem], Dict[str, Any]]:
    app_id = _first_env("RAKUTEN_APPLICATION_ID")
    if not app_id:
        raise ValueError("RAKUTEN_APPLICATION_ID が未設定です")
    capped_limit = max(1, min(30, limit))
    safe_page = max(1, min(100, int(page)))
    if safe_page != int(page):
        return [], {"http": 200, "raw_count": 0, "page": int(page), "truncated": True}
    params = {
        "applicationId": app_id,
        "keyword": query,
        "hits": str(capped_limit),
        "page": str(safe_page),
        "format": "json",
    }
    # 既定は在庫あり必須。カスタム解除時のみ在庫情報フィルタを外す。
    if require_in_stock:
        params["availability"] = "1"
    access_key = _first_env("RAKUTEN_PUBLIC_KEY")
    if access_key:
        params["accessKey"] = access_key
    affiliate_id = _first_env("RAKUTEN_AFFILIATE_ID")
    if affiliate_id:
        params["affiliateId"] = affiliate_id
    base_url = (
        _first_env("RAKUTEN_API_BASE_URL")
        or "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601"
    )
    status, res_headers, payload = _request_with_retry(
        f"{base_url}?{urllib.parse.urlencode(params)}",
        timeout=timeout,
        site="rakuten",
    )
    if status != 200:
        detail = str(payload.get("error", "") or "").strip()
        suffix = f" ({detail})" if detail else ""
        raise ValueError(f"楽天検索失敗: http={status}{suffix}")

    items: List[MarketItem] = []
    raw_items = payload.get("Items", []) or []
    for row in raw_items:
        item = row.get("Item") if isinstance(row, dict) and isinstance(row.get("Item"), dict) else row
        if not isinstance(item, dict):
            continue
        item_code = str(item.get("itemCode", "") or "").strip()
        shop_code = item_code.split(":", 1)[0].strip().lower() if ":" in item_code else ""
        item_url_text = str(item.get("itemUrl", "") or "").lower()
        # 楽天の auc-* 系ショップは中古/委託比率が高く、API上で新品判定が難しいため除外。
        if shop_code.startswith("auc-") or "/auc-" in item_url_text:
            continue
        availability = item.get("availability")
        if require_in_stock and availability is not None:
            availability_text = str(availability).strip().lower()
            if availability_text not in {"1", "true"}:
                continue
        title = str(item.get("itemName", "") or "").strip()
        if not title:
            continue
        caption = str(item.get("itemCaption", "") or "")
        condition_text = f"{title} {caption}".strip()
        if require_in_stock and _contains_out_of_stock_marker(condition_text):
            continue
        if _is_accessory_title(title):
            continue
        if not _is_new_listing(condition_text):
            continue
        image_url = _extract_image_url(item.get("mediumImageUrls")) or _extract_image_url(
            item.get("smallImageUrls")
        )
        identifiers = _with_title_identifier_hints({}, title)
        code_hint = _extract_primary_model_code(item_code.replace(":", " "))
        if code_hint:
            identifiers.setdefault("model", code_hint)
            identifiers.setdefault("mpn", code_hint)
        market_item = MarketItem(
            site="rakuten",
            item_id=str(item.get("itemCode", "") or ""),
            title=title,
            item_url=str(item.get("itemUrl", "") or ""),
            image_url=image_url,
            price=_to_float(item.get("itemPrice"), 0.0),
            shipping=0.0,
            currency="JPY",
            condition="new",
            identifiers=identifiers,
            raw=item,
        )
        if market_item.price > 0:
            items.append(market_item)
    return items, {
        "http": status,
        "raw_count": payload.get("count"),
        "page": safe_page,
        "cache_hit": _header_cache_hit(res_headers),
        "cache_age_sec": _header_cache_age_sec(res_headers),
        "budget_remaining": _header_budget_remaining(res_headers),
    }


def _search_yahoo(
    query: str, limit: int, timeout: int, page: int = 1, require_in_stock: bool = True
) -> Tuple[List[MarketItem], Dict[str, Any]]:
    app_id = _first_env("YAHOO_APP_ID", "YAHOO_CLIENT_ID")
    if not app_id:
        raise ValueError("YAHOO_APP_ID が未設定です")
    # Yahoo ItemSearch は results 最大100。API側で広めに取り、呼び出し回数を削減。
    capped_limit = max(1, min(100, limit))
    safe_page = max(1, int(page))
    start = 1 + (safe_page - 1) * capped_limit
    if start > 1000:
        return [], {"http": 200, "raw_total": 0, "page": safe_page, "start": start, "truncated": True}
    if start + capped_limit - 1 > 1000:
        capped_limit = max(1, 1000 - start + 1)
    params_dict = {
        "appid": app_id,
        "query": query,
        "results": str(capped_limit),
        "start": str(start),
        "sort": "-score",
        # 新品固定（MVP方針）。
        "condition": "new",
    }
    # 既定は在庫あり必須。カスタム解除時は在庫フィルタを外す。
    if require_in_stock:
        params_dict["in_stock"] = "true"
    params = urllib.parse.urlencode(params_dict)
    status, headers, payload = _request_with_retry(
        f"https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch?{params}",
        timeout=timeout,
        site="yahoo",
    )
    if status != 200:
        auth_error = (headers.get("x-yahooj-autherror") or "").strip()
        parts: List[str] = []
        if auth_error:
            parts.append(f"auth={auth_error}")
        payload_error = str(payload.get("error", "") or "").strip()
        if payload_error:
            parts.append(payload_error)
        suffix = f" ({' / '.join(parts)})" if parts else ""
        raise ValueError(f"Yahoo検索失敗: http={status}{suffix}")

    items: List[MarketItem] = []
    for row in payload.get("hits", []) or []:
        if not isinstance(row, dict):
            continue
        title = str(row.get("name", "") or "").strip()
        if not title:
            continue
        head_line = str(row.get("headLine", "") or "")
        source_condition = str(row.get("condition", "") or "").strip()
        description_text = str(row.get("description", "") or "")
        condition_text = f"{title} {head_line} {description_text[:260]}".strip()
        in_stock = row.get("inStock")
        if require_in_stock and isinstance(in_stock, bool) and not in_stock:
            continue
        if require_in_stock and isinstance(in_stock, (int, float)) and int(in_stock) == 0:
            continue
        if require_in_stock and isinstance(in_stock, str):
            in_stock_norm = in_stock.strip().lower()
            if in_stock_norm in {"false", "0", "no"}:
                continue
        if require_in_stock and _contains_out_of_stock_marker(condition_text):
            continue
        if _is_accessory_title(title):
            continue
        if not _is_new_listing(condition_text, source_condition):
            continue
        price = _to_float(row.get("price"), 0.0)
        if price <= 0:
            price = _to_float((row.get("priceLabel") or {}).get("defaultPrice"), 0.0)
        shipping = 0.0
        shipping_info = row.get("shipping")
        if isinstance(shipping_info, dict):
            shipping = _to_float(shipping_info.get("price"), 0.0)
        identifiers: Dict[str, str] = {}
        jan = str(row.get("janCode", "") or "").strip()
        if jan:
            identifiers["jan"] = jan
        identifiers = _with_title_identifier_hints(identifiers, title)

        item = MarketItem(
            site="yahoo_shopping",
            item_id=str(row.get("code", "") or ""),
            title=title,
            item_url=str(row.get("url", "") or ""),
            image_url=_extract_image_url(row.get("image")),
            price=price,
            shipping=shipping,
            currency="JPY",
            condition=source_condition or "new",
            identifiers=identifiers,
            raw=row,
        )
        if item.price > 0:
            items.append(item)
    return items, {
        "http": status,
        "raw_total": payload.get("totalResultsAvailable"),
        "page": safe_page,
        "start": start,
        "cache_hit": _header_cache_hit(headers),
        "cache_age_sec": _header_cache_age_sec(headers),
        "budget_remaining": _header_budget_remaining(headers),
    }


def _compact_query(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _query_tokens(query: str) -> List[str]:
    normalized = _normalize_for_tokens(query)
    raw = [tok for tok in normalized.split() if len(tok) >= 2]
    out: List[str] = []
    for token in raw:
        if token in _QUERY_NOISE_TERMS:
            continue
        out.append(token)
    return out


def _pick_brand_token(tokens: Sequence[str]) -> str:
    for token in tokens:
        if token in _QUERY_STOPWORDS:
            continue
        if token in _GENERIC_TOKENS:
            continue
        if any(ch.isdigit() for ch in token):
            continue
        if len(token) < 3:
            continue
        return token
    return ""


def _pick_generic_token(tokens: Sequence[str]) -> str:
    for token in tokens:
        if token in _GENERIC_TOKENS:
            return token
    for token in reversed(tokens):
        if any(ch.isdigit() for ch in token):
            continue
        if token in _QUERY_STOPWORDS:
            continue
        if len(token) >= 4:
            return token
    return ""


def _normalize_category_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(text or "")).strip().lower()
    normalized = normalized.replace("_", " ").replace("-", " ").replace("/", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _load_category_knowledge() -> Dict[str, Any]:
    try:
        mtime = _CATEGORY_KNOWLEDGE_PATH.stat().st_mtime
    except OSError:
        _CATEGORY_KNOWLEDGE_CACHE["mtime"] = 0.0
        _CATEGORY_KNOWLEDGE_CACHE["payload"] = {}
        return {}
    cached_mtime = _to_float(_CATEGORY_KNOWLEDGE_CACHE.get("mtime"), 0.0)
    if cached_mtime == mtime:
        payload = _CATEGORY_KNOWLEDGE_CACHE.get("payload")
        return payload if isinstance(payload, dict) else {}
    payload = _load_json_file(_CATEGORY_KNOWLEDGE_PATH)
    _CATEGORY_KNOWLEDGE_CACHE["mtime"] = mtime
    _CATEGORY_KNOWLEDGE_CACHE["payload"] = payload if isinstance(payload, dict) else {}
    return _CATEGORY_KNOWLEDGE_CACHE["payload"]


def _load_known_brand_aliases() -> Dict[str, Tuple[str, ...]]:
    payload = _load_category_knowledge()
    mtime = _to_float(_CATEGORY_KNOWLEDGE_CACHE.get("mtime"), 0.0)
    cached_mtime = _to_float(_CATEGORY_BRAND_CACHE.get("mtime"), -1.0)
    cached = _CATEGORY_BRAND_CACHE.get("brands")
    if cached_mtime == mtime and isinstance(cached, dict):
        return cached

    out: Dict[str, Tuple[str, ...]] = {}
    categories = payload.get("categories", []) if isinstance(payload, dict) else []
    if isinstance(categories, list):
        for row in categories:
            if not isinstance(row, dict):
                continue
            for raw in row.get("seed_brands", []) or []:
                brand = str(raw or "").strip()
                if not brand:
                    continue
                canonical = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", brand)).strip().upper()
                if not canonical:
                    continue
                aliases = set(out.get(canonical, ()))
                aliases.add(canonical)
                aliases.add(brand)
                compact = re.sub(r"\s+", "", canonical)
                if len(compact) >= 3:
                    aliases.add(compact)
                normalized_aliases = tuple(
                    sorted(
                        {
                            re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(v or ""))).strip()
                            for v in aliases
                            if str(v or "").strip()
                        },
                        key=lambda text: len(text),
                        reverse=True,
                    )
                )
                out[canonical] = normalized_aliases

    _CATEGORY_BRAND_CACHE["mtime"] = mtime
    _CATEGORY_BRAND_CACHE["brands"] = out
    return out


def _extract_known_brand_tags(title: str) -> set[str]:
    normalized = unicodedata.normalize("NFKC", str(title or ""))
    upper = normalized.upper()
    brands = _load_known_brand_aliases()
    if not brands:
        return set()
    found: set[str] = set()
    for canonical, aliases in brands.items():
        for alias in aliases:
            token = str(alias or "").strip()
            if not token:
                continue
            if token.isascii():
                if _contains_ascii_token(upper, token):
                    found.add(canonical)
                    break
            else:
                if token in normalized:
                    found.add(canonical)
                    break
    return found


def _category_aliases(category_row: Dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    key = _normalize_category_text(str(category_row.get("category_key", "") or ""))
    if key:
        aliases.add(key)
        aliases.add(key.replace(" ", "_"))
        aliases.add(key.replace(" ", ""))
    display = _normalize_category_text(str(category_row.get("display_name_ja", "") or ""))
    if display:
        aliases.add(display)
        aliases.add(display.replace(" ", ""))
    raw_aliases = _CATEGORY_QUERY_ALIASES.get(key.replace(" ", "_"), ())
    for alias in raw_aliases:
        norm = _normalize_category_text(alias)
        if norm:
            aliases.add(norm)
            aliases.add(norm.replace(" ", ""))
    user_aliases = category_row.get("aliases", [])
    if isinstance(user_aliases, list):
        for alias in user_aliases:
            norm = _normalize_category_text(alias)
            if norm:
                aliases.add(norm)
                aliases.add(norm.replace(" ", ""))
    return aliases


def _looks_like_category_query(query: str) -> bool:
    compact = _compact_query(query)
    if not compact:
        return False
    if _extract_codes(compact):
        return False
    tokenized = _query_tokens(compact)
    brand = _pick_brand_token(tokenized)
    return not bool(brand)


def _match_category_row(query: str) -> Optional[Dict[str, Any]]:
    payload = _load_category_knowledge()
    categories = payload.get("categories", [])
    if not isinstance(categories, list):
        return None
    norm_query = _normalize_category_text(query)
    if not norm_query:
        return None
    compact_query = norm_query.replace(" ", "")
    stripped_tokens = [tok for tok in norm_query.split() if tok not in _CATEGORY_QUERY_NOISE_TOKENS]
    stripped_query = " ".join(stripped_tokens).strip()
    stripped_compact_query = stripped_query.replace(" ", "")
    for row in categories:
        if not isinstance(row, dict):
            continue
        aliases = _category_aliases(row)
        if (
            norm_query in aliases
            or compact_query in aliases
            or (stripped_query and stripped_query in aliases)
            or (stripped_compact_query and stripped_compact_query in aliases)
        ):
            return row
    if not _looks_like_category_query(query):
        return None
    return None


def _active_season_tags(category_row: Dict[str, Any], month: int) -> List[str]:
    tags: List[str] = []
    seasonality = category_row.get("seasonality", [])
    if not isinstance(seasonality, list):
        return tags
    for row in seasonality:
        if not isinstance(row, dict):
            continue
        months = row.get("months", [])
        if not isinstance(months, list):
            continue
        months_set = {int(m) for m in months if isinstance(m, int) or (isinstance(m, str) and str(m).isdigit())}
        if month in months_set:
            tag = str(row.get("tag", "") or "").strip()
            if tag:
                tags.append(tag)
    return tags


def _is_specific_model_example(text: str) -> bool:
    raw = str(text or "").strip()
    if len(raw) < 4:
        return False
    if raw.lower() in _CATEGORY_PLACEHOLDER_MODEL_TERMS:
        return False
    if _extract_codes(raw):
        return True
    return bool(re.search(r"\d", raw))


def _build_category_relevance_terms(category_row: Dict[str, Any]) -> Tuple[str, ...]:
    key = _normalize_category_text(str(category_row.get("category_key", "") or "")).replace(" ", "_")
    noun = _CATEGORY_NOUN_HINT.get(key, key.replace("_", " ")).strip()
    terms: List[str] = []
    for raw in category_row.get("model_examples", []) or []:
        model = str(raw or "").strip()
        if _is_specific_model_example(model):
            terms.append(model)
    for raw in category_row.get("seed_series", []) or []:
        series = str(raw or "").strip()
        if len(series) >= 3:
            terms.append(series)
    for raw in category_row.get("seed_brands", []) or []:
        brand = str(raw or "").strip()
        if len(brand) >= 3:
            terms.append(brand)
    aliases = _CATEGORY_QUERY_ALIASES.get(key, ())
    for raw in aliases:
        alias = str(raw or "").strip()
        if len(alias) >= 3:
            terms.append(alias)
    if noun and len(noun) >= 3:
        terms.append(noun)

    max_terms = max(8, min(80, _env_int("MINER_CATEGORY_RELEVANCE_MAX_TERMS", 32)))
    seen: set[str] = set()
    out: List[str] = []
    for raw in terms:
        term = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(raw or ""))).strip()
        if len(term) < 3:
            continue
        key_norm = term.upper()
        if key_norm in seen:
            continue
        seen.add(key_norm)
        out.append(term)
        if len(out) >= max_terms:
            break
    return tuple(out)


def _title_matches_category_terms(title: str, terms: Sequence[str]) -> bool:
    if not terms:
        return True
    normalized = unicodedata.normalize("NFKC", str(title or ""))
    upper = normalized.upper()
    for raw in terms:
        term = str(raw or "").strip()
        if not term:
            continue
        if term.isascii():
            if _contains_ascii_token(upper, term):
                return True
            continue
        if term in normalized:
            return True
    return False


def _build_category_seed_queries(
    *,
    category_row: Dict[str, Any],
    site: str,
) -> Tuple[List[str], Dict[str, Any]]:
    key = _normalize_category_text(str(category_row.get("category_key", "") or "")).replace(" ", "_")
    display_name = str(category_row.get("display_name_ja", "") or "").strip()
    brands = [str(v).strip() for v in category_row.get("seed_brands", []) if str(v).strip()]
    series = [str(v).strip() for v in category_row.get("seed_series", []) if str(v).strip()]
    models = [str(v).strip() for v in category_row.get("model_examples", []) if _is_specific_model_example(v)]
    month = int(time.gmtime().tm_mon)
    active_tags = _active_season_tags(category_row, month)
    base_depth = max(1, min(6, _env_int("MINER_CATEGORY_KNOWLEDGE_DEPTH", 3)))
    if active_tags:
        base_depth = min(6, base_depth + 1)
    noun = _CATEGORY_NOUN_HINT.get(key, key.replace("_", " "))

    candidates: List[str] = []
    series_depth = min(base_depth, len(series))
    for idx in range(series_depth):
        serie = series[idx]
        if site == "ebay":
            candidates.append(_compact_query(f"{serie} NEW"))
        candidates.append(_compact_query(serie))

    model_depth = min(base_depth, len(models))
    for idx in range(model_depth):
        model = models[idx]
        if site == "ebay":
            candidates.append(_compact_query(f"{model} NEW"))
        candidates.append(_compact_query(model))

    brand_depth = min(base_depth, len(brands))
    for idx in range(brand_depth):
        brand = brands[idx]
        if noun:
            if site == "ebay":
                candidates.append(_compact_query(f"{brand} {noun} NEW"))
            candidates.append(_compact_query(f"{brand} {noun}"))
        else:
            candidates.append(_compact_query(brand))

    if noun:
        candidates.append(noun)

    seen: set[str] = set()
    out: List[str] = []
    for candidate in candidates:
        q = _compact_query(candidate)
        if not q:
            continue
        k = q.upper()
        if k in seen:
            continue
        seen.add(k)
        out.append(q)

    metadata = {
        "applied": len(out) > 0,
        "category_key": key,
        "category_name": display_name or key,
        "active_season_tags": active_tags,
        "query_count": len(out),
    }
    return out, metadata


def _build_site_queries_with_meta(query: str, site: str) -> Tuple[List[str], Dict[str, Any]]:
    base = _compact_query(query)
    tokens = _query_tokens(base)
    title_tokens = _title_tokens(base)
    brand = _pick_brand_token(tokens or title_tokens)
    noun = _pick_generic_token(tokens or title_tokens)
    codes = _extract_codes(base)[:2]
    mid = [tok for tok in title_tokens if tok != brand and tok not in _GENERIC_TOKENS]

    candidates: List[str] = []
    knowledge_meta: Dict[str, Any] = {"applied": False, "category_key": "", "category_name": "", "active_season_tags": [], "query_count": 0}
    category_row = _match_category_row(base)
    if isinstance(category_row, dict):
        seed_queries, seed_meta = _build_category_seed_queries(category_row=category_row, site=site)
        candidates.extend(seed_queries)
        knowledge_meta = seed_meta

    if site == "ebay":
        if brand and codes:
            candidates.append(_compact_query(f"{brand} {codes[0]} NEW"))
        if codes:
            candidates.append(_compact_query(codes[0]))
        candidates.append(base)
        if brand and noun and noun != brand:
            candidates.append(_compact_query(f"{brand} {noun}"))
        if brand and mid:
            candidates.append(_compact_query(f"{brand} {mid[0]}"))
        if brand:
            candidates.append(brand)
    else:
        candidates.append(base)
        if brand and codes:
            candidates.append(_compact_query(f"{brand} {codes[0]}"))
        if codes:
            candidates.append(_compact_query(codes[0]))
        if brand and noun and noun != brand:
            candidates.append(_compact_query(f"{brand} {noun}"))
        if brand and mid:
            candidates.append(_compact_query(f"{brand} {mid[0]}"))
        if brand:
            candidates.append(brand)

    seen: set[str] = set()
    out: List[str] = []
    for candidate in candidates:
        q = _compact_query(candidate)
        if not q or len(q) < 2:
            continue
        key = q.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
    return out, knowledge_meta


def _build_site_queries(query: str, site: str) -> List[str]:
    queries, _ = _build_site_queries_with_meta(query, site)
    return queries


def _site_fetch_profile(site: str, cap_site: int) -> SiteFetchProfile:
    if site == "ebay":
        max_calls = max(1, min(6, _env_int("EBAY_FETCH_MAX_CALLS", 3)))
        per_call_limit = max(1, min(200, _env_int("EBAY_FETCH_PER_CALL_LIMIT", max(cap_site, 50))))
        target_items = max(4, min(120, _env_int("EBAY_FETCH_TARGET_ITEMS", max(cap_site, 12))))
        min_new_items = max(0, min(8, _env_int("EBAY_FETCH_MIN_NEW_ITEMS", 2)))
        max_pages_per_query = max(1, min(6, _env_int("EBAY_FETCH_MAX_PAGES_PER_QUERY", 2)))
        sleep_sec = max(0.0, min(3.0, _env_float("EBAY_FETCH_SLEEP_SEC", 0.25)))
        return SiteFetchProfile(
            site,
            max_calls,
            per_call_limit,
            target_items,
            min_new_items,
            max_pages_per_query,
            sleep_sec,
        )
    if site == "rakuten":
        max_calls = max(1, min(5, _env_int("RAKUTEN_FETCH_MAX_CALLS", 2)))
        per_call_limit = max(1, min(30, _env_int("RAKUTEN_FETCH_PER_CALL_LIMIT", max(cap_site, 30))))
        target_items = max(4, min(90, _env_int("RAKUTEN_FETCH_TARGET_ITEMS", max(10, cap_site))))
        min_new_items = max(0, min(6, _env_int("RAKUTEN_FETCH_MIN_NEW_ITEMS", 1)))
        max_pages_per_query = max(1, min(5, _env_int("RAKUTEN_FETCH_MAX_PAGES_PER_QUERY", 2)))
        sleep_sec = max(0.0, min(3.0, _env_float("RAKUTEN_FETCH_SLEEP_SEC", 1.0)))
        return SiteFetchProfile(
            site,
            max_calls,
            per_call_limit,
            target_items,
            min_new_items,
            max_pages_per_query,
            sleep_sec,
        )
    max_calls = max(1, min(5, _env_int("YAHOO_FETCH_MAX_CALLS", 3)))
    per_call_limit = max(1, min(100, _env_int("YAHOO_FETCH_PER_CALL_LIMIT", max(cap_site, 80))))
    target_items = max(4, min(90, _env_int("YAHOO_FETCH_TARGET_ITEMS", max(10, cap_site))))
    min_new_items = max(0, min(6, _env_int("YAHOO_FETCH_MIN_NEW_ITEMS", 1)))
    max_pages_per_query = max(1, min(5, _env_int("YAHOO_FETCH_MAX_PAGES_PER_QUERY", 2)))
    sleep_sec = max(0.0, min(3.0, _env_float("YAHOO_FETCH_SLEEP_SEC", 0.8)))
    return SiteFetchProfile(
        site,
        max_calls,
        per_call_limit,
        target_items,
        min_new_items,
        max_pages_per_query,
        sleep_sec,
    )


def _site_profile_bounds(site: str) -> Tuple[int, int, int, int, int]:
    site_key = str(site or "").strip().lower()
    if site_key == "ebay":
        return (1, 6, 20, 200, 20)
    if site_key == "rakuten":
        return (1, 5, 10, 30, 5)
    return (1, 5, 20, 100, 20)


def _site_min_new_bounds(site: str) -> Tuple[int, int]:
    site_key = str(site or "").strip().lower()
    if site_key == "ebay":
        return (1, 8)
    if site_key == "rakuten":
        return (0, 6)
    return (0, 6)


def _apply_fetch_tuner(
    *,
    site: str,
    profile: SiteFetchProfile,
) -> Tuple[SiteFetchProfile, Dict[str, Any]]:
    enabled = _env_bool("MINER_FETCH_AUTOTUNE_ENABLED", True)
    if not enabled:
        return profile, {"enabled": False, "applied": False}
    entries = _load_fetch_tuner_entries()
    site_key = str(site or "").strip().lower()
    row = entries.get(site_key)
    if not isinstance(row, dict):
        return profile, {"enabled": True, "applied": False}

    max_age = max(600, _env_int("MINER_FETCH_AUTOTUNE_MAX_AGE_SECONDS", 604800))
    now_ts = int(time.time())
    updated_at = _to_int(row.get("updated_at"), 0)
    if updated_at <= 0 or (now_ts - updated_at) > max_age:
        return profile, {"enabled": True, "applied": False, "stale": True}

    min_calls, max_calls_limit, min_limit, max_limit, _step = _site_profile_bounds(site_key)
    tuned_max_calls = _to_int(row.get("max_calls"), profile.max_calls)
    tuned_per_call_limit = _to_int(row.get("per_call_limit"), profile.per_call_limit)
    min_new_min, min_new_max = _site_min_new_bounds(site_key)
    tuned_min_new_items = _to_int(row.get("min_new_items"), profile.min_new_items)
    tuned = SiteFetchProfile(
        site=profile.site,
        max_calls=max(min_calls, min(max_calls_limit, tuned_max_calls)),
        per_call_limit=max(min_limit, min(max_limit, tuned_per_call_limit)),
        target_items=profile.target_items,
        min_new_items=max(min_new_min, min(min_new_max, tuned_min_new_items)),
        max_pages_per_query=profile.max_pages_per_query,
        sleep_sec=profile.sleep_sec,
    )
    return tuned, {
        "enabled": True,
        "applied": True,
        "updated_at": updated_at,
        "updated_at_iso": _epoch_to_iso(updated_at),
        "ema_efficiency": _to_float(row.get("ema_efficiency"), -1.0),
        "min_new_items": int(tuned.min_new_items),
    }


def _update_fetch_tuner(
    *,
    site: str,
    profile: SiteFetchProfile,
    calls_made: int,
    network_calls: int,
    merged_count: int,
    stop_reason: str,
) -> Dict[str, Any]:
    enabled = _env_bool("MINER_FETCH_AUTOTUNE_ENABLED", True)
    if not enabled:
        return {"enabled": False}
    # キャッシュのみの実行は外部API効率の学習データにならないため更新しない。
    if network_calls <= 0:
        return {"enabled": True, "saved": False, "reason": "no_network_calls"}
    site_key = str(site or "").strip().lower()
    entries = _load_fetch_tuner_entries()
    row = entries.get(site_key)
    if not isinstance(row, dict):
        row = {}

    min_calls, max_calls_limit, min_limit, max_limit, step = _site_profile_bounds(site_key)
    prev_ema = _to_float(row.get("ema_efficiency"), -1.0)
    eff = (float(merged_count) / float(network_calls)) if network_calls > 0 else -1.0
    if eff >= 0 and prev_ema >= 0:
        ema_eff = (prev_ema * 0.7) + (eff * 0.3)
    else:
        ema_eff = eff if eff >= 0 else prev_ema

    next_max_calls = int(profile.max_calls)
    next_per_call = int(profile.per_call_limit)
    min_new_min, min_new_max = _site_min_new_bounds(site_key)
    next_min_new = int(max(min_new_min, min(min_new_max, profile.min_new_items)))

    if stop_reason == "target_reached" and calls_made <= max(1, profile.max_calls // 2):
        next_max_calls = max(min_calls, profile.max_calls - 1)
        if merged_count >= profile.target_items:
            next_min_new = max(min_new_min, next_min_new - 1)
    elif stop_reason == "max_calls_reached":
        if merged_count <= 0:
            next_max_calls = max(min_calls, profile.max_calls - 1)
            next_per_call = max(min_limit, profile.per_call_limit - step)
            next_min_new = min(min_new_max, next_min_new + 1)
        elif merged_count < profile.target_items:
            next_max_calls = min(max_calls_limit, profile.max_calls + 1)

    if stop_reason == "low_yield_stop" and eff >= 0 and eff < max(1.0, float(profile.min_new_items)):
        next_max_calls = max(min_calls, next_max_calls - 1)
        next_per_call = max(min_limit, profile.per_call_limit - step)
        next_min_new = min(min_new_max, next_min_new + 1)
    elif stop_reason in {"max_calls_reached", "target_reached"} and network_calls >= 2 and merged_count > 0:
        next_per_call = min(max_limit, profile.per_call_limit + step)
        if merged_count >= profile.target_items:
            next_min_new = max(min_new_min, next_min_new - 1)

    now_ts = int(time.time())
    entries[site_key] = {
        "max_calls": int(max(min_calls, min(max_calls_limit, next_max_calls))),
        "per_call_limit": int(max(min_limit, min(max_limit, next_per_call))),
        "min_new_items": int(max(min_new_min, min(min_new_max, next_min_new))),
        "ema_efficiency": round(ema_eff, 4) if ema_eff >= 0 else -1.0,
        "last_efficiency": round(eff, 4) if eff >= 0 else -1.0,
        "last_stop_reason": str(stop_reason or ""),
        "last_calls_made": int(calls_made),
        "last_network_calls": int(network_calls),
        "last_merged_count": int(merged_count),
        "updated_at": now_ts,
    }
    _save_fetch_tuner_entries(entries)
    saved = entries[site_key]
    return {
        "enabled": True,
        "saved": True,
        "max_calls": int(saved.get("max_calls", profile.max_calls)),
        "per_call_limit": int(saved.get("per_call_limit", profile.per_call_limit)),
        "min_new_items": int(saved.get("min_new_items", profile.min_new_items)),
        "ema_efficiency": _to_float(saved.get("ema_efficiency"), -1.0),
        "updated_at": int(saved.get("updated_at", now_ts)),
        "updated_at_iso": _epoch_to_iso(_to_int(saved.get("updated_at"), now_ts)),
    }


def _item_identity(item: MarketItem) -> Tuple[str, str]:
    fallback = item.item_url or _title_key(item.title)
    return item.site, (item.item_id or fallback or "").strip().lower()


def _market_identity_key(item: MarketItem) -> str:
    fallback = item.item_url or _title_key(item.title)
    return (item.item_id or fallback or "").strip().lower()


def _sale_price_basis_from_signal(market: MarketItem, liquidity_signal: Dict[str, Any]) -> Tuple[float, str, float]:
    meta = liquidity_signal.get("metadata") if isinstance(liquidity_signal.get("metadata"), dict) else {}
    sold_min = _to_float(meta.get("sold_price_min"), -1.0)
    sold_min_raw = _to_float(meta.get("sold_price_min_raw"), sold_min)
    sold_min_outlier = bool(meta.get("sold_price_min_outlier"))
    sold_median = _to_float(liquidity_signal.get("sold_price_median"), -1.0)
    active_price = _to_float(market.price, -1.0)
    sold_min_vs_median = (sold_min / sold_median) if (sold_min > 0 and sold_median > 0) else -1.0
    min_ratio_vs_active = max(0.0, min(1.0, _env_float("LIQUIDITY_SOLD_PRICE_MIN_ACTIVE_RATIO_MIN", 0.08)))
    sold_min_vs_active = (sold_min / active_price) if (sold_min > 0 and active_price > 0) else -1.0
    sold_min_too_low_vs_active = sold_min_vs_active > 0 and sold_min_vs_active < min_ratio_vs_active
    robust_ratio_floor = max(0.0, min(1.0, _env_float("LIQUIDITY_SOLD_MIN_RATIO_FLOOR_FOR_FALLBACK", 0.45)))
    needs_median_fallback = (
        sold_median > 0
        and (
            sold_min_outlier
            or (sold_min_vs_median > 0 and sold_min_vs_median < robust_ratio_floor)
            or sold_min_too_low_vs_active
            or sold_min <= 0
        )
    )
    if needs_median_fallback:
        allow_fallback = _env_bool("LIQUIDITY_ALLOW_MEDIAN_FALLBACK_ON_OUTLIER", True)
        fallback_ratio = max(0.1, min(1.0, _env_float("LIQUIDITY_MEDIAN_FALLBACK_RATIO", 0.72)))
        if allow_fallback:
            # sold_minが付属品混入で無効化された場合、中央値に安全率を掛けて保守的に売値基準化する。
            fallback_basis = sold_median * fallback_ratio
            if sold_min_raw > 0:
                fallback_basis = max(fallback_basis, sold_min_raw)
            return fallback_basis, "sold_price_median_fallback_90d", 0.0
    if sold_min > 0 and not sold_min_too_low_vs_active:
        return sold_min, "sold_price_min_90d", 0.0
    if sold_median > 0:
        return sold_median, "sold_price_median_90d", 0.0
    return market.price, "active_listing_price", market.shipping


def _is_strict_sold_min_basis_candidate(
    *,
    sale_price_basis_type: str,
    sold_min_basis: float,
    sold_min_outlier: bool,
) -> bool:
    if sold_min_basis <= 0:
        return False
    if sold_min_outlier:
        return False
    return str(sale_price_basis_type or "").strip().lower() == "sold_price_min_90d"


def _liquidity_sold_sample(liquidity_signal: Dict[str, Any]) -> Dict[str, Any]:
    meta = liquidity_signal.get("metadata") if isinstance(liquidity_signal.get("metadata"), dict) else {}
    sample = meta.get("sold_sample") if isinstance(meta.get("sold_sample"), dict) else {}
    if not sample:
        return {}
    item_url = str(sample.get("item_url", "") or "").strip()
    image_url = str(sample.get("image_url", "") or "").strip()
    title = str(sample.get("title", "") or "").strip()
    sold_price = _to_float(sample.get("sold_price"), -1.0)
    out: Dict[str, Any] = {}
    if item_url:
        out["item_url"] = item_url
    if image_url:
        out["image_url"] = image_url
    if title:
        out["title"] = title
    if sold_price > 0:
        out["sold_price_usd"] = sold_price
    return out


def _has_sold_sample_reference(sample: Dict[str, Any]) -> bool:
    if not isinstance(sample, dict) or not sample:
        return False
    item_url = str(sample.get("item_url", "") or "").strip()
    sold_price = _to_float(sample.get("sold_price_usd"), -1.0)
    return bool(item_url and sold_price > 0)


def _liquidity_signal_is_reliable_for_pair(
    *,
    signal: Dict[str, Any],
    liquidity_query: str,
    source: "MarketItem",
    market: "MarketItem",
) -> Tuple[bool, str]:
    if not isinstance(signal, dict) or not signal:
        return False, "signal_empty"

    sold_90d_count = _to_int(signal.get("sold_90d_count"), -1)
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    filter_state = metadata.get("filter_state") if isinstance(metadata.get("filter_state"), dict) else {}

    # Positive sold count must come from confirmed Sold + Last 90 days, or explicit early-no-sold path.
    if sold_90d_count > 0:
        sold_tab_selected = bool(filter_state.get("sold_tab_selected"))
        lookback_selected = str(filter_state.get("lookback_selected", "") or "").strip().lower()
        if filter_state and ((not sold_tab_selected) or lookback_selected != "last 90 days"):
            return False, "signal_unconfirmed_sold_last90"
        filtered_row_count = _to_int(metadata.get("filtered_row_count"), -1)
        sold_sample = _liquidity_sold_sample(signal)
        has_sold_sample = _has_sold_sample_reference(sold_sample)
        if filtered_row_count <= 0 and not has_sold_sample:
            return False, "signal_positive_without_filtered_rows_or_sample"

    # If RPA query has a model-like code, it must match this pair.
    rpa_query = str(metadata.get("rpa_query", "") or metadata.get("query", "") or "").strip()
    if rpa_query:
        rpa_codes = _query_specific_codes(rpa_query)
        if rpa_codes:
            pair_codes = _item_model_code_keys(source) | _item_model_code_keys(market)
            if pair_codes:
                related = False
                for rp in rpa_codes:
                    if any(_is_related_model_code(code, rp) or _is_related_model_code(rp, code) for code in pair_codes):
                        related = True
                        break
                if not related:
                    return False, "signal_rpa_query_model_mismatch"

    # The selected liquidity query itself must match pair as well.
    if not _liquidity_query_matches_pair(query=liquidity_query, source=source, market=market):
        return False, "signal_liquidity_query_pair_mismatch"
    return True, ""


def _is_implausible_sold_min(
    *,
    sold_min_raw_usd: float,
    source_total_usd: float,
    active_total_usd: float,
    sold_min_outlier_flag: bool,
) -> Tuple[bool, Dict[str, Any]]:
    if sold_min_raw_usd <= 0:
        return False, {}
    ratio_to_source = (sold_min_raw_usd / source_total_usd) if source_total_usd > 0 else -1.0
    ratio_to_active = (sold_min_raw_usd / active_total_usd) if active_total_usd > 0 else -1.0
    ratio_source_floor = max(0.0, min(1.0, _env_float("LIQUIDITY_SOLD_PRICE_MIN_SOURCE_RATIO_MIN", 0.08)))
    ratio_active_floor = max(0.0, min(1.0, _env_float("LIQUIDITY_SOLD_PRICE_MIN_ACTIVE_RATIO_MIN", 0.08)))
    abs_floor = max(0.01, _env_float("LIQUIDITY_SOLD_PRICE_MIN_ABS_FLOOR_USD", 1.0))

    too_low_abs = sold_min_raw_usd < abs_floor
    too_low_source = ratio_to_source > 0 and ratio_to_source < ratio_source_floor
    too_low_active = ratio_to_active > 0 and ratio_to_active < ratio_active_floor
    enabled = _env_bool("LIQUIDITY_SOLD_PRICE_MIN_IMPLAUSIBLE_REJECT_ENABLED", True)
    reject = enabled and (too_low_abs or too_low_source or too_low_active) and (not sold_min_outlier_flag)
    return reject, {
        "enabled": bool(enabled),
        "abs_floor_usd": float(abs_floor),
        "ratio_source_floor": float(ratio_source_floor),
        "ratio_active_floor": float(ratio_active_floor),
        "sold_min_raw_usd": float(round(sold_min_raw_usd, 6)),
        "source_total_usd": float(round(source_total_usd, 6)) if source_total_usd > 0 else -1.0,
        "active_total_usd": float(round(active_total_usd, 6)) if active_total_usd > 0 else -1.0,
        "ratio_to_source": float(round(ratio_to_source, 6)) if ratio_to_source > 0 else -1.0,
        "ratio_to_active": float(round(ratio_to_active, 6)) if ratio_to_active > 0 else -1.0,
        "too_low_abs": bool(too_low_abs),
        "too_low_source": bool(too_low_source),
        "too_low_active": bool(too_low_active),
        "sold_min_outlier_flag": bool(sold_min_outlier_flag),
    }


def _resolve_current_fx_rate(settings: Settings) -> float:
    calc = calculate_profit(
        ProfitInput(
            sale_price_usd=1.0,
            purchase_price_jpy=0.0,
            domestic_shipping_jpy=0.0,
            international_shipping_usd=0.0,
            customs_usd=0.0,
            packaging_usd=0.0,
            marketplace_fee_rate=0.0,
            payment_fee_rate=0.0,
            fixed_fee_usd=0.0,
        ),
        settings=settings,
    )
    return _to_float((calc.get("fx") or {}).get("rate"), 0.0)


def _required_profit_floor_usd(*, sale_total_usd: float, min_profit_usd: float, min_margin_rate: float) -> float:
    if sale_total_usd <= 0:
        return float(min_profit_usd)
    return max(float(min_profit_usd), float(sale_total_usd) * max(0.0, float(min_margin_rate)))


def _max_purchase_total_jpy_for_sale(
    *,
    sale_total_usd: float,
    fx_rate: float,
    min_profit_usd: float,
    min_margin_rate: float,
    marketplace_fee_rate: float,
    payment_fee_rate: float,
    international_shipping_usd: float,
    customs_usd: float,
    packaging_usd: float,
    fixed_fee_usd: float,
    misc_cost_usd: float = 0.0,
) -> float:
    if sale_total_usd <= 0 or fx_rate <= 0:
        return -1.0
    required_profit = _required_profit_floor_usd(
        sale_total_usd=sale_total_usd,
        min_profit_usd=min_profit_usd,
        min_margin_rate=min_margin_rate,
    )
    fee_rate = max(0.0, float(marketplace_fee_rate) + float(payment_fee_rate))
    variable_fee = float(sale_total_usd) * fee_rate
    non_jpy_usd = (
        float(international_shipping_usd)
        + float(customs_usd)
        + float(packaging_usd)
        + float(fixed_fee_usd)
        + float(misc_cost_usd)
    )
    purchase_budget_usd = float(sale_total_usd) - variable_fee - non_jpy_usd - required_profit
    if purchase_budget_usd <= 0:
        return -1.0
    return purchase_budget_usd * float(fx_rate)


def _build_ebay_sold_first_plan(
    *,
    query: str,
    ebay_items: Sequence[MarketItem],
    query_specific_codes: set[str],
    active_count_hint: int,
    timeout: int,
    settings: Settings,
    min_sold_90d_count: int,
    min_sell_through_90d: float,
    liquidity_require_signal: bool,
    min_profit_usd: float,
    min_margin_rate: float,
    marketplace_fee_rate: float,
    payment_fee_rate: float,
    international_shipping_usd: float,
    customs_usd: float,
    packaging_usd: float,
    fixed_fee_usd: float,
) -> Dict[str, Any]:
    enabled = _env_bool("MINER_FETCH_EBAY_SOLD_FIRST_ENABLED", True)
    summary: Dict[str, Any] = {
        "enabled": bool(enabled),
        "applied": False,
        "codes_considered": 0,
        "codes_passed": 0,
        "selected_codes": [],
        "reason": "",
        "rows": [],
    }
    if not enabled:
        summary["reason"] = "disabled"
        return {"summary": summary, "selected_codes": set(), "max_purchase_jpy_by_code": {}, "signals": {}}

    max_codes = max(1, min(24, _env_int("MINER_FETCH_EBAY_SOLD_FIRST_MAX_CODES", 10)))
    stats: Dict[str, Dict[str, Any]] = {}
    for code in query_specific_codes:
        stats[code] = {"count": 1000, "example": code}
    for item in ebay_items:
        for raw in _extract_codes(item.title):
            if not _is_specific_model_code(raw):
                continue
            canon = _canonicalize_code(raw)
            if not canon:
                continue
            row = stats.get(canon)
            if not isinstance(row, dict):
                row = {"count": 0, "example": str(raw).strip().upper()}
                stats[canon] = row
            row["count"] = _to_int(row.get("count"), 0) + 1
            example = str(row.get("example", "") or "")
            code = str(raw).strip().upper()
            if len(code) > len(example):
                row["example"] = code

    ranked_codes = [
        str((row or {}).get("example", "") or "").strip().upper()
        for _, row in sorted(
            stats.items(),
            key=lambda kv: (
                -_to_int((kv[1] or {}).get("count"), 0),
                -len(str((kv[1] or {}).get("example", "") or "")),
            ),
        )
    ]
    ranked_codes = [c for c in ranked_codes if c][:max_codes]
    summary["codes_considered"] = len(ranked_codes)
    if not ranked_codes:
        summary["reason"] = "no_model_codes"
        return {"summary": summary, "selected_codes": set(), "max_purchase_jpy_by_code": {}, "signals": {}}

    fx_rate = _resolve_current_fx_rate(settings)
    require_sold_sample = _env_bool("MINER_FETCH_EBAY_SOLD_FIRST_REQUIRE_SOLD_SAMPLE", True)
    selected_codes: set[str] = set()
    max_purchase_jpy_by_code: Dict[str, float] = {}
    signals_by_code: Dict[str, Dict[str, Any]] = {}
    rows: List[Dict[str, Any]] = []

    for code in ranked_codes:
        signal = get_liquidity_signal(
            query=code,
            source_title=code,
            market_title=code,
            source_identifiers={},
            market_identifiers={},
            active_count_hint=active_count_hint,
            timeout=timeout,
            settings=settings,
        )
        gate = evaluate_liquidity_gate(
            signal,
            min_sold_90d_count=min_sold_90d_count,
            min_sell_through_90d=min_sell_through_90d,
            require_signal=liquidity_require_signal,
        )
        sold_sample = _liquidity_sold_sample(signal)
        sold_sample_ok = _has_sold_sample_reference(sold_sample)
        signal_meta = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        median_price = _to_float(signal.get("sold_price_median"), 0.0)
        market_stub = MarketItem(
            site="ebay",
            item_id=f"sold:{code}",
            title=code,
            item_url="",
            image_url="",
            price=median_price if median_price > 0 else 0.0,
            shipping=0.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )
        sale_basis_usd, sale_basis_type, sale_basis_ship = _sale_price_basis_from_signal(market_stub, signal)
        sale_total_usd = sale_basis_usd + sale_basis_ship
        max_purchase_jpy = _max_purchase_total_jpy_for_sale(
            sale_total_usd=sale_total_usd,
            fx_rate=fx_rate,
            min_profit_usd=min_profit_usd,
            min_margin_rate=min_margin_rate,
            marketplace_fee_rate=marketplace_fee_rate,
            payment_fee_rate=payment_fee_rate,
            international_shipping_usd=international_shipping_usd,
            customs_usd=customs_usd,
            packaging_usd=packaging_usd,
            fixed_fee_usd=fixed_fee_usd,
        )
        passed = bool(gate.get("pass")) and max_purchase_jpy > 0 and sale_total_usd > 0
        if require_sold_sample and not sold_sample_ok:
            passed = False
        row = {
            "code": code,
            "signal_key": str(signal.get("signal_key", "") or ""),
            "sold_90d_count": _to_int(signal.get("sold_90d_count"), -1),
            "sold_price_median": _to_float(signal.get("sold_price_median"), -1.0),
            "sold_price_min": _to_float(signal_meta.get("sold_price_min"), -1.0),
            "sold_sample_ok": bool(sold_sample_ok),
            "sale_basis_usd": round(sale_basis_usd, 4) if sale_basis_usd > 0 else -1.0,
            "sale_basis_type": sale_basis_type,
            "gate_pass": bool(gate.get("pass")),
            "gate_reason": (
                "missing_sold_sample_reference"
                if (require_sold_sample and (not sold_sample_ok))
                else str(gate.get("reason", "") or "")
            ),
            "max_purchase_total_jpy": round(max_purchase_jpy, 2) if max_purchase_jpy > 0 else -1.0,
            "pass": bool(passed),
        }
        rows.append(row)
        if not passed:
            continue
        canon = _canonicalize_code(code)
        if not canon:
            continue
        selected_codes.add(canon)
        max_purchase_jpy_by_code[canon] = float(max_purchase_jpy)
        signals_by_code[canon] = signal

    summary["rows"] = rows
    summary["selected_codes"] = sorted(selected_codes)
    summary["codes_passed"] = len(selected_codes)
    summary["applied"] = len(selected_codes) > 0
    summary["reason"] = "ok" if selected_codes else "no_codes_passed"
    return {
        "summary": summary,
        "selected_codes": selected_codes,
        "max_purchase_jpy_by_code": max_purchase_jpy_by_code,
        "signals": signals_by_code,
    }


def _filter_source_items_by_purchase_ceiling(
    *,
    items: Sequence[MarketItem],
    max_purchase_jpy_by_code: Dict[str, float],
    require_code_match: bool,
) -> Tuple[List[MarketItem], Dict[str, Any]]:
    if not max_purchase_jpy_by_code:
        return list(items), {"enabled": False, "applied": False}
    slack_ratio = max(1.0, _env_float("MINER_FETCH_EBAY_SOLD_FIRST_BUDGET_SLACK_RATIO", 2.0))
    kept: List[MarketItem] = []
    dropped_no_code = 0
    dropped_over_budget = 0
    for item in items:
        item_codes = _item_model_code_keys(item)
        if not item_codes:
            if require_code_match:
                dropped_no_code += 1
                continue
            kept.append(item)
            continue
        matched = [max_purchase_jpy_by_code.get(code) for code in item_codes if code in max_purchase_jpy_by_code]
        if not matched:
            if require_code_match:
                dropped_no_code += 1
                continue
            kept.append(item)
            continue
        ceiling = max(v for v in matched if isinstance(v, (int, float)))
        purchase_total = _to_float(item.price, 0.0) + _to_float(item.shipping, 0.0)
        if ceiling > 0 and purchase_total > (ceiling * slack_ratio):
            dropped_over_budget += 1
            continue
        kept.append(item)
    return kept, {
        "enabled": True,
        "applied": True,
        "before": len(items),
        "after": len(kept),
        "dropped_no_code": dropped_no_code,
        "dropped_over_budget": dropped_over_budget,
        "slack_ratio": float(round(slack_ratio, 4)),
    }


def _rank_specific_codes_from_market_items(
    market_items: Sequence[MarketItem],
    *,
    max_codes: int,
) -> List[str]:
    cap = max(1, int(max_codes))
    stats: Dict[str, Dict[str, Any]] = {}
    for item in market_items:
        for raw in _extract_codes(item.title):
            code = str(raw or "").strip().upper()
            if not _is_specific_model_code(code):
                continue
            canon = _canonicalize_code(code)
            if not canon:
                continue
            row = stats.get(canon)
            if not isinstance(row, dict):
                row = {"code": code, "count": 0}
                stats[canon] = row
            row["count"] = _to_int(row.get("count"), 0) + 1
            best = str(row.get("code", "") or "")
            if len(code) > len(best):
                row["code"] = code
    ranked = [
        str((row or {}).get("code", "") or "").strip().upper()
        for _, row in sorted(
            stats.items(),
            key=lambda kv: (
                -_to_int((kv[1] or {}).get("count"), 0),
                -len(str((kv[1] or {}).get("code", "") or "")),
                str((kv[1] or {}).get("code", "") or ""),
            ),
        )
    ]
    return [code for code in ranked if code][:cap]


def _fetch_site_items_adaptive(
    *,
    site: str,
    query: str,
    cap_site: int,
    timeout: int,
    require_in_stock: bool = True,
) -> Tuple[List[MarketItem], Dict[str, Any], Optional[str]]:
    fetcher: Callable[[str, int, int, int, bool], Tuple[List[MarketItem], Dict[str, Any]]]
    if site == "ebay":
        fetcher = _search_ebay
    elif site == "rakuten":
        fetcher = _search_rakuten
    elif site == "yahoo":
        fetcher = _search_yahoo
    else:
        raise ValueError(f"unsupported site: {site}")

    base_profile = _site_fetch_profile(site, cap_site)
    profile, autotune_in = _apply_fetch_tuner(site=site, profile=base_profile)
    queries, knowledge_meta = _build_site_queries_with_meta(query, site)
    if not queries:
        queries = [_compact_query(query)]
    total_queries = len(queries)
    knowledge_applied = bool(knowledge_meta.get("applied"))
    relevance_filter_enabled = _env_bool("MINER_CATEGORY_RELEVANCE_FILTER_ENABLED", True)
    relevance_terms: Tuple[str, ...] = ()
    if knowledge_applied and relevance_filter_enabled:
        category_row = _match_category_row(_compact_query(query))
        if isinstance(category_row, dict):
            relevance_terms = _build_category_relevance_terms(category_row)
    min_queries_before_target = 1
    if knowledge_applied:
        if site == "ebay":
            min_calls_floor = max(1, min(6, _env_int("MINER_CATEGORY_FETCH_MIN_CALLS_EBAY", 3)))
            per_call_cap = max(20, min(200, _env_int("MINER_CATEGORY_FETCH_PER_CALL_LIMIT_CAP_EBAY", 50)))
            min_queries_before_target = max(
                1,
                min(
                    min_calls_floor,
                    _env_int("MINER_CATEGORY_MIN_QUERIES_BEFORE_TARGET_EBAY", min_calls_floor),
                ),
            )
        elif site == "rakuten":
            min_calls_floor = max(1, min(5, _env_int("MINER_CATEGORY_FETCH_MIN_CALLS_RAKUTEN", 2)))
            per_call_cap = max(10, min(30, _env_int("MINER_CATEGORY_FETCH_PER_CALL_LIMIT_CAP_RAKUTEN", 20)))
            min_queries_before_target = max(
                1,
                min(
                    min_calls_floor,
                    _env_int("MINER_CATEGORY_MIN_QUERIES_BEFORE_TARGET_RAKUTEN", min_calls_floor),
                ),
            )
        else:
            min_calls_floor = max(1, min(5, _env_int("MINER_CATEGORY_FETCH_MIN_CALLS_YAHOO", 2)))
            per_call_cap = max(20, min(100, _env_int("MINER_CATEGORY_FETCH_PER_CALL_LIMIT_CAP_YAHOO", 40)))
            min_queries_before_target = max(
                1,
                min(
                    min_calls_floor,
                    _env_int("MINER_CATEGORY_MIN_QUERIES_BEFORE_TARGET_YAHOO", min_calls_floor),
                ),
            )
        profile = SiteFetchProfile(
            site=profile.site,
            max_calls=max(profile.max_calls, min_calls_floor),
            per_call_limit=min(profile.per_call_limit, per_call_cap),
            target_items=max(profile.target_items, cap_site * max(1, min_queries_before_target)),
            min_new_items=profile.min_new_items,
            max_pages_per_query=profile.max_pages_per_query,
            sleep_sec=profile.sleep_sec,
        )

    entries = _load_fetch_cursor_entries()
    cursor_key = _fetch_cursor_key(site=site, query=query)
    cursor_row = entries.get(cursor_key) if isinstance(entries.get(cursor_key), dict) else {}
    start_query_index = _to_int((cursor_row or {}).get("query_index"), 0)
    start_page = _to_int((cursor_row or {}).get("page"), 1)
    force_exact_model_query = _env_bool("MINER_FETCH_FORCE_EXACT_FOR_MODEL_QUERY", True)
    if force_exact_model_query and _extract_codes(_compact_query(query)):
        start_query_index = 0
        start_page = 1
    if start_query_index < 0 or start_query_index >= total_queries:
        start_query_index = 0
    if start_page < 1 or start_page > profile.max_pages_per_query:
        start_page = 1
    query_order = list(range(start_query_index, total_queries)) + list(range(0, start_query_index))

    seen_items: set[Tuple[str, str]] = set()
    merged_items: List[MarketItem] = []
    query_logs: List[Dict[str, Any]] = []
    touched_queries: set[int] = set()
    relevance_filtered_out = 0
    calls_made = 0
    stop_reason = "query_exhausted"
    last_error: Optional[str] = None
    next_query_index = start_query_index
    next_page = start_page

    for order_pos, q_index in enumerate(query_order):
        q = queries[q_index]
        touched_queries.add(q_index)
        if calls_made >= profile.max_calls:
            stop_reason = "max_calls_reached"
            next_query_index = q_index
            next_page = start_page if order_pos == 0 else 1
            break

        page = start_page if order_pos == 0 else 1
        zero_new_streak = 0
        while page <= profile.max_pages_per_query:
            if calls_made >= profile.max_calls:
                stop_reason = "max_calls_reached"
                next_query_index = q_index
                next_page = page
                break

            started_at = time.time()
            calls_made += 1
            try:
                rows, info = fetcher(q, profile.per_call_limit, timeout, page, require_in_stock)
            except Exception as err:
                last_error = str(err)
                query_logs.append(
                    {
                        "query": q,
                        "query_index": q_index,
                        "page": page,
                        "ok": False,
                        "error": str(err),
                        "duration_ms": int((time.time() - started_at) * 1000),
                    }
                )
                stop_reason = "error"
                next_query_index = q_index
                next_page = page
                break

            before = len(merged_items)
            for item in rows:
                if relevance_terms and (not _title_matches_category_terms(item.title, relevance_terms)):
                    relevance_filtered_out += 1
                    continue
                ident = _item_identity(item)
                if ident in seen_items:
                    continue
                seen_items.add(ident)
                merged_items.append(item)
            new_items = len(merged_items) - before
            query_logs.append(
                {
                    "query": q,
                    "query_index": q_index,
                    "page": page,
                    "ok": True,
                    "fetched_count": len(rows),
                    "new_count": new_items,
                    "duration_ms": int((time.time() - started_at) * 1000),
                    "info": info,
                }
            )
            next_query_index = q_index
            next_page = page + 1
            if next_page > profile.max_pages_per_query:
                next_query_index = (q_index + 1) % total_queries
                next_page = 1

            if len(merged_items) >= profile.target_items:
                if len(touched_queries) >= max(1, min_queries_before_target):
                    stop_reason = "target_reached"
                    break
                stop_reason = "target_deferred"
                next_query_index = (q_index + 1) % total_queries
                next_page = 1
                break

            if new_items == 0:
                zero_new_streak += 1
            else:
                zero_new_streak = 0

            # 低ヒットクエリで終盤まで新規が増えない場合は早期停止してAPIを節約。
            if (
                calls_made >= max(2, profile.max_calls - 1)
                and len(merged_items) <= max(1, profile.min_new_items)
                and zero_new_streak >= 1
            ):
                stop_reason = "low_yield_stop"
                next_query_index = (q_index + 1) % total_queries
                next_page = 1
                break

            if (
                new_items < profile.min_new_items
                and calls_made >= 2
                and len(merged_items) >= max(4, profile.target_items // 2)
            ):
                stop_reason = "low_yield_stop"
                next_query_index = (q_index + 1) % total_queries
                next_page = 1
                break
            if zero_new_streak >= 1 and page >= 2:
                next_query_index = (q_index + 1) % total_queries
                next_page = 1
                break

            raw_total = _to_int((info or {}).get("raw_total"), -1)
            raw_count = _to_int((info or {}).get("raw_count"), -1)
            total = raw_total if raw_total >= 0 else raw_count
            if total >= 0 and page * profile.per_call_limit >= total:
                next_query_index = (q_index + 1) % total_queries
                next_page = 1
                break
            if len(rows) < profile.per_call_limit:
                next_query_index = (q_index + 1) % total_queries
                next_page = 1
                break

            page += 1
            if profile.sleep_sec > 0 and calls_made < profile.max_calls:
                time.sleep(profile.sleep_sec)

        if stop_reason in {"error", "target_reached", "low_yield_stop", "max_calls_reached"}:
            break
        if page > profile.max_pages_per_query:
            next_query_index = (q_index + 1) % total_queries
            next_page = 1

    if stop_reason == "target_deferred":
        stop_reason = "query_exhausted"

    now_ts = int(time.time())
    retention_sec = max(3600, _env_int("MINER_FETCH_CURSOR_RETENTION_SECONDS", 604800))
    for key in list(entries.keys()):
        row = entries.get(key) if isinstance(entries.get(key), dict) else {}
        updated_at = _to_int((row or {}).get("updated_at"), 0)
        if updated_at <= 0:
            continue
        if now_ts - updated_at > retention_sec:
            entries.pop(key, None)
    entries[cursor_key] = {
        "query_index": int(next_query_index),
        "page": int(next_page),
        "updated_at": now_ts,
    }
    _save_fetch_cursor_entries(entries)

    cache_hits = 0
    network_calls = 0
    budget_remaining = -1
    for row in query_logs:
        if not isinstance(row, dict):
            continue
        if not bool(row.get("ok")):
            network_calls += 1
            continue
        info_row = row.get("info", {})
        if not isinstance(info_row, dict):
            network_calls += 1
            continue
        if bool(info_row.get("cache_hit")):
            cache_hits += 1
        else:
            network_calls += 1
        br = _to_int(info_row.get("budget_remaining"), -1)
        if br >= 0:
            budget_remaining = br

    details = {
        "ok": last_error is None,
        "count": len(merged_items),
        "calls_made": calls_made,
        "cache_hits": cache_hits,
        "network_calls": network_calls,
        "budget_remaining": budget_remaining,
        "max_calls": profile.max_calls,
        "per_call_limit": profile.per_call_limit,
        "target_items": profile.target_items,
        "max_pages_per_query": profile.max_pages_per_query,
        "stop_reason": stop_reason,
        "require_in_stock": bool(require_in_stock),
        "autotune_in": autotune_in,
        "cursor_start": {"query_index": start_query_index, "page": start_page},
        "cursor_next": {"query_index": int(next_query_index), "page": int(next_page)},
        "queries": query_logs,
        "knowledge": knowledge_meta,
        "category_relevance_filter_enabled": bool(knowledge_applied and relevance_filter_enabled),
        "category_relevance_term_count": int(len(relevance_terms)),
        "category_relevance_filtered_out": int(relevance_filtered_out),
        "min_queries_before_target": int(max(1, min_queries_before_target)),
        "touched_query_count": int(len(touched_queries)),
    }
    autotune_out = _update_fetch_tuner(
        site=site,
        profile=profile,
        calls_made=calls_made,
        network_calls=network_calls,
        merged_count=len(merged_items),
        stop_reason=stop_reason,
    )
    details["autotune_out"] = autotune_out
    return merged_items, details, last_error


def _normalize_for_tokens(text: str) -> str:
    upper = text.upper()
    return re.sub(r"[^A-Z0-9]+", " ", upper)


def _contains_used_marker(text: str) -> bool:
    normalized = unicodedata.normalize("NFKC", str(text or ""))
    upper = normalized.upper()
    compact = re.sub(r"\s+", "", normalized)
    for term in _USED_TERMS:
        if term.isascii():
            if term in upper:
                return True
        else:
            if term in normalized or term in compact:
                return True
    for pattern in _USED_PATTERNS:
        if pattern.search(normalized):
            return True
    return False


def _contains_out_of_stock_marker(text: str) -> bool:
    normalized = unicodedata.normalize("NFKC", str(text or ""))
    upper = normalized.upper()
    compact = re.sub(r"\s+", "", normalized)
    for term in _OUT_OF_STOCK_TERMS:
        if term.isascii():
            if term in upper:
                return True
        else:
            if term in normalized or term in compact:
                return True
    for pattern in _OUT_OF_STOCK_PATTERNS:
        if pattern.search(normalized):
            return True
    return False


def _is_new_listing(title: str, condition: str = "") -> bool:
    if _contains_used_marker(title):
        return False

    cond_raw = unicodedata.normalize("NFKC", str(condition or "")).strip()
    cond = cond_raw.upper()
    if cond:
        if cond in {"NEW", "NEW_OTHER", "NEW_WITH_TAGS", "NEW_WITHOUT_TAGS", "NEW_WITH_DEFECTS"}:
            return True
        if cond in {"USED", "PRE_OWNED", "PRE-OWNED", "REFURBISHED", "SELLER_REFURBISHED", "OPEN_BOX"}:
            return False
        if "USED" in cond or "REFURB" in cond:
            return False
        if "NEW" in cond and "USED" not in cond:
            return True
        # 条件があるが新品を断定できない場合は precision-first で除外。
        return False
    return True


def _source_stock_status(item: MarketItem) -> str:
    raw = item.raw if isinstance(item.raw, dict) else {}
    if not raw:
        return ""
    site = str(item.site or "").strip().lower()
    if site in {"yahoo", "yahoo_shopping"}:
        val = raw.get("inStock")
        if isinstance(val, bool):
            return "在庫あり" if val else "在庫なし"
        if isinstance(val, (int, float)):
            return "在庫あり" if int(val) > 0 else "在庫なし"
        if isinstance(val, str):
            norm = val.strip().lower()
            if norm in {"true", "1", "yes"}:
                return "在庫あり"
            if norm in {"false", "0", "no"}:
                return "在庫なし"
    if site == "rakuten":
        val = raw.get("availability")
        if isinstance(val, (int, float)):
            return "在庫あり" if int(val) == 1 else "在庫なし"
        if isinstance(val, str):
            norm = val.strip().lower()
            if norm in {"1", "true"}:
                return "在庫あり"
            if norm in {"0", "false"}:
                return "在庫なし"
    return ""


def _is_accessory_title(title: str) -> bool:
    upper = str(title or "").upper()
    jp = str(title or "")
    has_case_dimension = ("ケース幅" in jp) or ("CASE WIDTH" in upper) or ("CASE SIZE" in upper)
    for term in _FORCE_ACCESSORY_TERMS:
        if term in {"ケース", "CASE"} and has_case_dimension:
            continue
        if term.isascii():
            if term in upper:
                return True
        else:
            if term in jp:
                return True

    if "BRACELET" in upper and "WATCH" not in upper and "腕時計" not in jp:
        return True

    for term in _STRONG_ACCESSORY_TERMS:
        if term in {"CASE", "WATCH CASE", "METAL CASE"} and has_case_dimension:
            continue
        if term.isascii():
            if term in upper:
                return True
        else:
            if term in jp:
                return True

    has_accessory = False
    accessory_hits = 0
    for term in _ACCESSORY_TERMS:
        if term in {"ケース", "CASE"} and has_case_dimension:
            continue
        if term.isascii():
            if term in upper:
                has_accessory = True
                accessory_hits += 1
        else:
            if term in jp:
                has_accessory = True
                accessory_hits += 1
    if not has_accessory:
        return False
    if accessory_hits >= 2:
        return True

    has_hint = False
    for hint in _ACCESSORY_HINTS:
        if hint.isascii():
            if hint in upper:
                has_hint = True
                break
        else:
            if hint in jp:
                has_hint = True
                break

    has_watch_core = False
    for term in _WATCH_CORE_TERMS:
        if term.isascii():
            if term in upper:
                has_watch_core = True
                break
        else:
            if term in jp:
                has_watch_core = True
                break

    if has_hint:
        return True
    return not has_watch_core


def _title_tokens(title: str) -> List[str]:
    base = _normalize_for_tokens(title)
    raw = [tok for tok in base.split() if len(tok) >= 3]
    return [tok for tok in raw if tok not in _STOPWORDS]


def _contains_ascii_token(text_upper: str, token: str) -> bool:
    if not token:
        return False
    escaped = re.escape(token.upper())
    return re.search(rf"(?<![A-Z0-9]){escaped}(?![A-Z0-9])", text_upper) is not None


def _is_informative_code(token: str) -> bool:
    cleaned = str(token or "").strip("-").upper()
    if len(cleaned) < 4:
        return False
    if cleaned in _STOPWORDS:
        return False
    if cleaned.isdigit():
        return False
    if _MEASUREMENT_CODE_RE.fullmatch(cleaned):
        return False
    alpha_count = sum(1 for ch in cleaned if "A" <= ch <= "Z")
    digit_count = sum(1 for ch in cleaned if ch.isdigit())
    if alpha_count < 2 or digit_count < 1:
        return False
    return True


def _extract_codes(title: str) -> List[str]:
    upper = title.upper()
    codes: List[str] = []
    for token in _CODE_RE.findall(upper):
        cleaned = token.strip("-")
        if not _is_informative_code(cleaned):
            continue
        codes.append(cleaned)
    return sorted(set(codes))


def _extract_codes_in_order(title: str) -> List[str]:
    upper = str(title or "").upper()
    seen: set[str] = set()
    out: List[str] = []
    for token in _CODE_RE.findall(upper):
        cleaned = token.strip("-")
        if not _is_informative_code(cleaned):
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _extract_primary_model_code(title: str) -> str:
    ordered_codes = _extract_codes_in_order(title)
    if not ordered_codes:
        return ""
    for code in ordered_codes:
        if _is_specific_model_code(code):
            return code
    return ordered_codes[0]


def _with_title_identifier_hints(base: Dict[str, str], title: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, val in (base or {}).items():
        key_text = str(key or "").strip().lower()
        value_text = str(val or "").strip()
        if not key_text or not value_text:
            continue
        out[key_text] = value_text
    model_hint = _extract_primary_model_code(title)
    if model_hint:
        out.setdefault("model", model_hint)
        out.setdefault("mpn", model_hint)
    return out


def _canonicalize_code(token: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(token or "").upper())


def _canonical_code_set(codes: Sequence[str]) -> set[str]:
    out: set[str] = set()
    for code in codes:
        normalized = _canonicalize_code(code)
        if normalized:
            out.add(normalized)
    return out


def _specific_model_codes_in_title(title: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in _extract_codes_in_order(title):
        if not _is_specific_model_code(raw):
            continue
        canon = _canonicalize_code(raw)
        if not canon or canon in seen:
            continue
        seen.add(canon)
        out.append(raw)
    return out


def _extract_rakuten_variant_price_from_html(html: str, *, target_code: str) -> float:
    text = unicodedata.normalize("NFKC", str(html or ""))
    if not text:
        return -1.0
    upper = text.upper()
    canon_target = _canonicalize_code(target_code)
    if not canon_target:
        return -1.0
    candidates: List[Tuple[int, float]] = []
    for m in _CODE_RE.finditer(upper):
        token = str(m.group(0) or "").strip("-")
        if _canonicalize_code(token) != canon_target:
            continue
        start = max(0, m.start() - 260)
        end = min(len(upper), m.end() + 260)
        chunk = upper[start:end]
        for pm in _PRICE_JPY_RE.finditer(chunk):
            value = _to_float(str(pm.group(1) or "").replace(",", ""), -1.0)
            if value < 1000 or value > 5_000_000:
                continue
            absolute_dist = abs((start + pm.start()) - m.start())
            candidates.append((absolute_dist, value))
        for pm in re.finditer(r'"PRICE"\s*:\s*([0-9]{4,7})', chunk):
            value = _to_float(pm.group(1), -1.0)
            if value < 1000 or value > 5_000_000:
                continue
            absolute_dist = abs((start + pm.start()) - m.start())
            candidates.append((absolute_dist + 8, value))
    if not candidates:
        escaped = re.escape(str(target_code or "").upper())
        around_patterns = (
            rf"{escaped}.{{0,90}}?([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]{{4,7}})\s*円",
            rf"([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]{{4,7}})\s*円.{{0,90}}?{escaped}",
        )
        for pat in around_patterns:
            for mm in re.finditer(pat, upper, re.IGNORECASE | re.DOTALL):
                value = _to_float(str(mm.group(1) or "").replace(",", ""), -1.0)
                if value < 1000 or value > 5_000_000:
                    continue
                candidates.append((120, value))
    if not candidates:
        return -1.0
    candidates.sort(key=lambda row: (row[0], row[1]))
    return float(candidates[0][1])


def _resolve_rakuten_variant_price_jpy(
    *,
    item_url: str,
    target_code: str,
    timeout: int,
) -> Tuple[float, Dict[str, Any]]:
    url = str(item_url or "").strip()
    code = str(target_code or "").strip().upper()
    if not url or not code:
        return -1.0, {"ok": False, "reason": "missing_url_or_code"}
    status, _headers, html = _request_text(url, timeout=max(5, int(timeout)))
    if status != 200 or not html:
        return -1.0, {"ok": False, "reason": f"http_{status if status else 'error'}"}
    price = _extract_rakuten_variant_price_from_html(html, target_code=code)
    if price <= 0:
        return -1.0, {"ok": False, "reason": "price_not_found"}
    return price, {"ok": True, "reason": "resolved", "target_code": code, "resolved_price_jpy": price}


def _is_ambiguous_model_title(title: str) -> bool:
    codes = _canonical_code_set(_extract_codes(title))
    if len(codes) <= 1:
        return False
    with_digit = [code for code in codes if any(ch.isdigit() for ch in code)]
    if len(with_digit) <= 1:
        return False
    # 複数型番を列挙したタイトルは同一商品判定の誤差が大きいため除外。
    return True


def _is_specific_model_code(code: str) -> bool:
    token = str(code or "").strip().upper()
    if not token:
        return False
    alpha = sum(1 for ch in token if "A" <= ch <= "Z")
    digit = sum(1 for ch in token if ch.isdigit())
    if alpha < 2 or digit < 2:
        return False
    return len(token) >= 6 or token.count("-") >= 1


def _query_specific_codes(query: str) -> set[str]:
    out: set[str] = set()
    for raw in _extract_codes(query):
        if not _is_specific_model_code(raw):
            continue
        canon = _canonicalize_code(raw)
        if canon:
            out.add(canon)
    return out


def _should_skip_model_backfill_for_query(query_specific_codes: set[str]) -> bool:
    if not query_specific_codes:
        return False
    return _env_bool("MINER_FETCH_MODEL_BACKFILL_SKIP_ON_SPECIFIC_QUERY", True)


def _is_related_model_code(candidate: str, query_code: str) -> bool:
    left = _canonicalize_code(candidate)
    right = _canonicalize_code(query_code)
    if not left or not right:
        return False
    if left == right:
        return True
    if left.startswith(right) or right.startswith(left):
        return True
    if min(len(left), len(right)) < 6:
        return False
    if left[:6] != right[:6]:
        return False
    left_digits = "".join(ch for ch in left if ch.isdigit())
    right_digits = "".join(ch for ch in right if ch.isdigit())
    if left_digits and right_digits and (
        left_digits != right_digits
        and (not left_digits.startswith(right_digits))
        and (not right_digits.startswith(left_digits))
    ):
        return False
    return True


def _filter_sold_first_codes_for_query(sold_first_codes: set[str], query_specific_codes: set[str]) -> set[str]:
    if not sold_first_codes or not query_specific_codes:
        return set(sold_first_codes or set())
    filtered: set[str] = set()
    for code in sold_first_codes:
        if any(_is_related_model_code(code, q) for q in query_specific_codes):
            filtered.add(code)
    return filtered if filtered else set(sold_first_codes)


def _liquidity_query_key(query: str) -> str:
    code = _canonicalize_code(query)
    if code:
        return code
    return _compact_query(query).upper()


def _build_sold_first_signal_lookup(signals_by_code: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(signals_by_code, dict):
        return out
    for key, signal in signals_by_code.items():
        if not isinstance(signal, dict):
            continue
        normalized = _liquidity_query_key(str(key or ""))
        if normalized:
            out[normalized] = signal
        alt_key = _liquidity_query_key(str(signal.get("query", "") or ""))
        if alt_key and alt_key not in out:
            out[alt_key] = signal
    return out


def _can_skip_source_fetch_after_preselection(
    *,
    sold_first_codes: set[str],
    jp_items: Sequence[MarketItem],
    cap_site: int,
    source_sites: Sequence[str],
) -> bool:
    if not sold_first_codes:
        return False
    normalized_sources = [str(v or "").strip().lower() for v in source_sites if str(v or "").strip()]
    if not normalized_sources:
        return False
    normalized_set = set(normalized_sources)
    target_min_items = max(1, int(cap_site)) * max(1, len(normalized_sources))
    if len(jp_items) < target_min_items:
        return False
    min_sites_required = max(
        1,
        min(
            len(normalized_sources),
            _env_int("MINER_FETCH_SOLD_FIRST_MIN_SOURCE_SITES_BEFORE_SKIP", 2),
        ),
    )
    covered_sites = {
        str(item.site or "").strip().lower()
        for item in jp_items
        if str(item.site or "").strip().lower() in normalized_set
    }
    return len(covered_sites) >= min_sites_required


def _item_model_code_keys(item: MarketItem) -> set[str]:
    keys = _canonical_code_set(_extract_codes(item.title))
    identifiers = item.identifiers if isinstance(item.identifiers, dict) else {}
    for name in ("model", "mpn"):
        raw = str(identifiers.get(name, "") or "").strip()
        if not raw:
            continue
        for token in _extract_codes(raw):
            canon = _canonicalize_code(token)
            if canon:
                keys.add(canon)
    return keys


def _filter_items_by_query_codes(
    *,
    items: Sequence[MarketItem],
    query_codes: set[str],
    allow_fallback_no_match: bool = True,
    allow_related_codes: bool = True,
) -> Tuple[List[MarketItem], Dict[str, Any]]:
    if not query_codes:
        return list(items), {"enabled": False, "applied": False}
    kept: List[MarketItem] = []
    dropped = 0
    for item in items:
        item_codes = _item_model_code_keys(item)
        has_exact = bool(item_codes and item_codes.intersection(query_codes))
        has_related = bool(
            allow_related_codes
            and item_codes
            and any(_is_related_model_code(code, q) for code in item_codes for q in query_codes)
        )
        if has_exact or has_related:
            kept.append(item)
            continue
        dropped += 1
    if kept:
        return kept, {
            "enabled": True,
            "applied": True,
            "query_codes": sorted(query_codes),
            "before": len(items),
            "after": len(kept),
            "dropped": dropped,
            "fallback_no_match": False,
        }
    if not allow_fallback_no_match:
        return [], {
            "enabled": True,
            "applied": True,
            "query_codes": sorted(query_codes),
            "before": len(items),
            "after": 0,
            "dropped": dropped,
            "fallback_no_match": False,
            "strict_drop_all": True,
        }
    # 取りこぼし回避: 全件除外になる場合はフィルタを外して従来挙動に戻す。
    return list(items), {
        "enabled": True,
        "applied": False,
        "query_codes": sorted(query_codes),
        "before": len(items),
        "after": len(items),
        "dropped": 0,
        "fallback_no_match": True,
    }


def _extract_color_tags(title: str) -> set[str]:
    normalized = unicodedata.normalize("NFKC", str(title or ""))
    upper = normalized.upper()
    tags: set[str] = set()
    for color, aliases in _COLOR_ALIASES.items():
        for alias in aliases:
            alias_text = str(alias or "")
            if not alias_text:
                continue
            if alias_text.isascii():
                if _contains_ascii_token(upper, alias_text):
                    tags.add(color)
                    break
            else:
                if alias_text in normalized:
                    tags.add(color)
                    break
    return tags


def _extract_variant_color_codes(title: str) -> set[str]:
    normalized = unicodedata.normalize("NFKC", str(title or ""))
    upper = normalized.upper()
    out: set[str] = set()
    for raw in re.findall(r"[\(\[\s/_-]([A-Z])[\)\]\s/_-]", f" {upper} "):
        code = str(raw or "").strip().upper()
        mapped = _VARIANT_COLOR_CODE_MAP.get(code)
        if mapped:
            out.add(mapped)
    return out


def _contains_mod_marker(title: str) -> bool:
    normalized = unicodedata.normalize("NFKC", str(title or ""))
    upper = f" {normalized.upper()} "
    for term in _MOD_TERMS:
        token = str(term or "")
        if not token:
            continue
        if token.isascii():
            if token in upper:
                return True
        else:
            if token in normalized:
                return True
    return False


def _extract_primary_families(title: str) -> set[str]:
    normalized = unicodedata.normalize("NFKC", str(title or ""))
    upper = normalized.upper()
    families: set[str] = set()
    for family, terms in _PRIMARY_FAMILY_TERMS.items():
        for term in terms:
            token = str(term or "")
            if not token:
                continue
            if token.isascii():
                if _contains_ascii_token(upper, token):
                    families.add(family)
                    break
            else:
                if token in normalized:
                    families.add(family)
                    break
    return families


def _bundle_mode(title: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(title or ""))
    upper = normalized.upper()
    has_bundle = False
    for token in _BODY_ONLY_TERMS:
        if token.isascii():
            if _contains_ascii_token(upper, token):
                break
        else:
            if token in normalized:
                break
    for token in _BUNDLE_TERMS:
        if token.isascii():
            if _contains_ascii_token(upper, token):
                has_bundle = True
                break
        else:
            if token in normalized:
                has_bundle = True
                break
    if has_bundle:
        return "bundle"
    for token in _BODY_ONLY_TERMS:
        if token.isascii():
            if _contains_ascii_token(upper, token):
                return "body_only"
        else:
            if token in normalized:
                return "body_only"
    return ""


def _has_near_specific_model_code(left_codes: set[str], right_codes: set[str]) -> bool:
    if not left_codes or not right_codes:
        return False
    for left in left_codes:
        if not left:
            continue
        for right in right_codes:
            if not right:
                continue
            if left == right:
                return True
            if min(len(left), len(right)) < 8:
                continue
            if left[:4] != right[:4]:
                continue
            left_digits = "".join(ch for ch in left if ch.isdigit())
            right_digits = "".join(ch for ch in right if ch.isdigit())
            if left_digits and right_digits and left_digits != right_digits:
                continue
            if abs(len(left) - len(right)) > 2:
                continue
            prefix_len = 0
            for lch, rch in zip(left, right):
                if lch != rch:
                    break
                prefix_len += 1
            if prefix_len >= max(6, min(len(left), len(right)) - 2):
                return True
    return False


def _recover_model_code_conflict(
    *,
    source: MarketItem,
    market: MarketItem,
    source_brands: set[str],
    market_brands: set[str],
    source_specific_canon: set[str],
    market_specific_canon: set[str],
    variant_missing_market: bool,
    variant_missing_source: bool,
    color_missing_market: bool,
) -> Optional[Tuple[float, str]]:
    if not _env_bool("MINER_MATCH_MODEL_CONFLICT_RECOVERY_ENABLED", True):
        return None

    source_token_list = _title_tokens(source.title)
    market_token_list = _title_tokens(market.title)
    if not source_token_list or not market_token_list:
        return None

    source_tokens = set(source_token_list)
    market_tokens = set(market_token_list)
    if not source_tokens or not market_tokens:
        return None

    common = source_tokens & market_tokens
    if not common:
        return None

    union = source_tokens | market_tokens
    jaccard = len(common) / len(union) if union else 0.0

    src_first = source_token_list[0] if source_token_list else ""
    mk_first = market_token_list[0] if market_token_list else ""
    first_token_match = bool(src_first and src_first == mk_first)
    brand_overlap = bool(source_brands and market_brands and source_brands.intersection(market_brands))
    near_code = _has_near_specific_model_code(source_specific_canon, market_specific_canon)
    source_families = _extract_primary_families(source.title)
    market_families = _extract_primary_families(market.title)
    family_overlap = bool(source_families and market_families and source_families.intersection(market_families))
    if not (brand_overlap or first_token_match or (near_code and family_overlap)):
        return None

    multi_code_side = len(source_specific_canon) >= 2 or len(market_specific_canon) >= 2

    if near_code:
        min_common = max(1, _env_int("MINER_MATCH_MODEL_CONFLICT_RECOVERY_MIN_COMMON_TOKENS_NEAR_CODE", 1))
        min_jaccard = _env_float("MINER_MATCH_MODEL_CONFLICT_RECOVERY_MIN_JACCARD_NEAR_CODE", 0.08)
        if len(common) < min_common or jaccard < max(0.0, min_jaccard):
            return None
        score = 0.78
        reason = "model_code_conflict_recovered_near_code"
    else:
        if not _env_bool("MINER_MATCH_MODEL_CONFLICT_RECOVERY_ALLOW_MULTI_CODE", True):
            return None
        if not multi_code_side:
            return None
        min_common = max(1, _env_int("MINER_MATCH_MODEL_CONFLICT_RECOVERY_MIN_COMMON_TOKENS", 4))
        min_jaccard = _env_float("MINER_MATCH_MODEL_CONFLICT_RECOVERY_MIN_JACCARD", 0.40)
        if len(common) < min_common or jaccard < max(0.0, min_jaccard):
            return None
        score = 0.76
        reason = "model_code_conflict_recovered_multi_code"

    if brand_overlap:
        score += 0.01
    if first_token_match:
        score += 0.01

    if variant_missing_market:
        score -= 0.03
        reason += "_variant_color_missing_market"
    elif variant_missing_source:
        score -= 0.03
        reason += "_variant_color_missing_source"
    elif color_missing_market:
        score -= 0.02
        reason += "_color_missing_market"

    min_score = _env_float("MINER_MATCH_MODEL_CONFLICT_RECOVERY_MIN_SCORE", 0.75)
    score = min(0.87, max(0.0, score))
    if score < max(0.0, min(1.0, min_score)):
        return None
    return score, reason


def _match_score(source: MarketItem, market: MarketItem) -> Tuple[float, str]:
    source_is_accessory = _is_accessory_title(source.title)
    market_is_accessory = _is_accessory_title(market.title)
    if source_is_accessory != market_is_accessory:
        return 0.0, "accessory_mismatch"

    source_families = _extract_primary_families(source.title)
    market_families = _extract_primary_families(market.title)
    if (source_families and market_families) and source_families.isdisjoint(market_families):
        return 0.0, "family_conflict"

    source_brands = _extract_known_brand_tags(source.title)
    market_brands = _extract_known_brand_tags(market.title)
    if (source_brands and market_brands) and source_brands.isdisjoint(market_brands):
        return 0.0, "brand_conflict"

    source_bundle = _bundle_mode(source.title)
    market_bundle = _bundle_mode(market.title)
    if source_bundle and market_bundle and source_bundle != market_bundle:
        return 0.0, "bundle_conflict"

    source_is_mod = _contains_mod_marker(source.title)
    market_is_mod = _contains_mod_marker(market.title)
    if source_is_mod != market_is_mod:
        return 0.0, "mod_conflict"

    source_colors = _extract_color_tags(source.title)
    market_colors = _extract_color_tags(market.title)
    source_variant_colors = _extract_variant_color_codes(source.title)
    market_variant_colors = _extract_variant_color_codes(market.title)
    variant_missing_market = bool(
        source_variant_colors and not market_variant_colors and source_variant_colors.isdisjoint(market_colors)
    )
    variant_missing_source = bool(
        market_variant_colors and not source_variant_colors and market_variant_colors.isdisjoint(source_colors)
    )
    color_missing_market = bool(source_colors and not market_colors)
    if source_variant_colors and market_variant_colors and source_variant_colors.isdisjoint(market_variant_colors):
        return 0.0, "variant_color_conflict"
    if source_colors and market_colors and source_colors.isdisjoint(market_colors):
        return 0.0, "color_conflict"

    source_ids = source.identifiers or {}
    market_ids = market.identifiers or {}
    allow_color_missing_with_identifier = _env_bool(
        "MINER_MATCH_ALLOW_COLOR_MISSING_WITH_IDENTIFIER",
        True,
    )
    allow_color_missing_with_model_code = _env_bool(
        "MINER_MATCH_ALLOW_COLOR_MISSING_WITH_MODEL_CODE",
        True,
    )
    for key in ("jan", "upc", "ean", "gtin"):
        left = str(source_ids.get(key, "") or "").strip()
        right = str(market_ids.get(key, "") or "").strip()
        if left and right and left == right:
            if allow_color_missing_with_identifier and variant_missing_market:
                return 0.96, f"{key}_exact_variant_color_missing_market"
            if allow_color_missing_with_identifier and variant_missing_source:
                return 0.95, f"{key}_exact_variant_color_missing_source"
            if allow_color_missing_with_identifier and color_missing_market:
                return 0.95, f"{key}_exact_color_missing_market"
            return 0.99, f"{key}_exact"

    source_codes = set(_extract_codes(source.title))
    market_codes = set(_extract_codes(market.title))
    source_codes_canon = _canonical_code_set(source_codes)
    market_codes_canon = _canonical_code_set(market_codes)
    same_codes = source_codes & market_codes
    same_codes_canon = source_codes_canon & market_codes_canon
    source_specific: set[str] = set()
    market_specific: set[str] = set()
    source_specific_canon: set[str] = set()
    market_specific_canon: set[str] = set()
    same_specific: set[str] = set()
    if source_codes and market_codes:
        source_specific = {code for code in source_codes if _is_specific_model_code(code)}
        market_specific = {code for code in market_codes if _is_specific_model_code(code)}
        source_specific_canon = _canonical_code_set(source_specific)
        market_specific_canon = _canonical_code_set(market_specific)
        same_specific = source_specific_canon & market_specific_canon
        if source_specific and market_specific and not same_specific:
            recovered = _recover_model_code_conflict(
                source=source,
                market=market,
                source_brands=source_brands,
                market_brands=market_brands,
                source_specific_canon=source_specific_canon,
                market_specific_canon=market_specific_canon,
                variant_missing_market=variant_missing_market,
                variant_missing_source=variant_missing_source,
                color_missing_market=color_missing_market,
            )
            if recovered is not None:
                return recovered
            return 0.12, "model_code_conflict"
    if same_codes:
        if len(same_codes) == 1:
            only = next(iter(same_codes))
            # 例: GW-5000 のようなベース型番一致だけでは枝番違いを誤検知しやすい。
            if (not _is_specific_model_code(only)) and source_codes != market_codes:
                return 0.18, "model_code_partial"
        boost = min(0.07, 0.02 * len(same_codes))
        if variant_missing_market:
            if allow_color_missing_with_model_code and same_specific:
                return min(0.89, 0.84 + boost), "model_code_variant_color_missing_market"
            return 0.16, "variant_color_missing_market"
        if variant_missing_source:
            if allow_color_missing_with_model_code and same_specific:
                return min(0.88, 0.83 + boost), "model_code_variant_color_missing_source"
            return 0.24, "variant_color_missing_source"
        if color_missing_market:
            if allow_color_missing_with_model_code and same_specific:
                return min(0.87, 0.82 + boost), "model_code_color_missing_market"
            return 0.16, "color_missing_market"
        return min(0.97, 0.90 + boost), "model_code"
    if same_codes_canon:
        # 例: SBDC-101 と SBDC101 のような表記ゆれを同一視する。
        boost = min(0.06, 0.02 * len(same_codes_canon))
        same_specific_canon = source_specific_canon & market_specific_canon
        if variant_missing_market:
            if allow_color_missing_with_model_code and same_specific_canon:
                return min(0.87, 0.82 + boost), "model_code_normalized_variant_color_missing_market"
            return 0.16, "variant_color_missing_market"
        if variant_missing_source:
            if allow_color_missing_with_model_code and same_specific_canon:
                return min(0.86, 0.81 + boost), "model_code_normalized_variant_color_missing_source"
            return 0.24, "variant_color_missing_source"
        if color_missing_market:
            if allow_color_missing_with_model_code and same_specific_canon:
                return min(0.85, 0.80 + boost), "model_code_normalized_color_missing_market"
            return 0.16, "color_missing_market"
        return min(0.95, 0.88 + boost), "model_code_normalized"
    if source_codes and market_codes:
        recovered = _recover_model_code_conflict(
            source=source,
            market=market,
            source_brands=source_brands,
            market_brands=market_brands,
            source_specific_canon=source_specific_canon,
            market_specific_canon=market_specific_canon,
            variant_missing_market=variant_missing_market,
            variant_missing_source=variant_missing_source,
            color_missing_market=color_missing_market,
        )
        if recovered is not None:
            return recovered
        return 0.12, "model_code_conflict"
    soft_color_reason = ""
    soft_color_penalty = 0.0
    if variant_missing_market:
        soft_color_reason = "variant_color_missing_market"
        soft_color_penalty = 0.04
    elif variant_missing_source:
        soft_color_reason = "variant_color_missing_source"
        soft_color_penalty = 0.04
    elif color_missing_market:
        soft_color_reason = "color_missing_market"
        soft_color_penalty = 0.02

    source_token_list = _title_tokens(source.title)
    market_token_list = _title_tokens(market.title)
    source_tokens = set(source_token_list)
    market_tokens = set(market_token_list)
    if not source_tokens or not market_tokens:
        return 0.0, "insufficient_tokens"
    common = source_tokens & market_tokens
    union = source_tokens | market_tokens
    jaccard = len(common) / len(union) if union else 0.0
    if jaccard <= 0:
        return 0.0, "token_overlap"

    src_first = source_token_list[0] if source_token_list else ""
    mk_first = market_token_list[0] if market_token_list else ""
    brand_bonus = 0.10 if src_first and src_first == mk_first else 0.0
    overlap_bonus = 0.06 if len(common) >= 3 else 0.0
    score = min(0.89, jaccard * 0.76 + brand_bonus + overlap_bonus)
    if soft_color_penalty > 0:
        score = max(0.0, score - soft_color_penalty)
    if soft_color_reason:
        return score, soft_color_reason
    return score, "token_overlap"


def _match_level(score: float, reason: str) -> str:
    if reason.endswith("_exact"):
        return "L1_identifier"
    if score >= 0.78:
        return "L2_precise"
    if score >= 0.58:
        return "L3_mid"
    return "L4_broad"


def _candidate_group_key(source: MarketItem, market: MarketItem) -> str:
    source_families = sorted(_extract_primary_families(source.title))
    market_families = sorted(_extract_primary_families(market.title))
    common_families = [v for v in source_families if v in set(market_families)]
    family = common_families[0] if common_families else (source_families[0] if source_families else "")

    source_codes = sorted(_extract_codes(source.title))
    market_codes = sorted(_extract_codes(market.title))
    source_code_map = {_canonicalize_code(v): v for v in source_codes}
    market_code_map = {_canonicalize_code(v): v for v in market_codes}
    common_code_keys = [v for v in source_code_map.keys() if v and v in set(market_code_map.keys())]
    model = common_code_keys[0] if common_code_keys else _canonicalize_code(source_codes[0] if source_codes else "")

    source_colors = sorted(_extract_color_tags(source.title) | _extract_variant_color_codes(source.title))
    market_colors = sorted(_extract_color_tags(market.title) | _extract_variant_color_codes(market.title))
    common_colors = [v for v in source_colors if v in set(market_colors)]
    color = common_colors[0] if common_colors else (source_colors[0] if source_colors else "")
    if not color and market_colors:
        color = market_colors[0]

    return f"{family}|{model}|{color}"


def _candidate_model_codes(source: MarketItem, market: MarketItem) -> List[str]:
    source_codes = _canonical_code_set(_extract_codes(source.title))
    market_codes = _canonical_code_set(_extract_codes(market.title))
    common = sorted(source_codes & market_codes)
    if common:
        return common[:4]
    source_only = sorted(source_codes)
    if source_only:
        return source_only[:4]
    return []


def _preferred_liquidity_query(
    *,
    source: MarketItem,
    market: MarketItem,
    base_query: str,
    preferred_codes: Optional[set[str]] = None,
) -> str:
    candidate_codes = _candidate_model_codes(source, market)
    if preferred_codes:
        for code in candidate_codes:
            if code in preferred_codes:
                return code
        union_codes = _item_model_code_keys(source) | _item_model_code_keys(market)
        preferred_union = sorted(union_codes & preferred_codes)
        if preferred_union:
            return preferred_union[0]
    if candidate_codes:
        return candidate_codes[0]
    return str(base_query or "").strip()


def _liquidity_query_matches_pair(
    *,
    query: str,
    source: MarketItem,
    market: MarketItem,
) -> bool:
    query_code = _canonicalize_code(query)
    # 非型番クエリ（ブランド/カテゴリ語）は一致判定対象外。
    if not query_code or not _is_specific_model_code(query_code):
        return True
    pair_codes = _item_model_code_keys(source) | _item_model_code_keys(market)
    if not pair_codes:
        return False
    for code in pair_codes:
        if _is_related_model_code(code, query_code) or _is_related_model_code(query_code, code):
            return True
    return False


def _analyze_candidate_matches(
    *,
    jp_items: Sequence[MarketItem],
    ebay_items: Sequence[MarketItem],
    min_score: float,
    allow_ambiguous_codes: Optional[set[str]] = None,
) -> Dict[str, Any]:
    candidate_matches: List[Dict[str, Any]] = []
    skipped_low_match = 0
    skipped_ambiguous_model_title = 0
    low_match_reason_counts: Dict[str, int] = {}
    low_match_samples: List[Dict[str, Any]] = []

    for source in jp_items:
        if _is_ambiguous_model_title(source.title):
            source_codes = _canonical_code_set(_extract_codes(source.title))
            if not (allow_ambiguous_codes and source_codes.intersection(allow_ambiguous_codes)):
                skipped_ambiguous_model_title += 1
                continue
        best: Optional[Tuple[float, str, MarketItem]] = None
        for market in ebay_items:
            score, reason = _match_score(source, market)
            if best is None or score > best[0]:
                best = (score, reason, market)
        if best is None or best[0] < min_score:
            skipped_low_match += 1
            reason_key = str(best[1] if best is not None else "no_market_match")
            low_match_reason_counts[reason_key] = int(low_match_reason_counts.get(reason_key, 0)) + 1
            if len(low_match_samples) < 8:
                low_match_samples.append(
                    {
                        "source_title": source.title,
                        "best_market_title": best[2].title if best is not None else "",
                        "score": round(float(best[0]), 4) if best is not None else 0.0,
                        "reason": reason_key,
                    }
                )
            continue
        candidate_matches.append(
            {
                "score": best[0],
                "reason": best[1],
                "source": source,
                "market": best[2],
            }
        )

    return {
        "candidate_matches": candidate_matches,
        "skipped_low_match": skipped_low_match,
        "skipped_ambiguous_model_title": skipped_ambiguous_model_title,
        "low_match_reason_counts": low_match_reason_counts,
        "low_match_samples": low_match_samples,
    }


def _collect_source_model_code_queries(
    source_items: Sequence[MarketItem],
    *,
    base_query: str,
    max_queries: int,
) -> List[str]:
    max_q = max(1, int(max_queries))
    brand = _pick_brand_token(_query_tokens(base_query))
    stats: Dict[str, Dict[str, Any]] = {}
    for item in source_items:
        item_price = _to_float(item.price, 0.0)
        for code in set(_extract_codes(item.title)):
            canon_probe = _canonicalize_code(code)
            is_modelish = _is_specific_model_code(code) or (
                len(canon_probe) >= 6
                and any(ch.isalpha() for ch in canon_probe)
                and any(ch.isdigit() for ch in canon_probe)
            )
            if not is_modelish:
                continue
            canon = _canonicalize_code(code)
            if not canon:
                continue
            row = stats.get(canon)
            if not isinstance(row, dict):
                stats[canon] = {
                    "code": str(code).strip().upper(),
                    "count": 1,
                    "min_price": item_price if item_price > 0 else 10**9,
                }
                continue
            row["count"] = int(row.get("count", 0)) + 1
            if item_price > 0:
                row["min_price"] = min(_to_float(row.get("min_price"), 10**9), item_price)

    ranked = sorted(
        stats.values(),
        key=lambda row: (
            -_to_int(row.get("count"), 0),
            _to_float(row.get("min_price"), 10**9),
            -len(str(row.get("code", ""))),
        ),
    )
    queries: List[str] = []
    seen: set[str] = set()
    for row in ranked:
        code = str(row.get("code", "") or "").strip().upper()
        if not code:
            continue
        candidates = [code]
        if brand and brand not in code:
            candidates.insert(0, _compact_query(f"{brand} {code}"))
        for candidate in candidates:
            q = _compact_query(candidate)
            if not q:
                continue
            key = q.upper()
            if key in seen:
                continue
            seen.add(key)
            queries.append(q)
            if len(queries) >= max_q:
                return queries
    return queries


def _fetch_ebay_model_backfill(
    *,
    source_items: Sequence[MarketItem],
    base_query: str,
    timeout: int,
    cap_site: int,
) -> Tuple[List[MarketItem], Dict[str, Any]]:
    enabled = _env_bool("MINER_FETCH_MODEL_BACKFILL_ENABLED", True)
    max_queries = max(1, min(10, _env_int("MINER_FETCH_MODEL_BACKFILL_MAX_QUERIES", 4)))
    queries = _collect_source_model_code_queries(source_items, base_query=base_query, max_queries=max_queries)
    if not enabled:
        return [], {"enabled": False, "ran": False, "reason": "disabled", "queries": queries}
    if not queries:
        return [], {"enabled": True, "ran": False, "reason": "no_model_queries", "queries": []}

    max_calls = max(1, min(10, _env_int("MINER_FETCH_MODEL_BACKFILL_MAX_CALLS", 4)))
    per_query_cap = max(5, min(80, _env_int("MINER_FETCH_MODEL_BACKFILL_CAP_SITE", min(20, cap_site))))
    target_items = max(10, min(240, _env_int("MINER_FETCH_MODEL_BACKFILL_TARGET_ITEMS", 80)))
    merged: List[MarketItem] = []
    seen_items: set[Tuple[str, str]] = set()
    fetch_logs: List[Dict[str, Any]] = []
    errors: List[str] = []

    for query in queries[:max_calls]:
        rows, info, fetch_error = _fetch_site_items_adaptive(
            site="ebay",
            query=query,
            cap_site=per_query_cap,
            timeout=timeout,
            require_in_stock=True,
        )
        if fetch_error:
            errors.append(f"{query}: {fetch_error}")
        before = len(merged)
        for item in rows:
            ident = _item_identity(item)
            if ident in seen_items:
                continue
            seen_items.add(ident)
            merged.append(item)
        fetch_logs.append(
            {
                "query": query,
                "count": len(rows),
                "new_count": len(merged) - before,
                "stop_reason": str(info.get("stop_reason", "") or "") if isinstance(info, dict) else "",
                "calls_made": _to_int((info or {}).get("calls_made"), 0),
            }
        )
        if len(merged) >= target_items:
            break

    return merged, {
        "enabled": True,
        "ran": True,
        "reason": "ok" if not errors else "partial_error",
        "queries": queries[:max_calls],
        "query_count": len(queries[:max_calls]),
        "cap_site": per_query_cap,
        "target_items": target_items,
        "added_items": len(merged),
        "errors": errors,
        "logs": fetch_logs,
    }


def _fetch_source_model_backfill_from_market(
    *,
    market_items: Sequence[MarketItem],
    source_sites: Sequence[str],
    timeout: int,
    cap_site: int,
    require_in_stock: bool,
) -> Tuple[List[MarketItem], Dict[str, Any]]:
    enabled = _env_bool("MINER_FETCH_SOURCE_MODEL_BACKFILL_ENABLED", True)
    max_codes = max(1, min(8, _env_int("MINER_FETCH_SOURCE_MODEL_BACKFILL_MAX_CODES", 3)))
    max_items = max(10, min(120, _env_int("MINER_FETCH_SOURCE_MODEL_BACKFILL_MAX_ITEMS", 50)))
    summary: Dict[str, Any] = {
        "enabled": bool(enabled),
        "ran": False,
        "reason": "",
        "model_code_count": 0,
        "queries": [],
        "added_items": 0,
        "errors": [],
    }
    if not enabled:
        summary["reason"] = "disabled"
        return [], summary

    code_rows: Dict[str, Dict[str, Any]] = {}
    for item in market_items:
        for raw_code in _extract_codes(item.title):
            code = str(raw_code or "").strip().upper()
            if not _is_specific_model_code(code):
                continue
            canon = _canonicalize_code(code)
            if not canon:
                continue
            row = code_rows.get(canon)
            if not isinstance(row, dict):
                row = {"code": code, "count": 0}
                code_rows[canon] = row
            row["count"] = _to_int(row.get("count"), 0) + 1
            best = str(row.get("code", "") or "")
            if len(code) > len(best):
                row["code"] = code

    ranked_codes = [
        str(row.get("code", "") or "").strip().upper()
        for _, row in sorted(
            code_rows.items(),
            key=lambda kv: (
                -_to_int((kv[1] or {}).get("count"), 0),
                -len(str((kv[1] or {}).get("code", "") or "")),
                str((kv[1] or {}).get("code", "") or ""),
            ),
        )
    ]
    ranked_codes = [code for code in ranked_codes if code][:max_codes]
    summary["model_code_count"] = len(ranked_codes)
    if not ranked_codes:
        summary["reason"] = "no_model_codes"
        return [], summary

    fetchers: Dict[str, Callable[[str, int, int, int, bool], Tuple[List[MarketItem], Dict[str, Any]]]] = {
        "rakuten": _search_rakuten,
        "yahoo": _search_yahoo,
    }
    normalized_sites = [str(v or "").strip().lower() for v in source_sites if str(v or "").strip()]
    normalized_sites = [site for site in normalized_sites if site in fetchers]
    if not normalized_sites:
        summary["reason"] = "no_supported_source_sites"
        return [], summary

    out: List[MarketItem] = []
    seen: set[Tuple[str, str]] = set()
    per_call_limit = max(1, min(30, cap_site))
    for site in normalized_sites:
        fetcher = fetchers[site]
        for code in ranked_codes:
            if len(out) >= max_items:
                break
            entry: Dict[str, Any] = {"site": site, "query": code}
            try:
                rows, info = fetcher(code, per_call_limit, timeout, 1, require_in_stock)
                entry["ok"] = True
                entry["count"] = len(rows)
                entry["info"] = {
                    "http": _to_int((info or {}).get("http"), -1),
                    "raw_total": _to_int((info or {}).get("raw_total"), -1),
                    "cache_hit": bool((info or {}).get("cache_hit")),
                    "budget_remaining": _to_int((info or {}).get("budget_remaining"), -1),
                }
                for item in rows:
                    ident = _item_identity(item)
                    if ident in seen:
                        continue
                    seen.add(ident)
                    out.append(item)
                    if len(out) >= max_items:
                        break
            except Exception as err:
                entry["ok"] = False
                entry["error"] = str(err)
                errors = summary.get("errors")
                if isinstance(errors, list):
                    errors.append({"site": site, "query": code, "message": str(err)})
            queries = summary.get("queries")
            if isinstance(queries, list):
                queries.append(entry)
        if len(out) >= max_items:
            break

    summary["ran"] = True
    summary["reason"] = "ok"
    summary["added_items"] = len(out)
    return out, summary


def _existing_pairs(settings: Settings) -> set[Tuple[str, str, str, str]]:
    existing: set[Tuple[str, str, str, str]] = set()
    with connect(settings.db_path) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT source_site, source_item_id, market_site, market_item_id
            FROM miner_candidates
            WHERE source_item_id IS NOT NULL
              AND market_item_id IS NOT NULL
              AND TRIM(source_item_id) <> ''
              AND TRIM(market_item_id) <> ''
            """
        ).fetchall()
        for row in rows:
            existing.add(
                (
                    str(row["source_site"]),
                    str(row["source_item_id"]),
                    str(row["market_site"]),
                    str(row["market_item_id"]),
                )
            )
    return existing


def _existing_pair_signatures(settings: Settings) -> set[str]:
    signatures: set[str] = set()
    with connect(settings.db_path) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT metadata_json
            FROM miner_candidates
            WHERE metadata_json IS NOT NULL
              AND TRIM(metadata_json) <> ''
            """
        ).fetchall()
        for row in rows:
            raw = str(row["metadata_json"] or "").strip()
            if not raw:
                continue
            try:
                metadata = json.loads(raw)
            except Exception:
                continue
            if not isinstance(metadata, dict):
                continue
            sig = str(metadata.get("pair_signature", "") or "").strip()
            if sig:
                signatures.add(sig)
    return signatures


def fetch_live_miner_candidates(
    *,
    query: str,
    source_sites: Sequence[str] | None = None,
    market_site: str = "ebay",
    limit_per_site: int = 20,
    max_candidates: int = 20,
    min_match_score: float = 0.75,
    min_profit_usd: float = 0.01,
    min_margin_rate: float = 0.03,
    require_in_stock: bool = True,
    timeout: int = 18,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    text_query = str(query or "").strip()
    if not text_query:
        raise ValueError("query is required")
    market_site = str(market_site or "ebay").strip().lower()
    if market_site != "ebay":
        raise ValueError("market_site currently supports only 'ebay'")

    requested_sources = [str(v).strip().lower() for v in (source_sites or ["rakuten", "yahoo"]) if str(v).strip()]
    normalized_sources: List[str] = []
    for raw in requested_sources:
        if raw in {"rakuten"}:
            normalized_sources.append("rakuten")
        elif raw in {"yahoo", "yahoo_shopping"}:
            normalized_sources.append("yahoo")
    normalized_sources = sorted(set(normalized_sources))
    if not normalized_sources:
        raise ValueError("source_sites must include at least one of rakuten,yahoo")

    cap_site = max(1, min(30, int(limit_per_site)))
    cap_candidates = max(1, min(50, int(max_candidates)))
    min_score = max(0.0, min(0.99, float(min_match_score)))
    min_profit = float(min_profit_usd)
    min_margin = float(min_margin_rate)
    require_in_stock_flag = bool(require_in_stock)
    applied_filters = {
        "condition": "new",
        "require_in_stock": require_in_stock_flag,
        "liquidity_require_signal": _env_bool("LIQUIDITY_REQUIRE_SIGNAL", True),
    }
    strict_sold_min_basis = _env_bool("LIQUIDITY_STRICT_SOLD_MIN_BASIS", True)
    applied_filters["strict_sold_min_basis"] = bool(strict_sold_min_basis)
    blocked_pair_signatures = _load_blocked_pair_signatures()
    liquidity_gate_enabled = _env_bool("LIQUIDITY_GATE_ENABLED", True)
    # 90日売却データ未取得を通さないことをデフォルトにする。
    liquidity_require_signal = _env_bool("LIQUIDITY_REQUIRE_SIGNAL", True)
    liquidity_mode = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    rpa_refresh_enabled = (
        liquidity_mode in {"rpa", "rpa_json"}
        and _env_bool("LIQUIDITY_RPA_AUTO_REFRESH", True)
        and _env_bool("LIQUIDITY_RPA_RUN_ON_FETCH", True)
    )
    rpa_refresh_summary: Dict[str, Any] = {
        "enabled": bool(rpa_refresh_enabled),
        "mode": liquidity_mode,
        "ran": False,
        "reason": "not_started",
        "queries": [],
    }
    liquidity_min_sold_90d = max(0, _env_int("LIQUIDITY_MIN_SOLD_90D", 3))
    liquidity_min_sell_through_90d = max(0.0, min(1.0, _env_float("LIQUIDITY_MIN_SELL_THROUGH_90D", 0.15)))
    est_intl_shipping = _to_float(os.getenv("EST_INTL_SHIPPING_USD"), 18.0)
    est_customs = _to_float(os.getenv("EST_CUSTOMS_USD"), 0.0)
    est_packaging = _to_float(os.getenv("EST_PACKAGING_USD"), 0.0)
    marketplace_fee_rate = _to_float(os.getenv("MARKETPLACE_FEE_RATE"), 0.13)
    payment_fee_rate = _to_float(os.getenv("PAYMENT_FEE_RATE"), 0.03)
    fixed_fee_usd = _to_float(os.getenv("FIXED_FEE_USD"), 0.0)
    query_skip_ttl_sec = max(0, _env_int("MINER_QUERY_SKIP_TTL_SECONDS", 1800))
    query_skip_ttl_done_sec = max(
        query_skip_ttl_sec,
        _env_int("MINER_QUERY_SKIP_TTL_DONE_SECONDS", 21600),
    )
    query_skip_ttl_no_gain_sec = max(
        query_skip_ttl_sec,
        _env_int("MINER_QUERY_SKIP_TTL_NO_GAIN_SECONDS", 3600),
    )
    query_skip_disabled = _env_bool("MINER_QUERY_SKIP_DISABLED", False)
    query_skip_key = _query_skip_key(
        query=text_query,
        market_site=market_site,
        source_sites=normalized_sources,
        limit_per_site=cap_site,
        max_candidates=cap_candidates,
        min_match_score=min_score,
        min_profit_usd=min_profit,
        min_margin_rate=min_margin,
        require_in_stock=require_in_stock_flag,
    )
    query_skip_entries = _load_query_skip_entries() if query_skip_ttl_sec > 0 else {}

    if (not query_skip_disabled) and query_skip_ttl_sec > 0:
        row = query_skip_entries.get(query_skip_key)
        if isinstance(row, dict):
            expires_at = _to_int(row.get("expires_at"), 0)
            now_ts = int(time.time())
            if (
                expires_at > now_ts
                and _to_int(row.get("created_count"), -1) == 0
                and (bool(row.get("search_scope_done")) or bool(row.get("no_gain_stop")))
            ):
                ttl_remaining = max(0, expires_at - now_ts)
                return {
                    "query": text_query,
                    "market_site": market_site,
                    "source_sites": normalized_sources,
                    "fetched": row.get("fetched", {}) if isinstance(row.get("fetched"), dict) else {},
                    "created_count": 0,
                    "created_ids": [],
                    "created": [],
                    "errors": row.get("errors", []) if isinstance(row.get("errors"), list) else [],
                    "skipped_duplicates": _to_int(row.get("skipped_duplicates"), 0),
                    "skipped_low_match": _to_int(row.get("skipped_low_match"), 0),
                    "skipped_invalid_price": _to_int(row.get("skipped_invalid_price"), 0),
                    "skipped_unprofitable": _to_int(row.get("skipped_unprofitable"), 0),
                    "skipped_low_margin": _to_int(row.get("skipped_low_margin"), 0),
                    "skipped_low_ev90": _to_int(row.get("skipped_low_ev90"), 0),
                    "skipped_low_liquidity": _to_int(row.get("skipped_low_liquidity"), 0),
                    "skipped_liquidity_query_mismatch": _to_int(row.get("skipped_liquidity_query_mismatch"), 0),
                    "skipped_unreliable_liquidity_signal": _to_int(row.get("skipped_unreliable_liquidity_signal"), 0),
                    "skipped_liquidity_unavailable": _to_int(row.get("skipped_liquidity_unavailable"), 0),
                    "skipped_blocked": _to_int(row.get("skipped_blocked"), 0),
                    "skipped_group_cap": _to_int(row.get("skipped_group_cap"), 0),
                    "skipped_ambiguous_model_title": _to_int(row.get("skipped_ambiguous_model_title"), 0),
                    "liquidity_unavailable_model_codes": row.get("liquidity_unavailable_model_codes", [])
                    if isinstance(row.get("liquidity_unavailable_model_codes"), list)
                    else [],
                    "low_match_reason_counts": row.get("low_match_reason_counts", {})
                    if isinstance(row.get("low_match_reason_counts"), dict)
                    else {},
                    "low_match_samples": row.get("low_match_samples", [])
                    if isinstance(row.get("low_match_samples"), list)
                    else [],
                    "search_scope_done": bool(row.get("search_scope_done")),
                    "hints": row.get("hints", []) if isinstance(row.get("hints"), list) else [],
                    "applied_filters": row.get("applied_filters", applied_filters)
                    if isinstance(row.get("applied_filters"), dict)
                    else dict(applied_filters),
                    "liquidity_rpa_refresh": rpa_refresh_summary,
                    "query_cache_skip": True,
                    "query_cache_ttl_sec": ttl_remaining,
                    "query_cache_saved_at": _epoch_to_iso(_to_int(row.get("updated_at"), 0)),
                    "query_cache_expires_at": _epoch_to_iso(expires_at),
                }

    def _build_rpa_daily_limit_result(
        *,
        fetched_payload: Optional[Dict[str, Any]] = None,
        errors_payload: Optional[List[Dict[str, str]]] = None,
        hints_extra: Optional[List[str]] = None,
        search_scope_done: bool = False,
    ) -> Dict[str, Any]:
        hints: List[str] = [
            "Product Researchの日次上限に到達したため、探索を停止しました。",
            "この上限は翌日にリセットされます。時間を空けて再実行してください。",
        ]
        if isinstance(hints_extra, list):
            for row in hints_extra:
                text = str(row or "").strip()
                if text:
                    hints.append(text)
        return {
            "query": text_query,
            "market_site": market_site,
            "source_sites": normalized_sources,
            "fetched": fetched_payload if isinstance(fetched_payload, dict) else {},
            "created_count": 0,
            "created_ids": [],
            "created": [],
            "errors": errors_payload if isinstance(errors_payload, list) else [],
            "skipped_duplicates": 0,
            "skipped_low_match": 0,
            "skipped_invalid_price": 0,
            "skipped_unprofitable": 0,
            "skipped_low_margin": 0,
            "skipped_missing_sold_min": 0,
            "skipped_non_min_basis": 0,
            "skipped_missing_sold_sample": 0,
            "skipped_below_sold_min": 0,
            "skipped_implausible_sold_min": 0,
            "skipped_source_variant_unresolved": 0,
            "skipped_low_ev90": 0,
            "skipped_low_liquidity": 0,
            "skipped_liquidity_query_mismatch": 0,
            "skipped_unreliable_liquidity_signal": 0,
            "skipped_liquidity_unavailable": 0,
            "skipped_blocked": 0,
            "skipped_group_cap": 0,
            "skipped_ambiguous_model_title": 0,
            "liquidity_unavailable_model_codes": [],
            "low_match_reason_counts": {},
            "low_match_samples": [],
            "search_scope_done": bool(search_scope_done),
            "hints": hints,
            "applied_filters": applied_filters,
            "liquidity_rpa_refresh": rpa_refresh_summary,
            "query_cache_skip": False,
            "query_cache_ttl_sec": 0,
            "rpa_daily_limit_reached": True,
        }

    def _finalize(result: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(result, dict):
            result.setdefault("liquidity_rpa_refresh", rpa_refresh_summary)
            daily_limit = _rpa_daily_limit_reached(result.get("liquidity_rpa_refresh"))
            result["rpa_daily_limit_reached"] = bool(daily_limit or bool(result.get("rpa_daily_limit_reached")))
            if result["rpa_daily_limit_reached"]:
                hints_row = result.get("hints") if isinstance(result.get("hints"), list) else []
                if not any("日次上限" in str(v or "") for v in hints_row):
                    hints_row.insert(0, "Product Researchの日次上限に到達したため、探索を停止しました。")
                result["hints"] = hints_row
        if query_skip_ttl_sec <= 0 or query_skip_disabled:
            if isinstance(result, dict):
                result.setdefault("query_cache_saved_at", "")
                result.setdefault("query_cache_expires_at", "")
            return result
        if not isinstance(result, dict):
            return result
        result.setdefault("applied_filters", dict(applied_filters))
        now_ts = int(time.time())
        created_count = _to_int(result.get("created_count"), 0)
        no_gain_stop = bool(_is_short_term_no_gain_result(result))
        if created_count == 0 and bool(result.get("search_scope_done")):
            effective_ttl = query_skip_ttl_done_sec
        elif created_count == 0 and no_gain_stop:
            effective_ttl = query_skip_ttl_no_gain_sec
        else:
            effective_ttl = query_skip_ttl_sec
        expires_at = now_ts + max(0, int(effective_ttl))
        query_skip_entries[query_skip_key] = {
            "updated_at": now_ts,
            "expires_at": expires_at,
            "created_count": _to_int(result.get("created_count"), 0),
            "search_scope_done": bool(result.get("search_scope_done")),
            "no_gain_stop": no_gain_stop,
            "fetched": result.get("fetched", {}) if isinstance(result.get("fetched"), dict) else {},
            "errors": result.get("errors", []) if isinstance(result.get("errors"), list) else [],
            "hints": result.get("hints", []) if isinstance(result.get("hints"), list) else [],
            "applied_filters": result.get("applied_filters", {})
            if isinstance(result.get("applied_filters"), dict)
            else dict(applied_filters),
            "skipped_duplicates": _to_int(result.get("skipped_duplicates"), 0),
            "skipped_low_match": _to_int(result.get("skipped_low_match"), 0),
            "skipped_invalid_price": _to_int(result.get("skipped_invalid_price"), 0),
            "skipped_unprofitable": _to_int(result.get("skipped_unprofitable"), 0),
            "skipped_low_margin": _to_int(result.get("skipped_low_margin"), 0),
            "skipped_low_ev90": _to_int(result.get("skipped_low_ev90"), 0),
            "skipped_low_liquidity": _to_int(result.get("skipped_low_liquidity"), 0),
            "skipped_liquidity_query_mismatch": _to_int(result.get("skipped_liquidity_query_mismatch"), 0),
            "skipped_unreliable_liquidity_signal": _to_int(result.get("skipped_unreliable_liquidity_signal"), 0),
            "skipped_liquidity_unavailable": _to_int(result.get("skipped_liquidity_unavailable"), 0),
            "skipped_source_variant_unresolved": _to_int(result.get("skipped_source_variant_unresolved"), 0),
            "skipped_blocked": _to_int(result.get("skipped_blocked"), 0),
            "skipped_group_cap": _to_int(result.get("skipped_group_cap"), 0),
            "skipped_ambiguous_model_title": _to_int(result.get("skipped_ambiguous_model_title"), 0),
            "liquidity_unavailable_model_codes": result.get("liquidity_unavailable_model_codes", [])
            if isinstance(result.get("liquidity_unavailable_model_codes"), list)
            else [],
            "low_match_reason_counts": result.get("low_match_reason_counts", {})
            if isinstance(result.get("low_match_reason_counts"), dict)
            else {},
            "low_match_samples": result.get("low_match_samples", [])
            if isinstance(result.get("low_match_samples"), list)
            else [],
        }
        _save_query_skip_entries(query_skip_entries)
        result["query_cache_saved_at"] = _epoch_to_iso(now_ts)
        result["query_cache_expires_at"] = _epoch_to_iso(expires_at)
        return result

    fetched: Dict[str, Dict[str, Any]] = {}
    errors: List[Dict[str, str]] = []
    jp_items: List[MarketItem] = []
    # 完走は厳格に「探索候補を最後まで消化した」時のみ。
    # max_calls_reached / target_reached / low_yield_stop は途中打ち切りの可能性がある。
    query_specific_codes = _query_specific_codes(text_query)
    strict_query_code_match = bool(
        _env_bool("MINER_FETCH_FORCE_EXACT_FOR_MODEL_QUERY", True) and query_specific_codes
    )
    sold_first_required = _env_bool("MINER_FETCH_EBAY_SOLD_FIRST_REQUIRED", False)

    ebay_items, ebay_info, ebay_error = _fetch_site_items_adaptive(
        site="ebay",
        query=text_query,
        cap_site=cap_site,
        timeout=timeout,
        require_in_stock=True,
    )
    ebay_items, ebay_query_code_filter = _filter_items_by_query_codes(
        items=ebay_items,
        query_codes=query_specific_codes,
        allow_fallback_no_match=(not strict_query_code_match),
    )
    if isinstance(ebay_info, dict):
        ebay_info["query_model_code_filter"] = ebay_query_code_filter
    fetched["ebay"] = ebay_info
    if ebay_error:
        errors.append({"site": "ebay", "message": ebay_error})

    # eBay候補が0件の場合はマッチング不能なので、日本側の追加取得を省略してAPIを節約する。
    if not ebay_items:
        if rpa_refresh_enabled:
            rpa_refresh_summary = _maybe_refresh_rpa_for_fetch([text_query], force=False)
            if _rpa_daily_limit_reached(rpa_refresh_summary):
                return _finalize(
                    _build_rpa_daily_limit_result(
                        fetched_payload=fetched,
                        errors_payload=errors,
                        hints_extra=["eBay側ヒット0のためのRPA補完取得中に上限へ到達しました。"],
                    )
                )
        for source in normalized_sources:
            fetched[source] = {
                "ok": True,
                "count": 0,
                "calls_made": 0,
                "cache_hits": 0,
                "network_calls": 0,
                "budget_remaining": -1,
                    "stop_reason": "skipped_no_market_hits",
                    "require_in_stock": bool(require_in_stock_flag),
                }
        hints: List[str] = []
        hints.append("eBay側のヒットがないため、日本側取得をスキップしました（API節約）。")
        hints.append("eBay側のヒットがありません。型番入りで再検索してください。")
        return _finalize({
            "query": text_query,
            "market_site": market_site,
            "source_sites": normalized_sources,
            "fetched": fetched,
            "created_count": 0,
            "created_ids": [],
            "created": [],
            "errors": errors,
            "skipped_duplicates": 0,
            "skipped_low_match": 0,
            "skipped_invalid_price": 0,
            "skipped_unprofitable": 0,
            "skipped_low_margin": 0,
            "skipped_low_ev90": 0,
            "skipped_low_liquidity": 0,
            "skipped_liquidity_query_mismatch": 0,
            "skipped_unreliable_liquidity_signal": 0,
            "skipped_liquidity_unavailable": 0,
            "skipped_source_variant_unresolved": 0,
            "skipped_blocked": 0,
            "skipped_group_cap": 0,
            "skipped_ambiguous_model_title": 0,
            "liquidity_unavailable_model_codes": [],
            "low_match_reason_counts": {},
            "low_match_samples": [],
            "search_scope_done": False,
            "hints": hints,
            "applied_filters": applied_filters,
            "liquidity_rpa_refresh": rpa_refresh_summary,
            "query_cache_skip": False,
            "query_cache_ttl_sec": 0,
        })

    if rpa_refresh_enabled:
        max_model_queries = max(1, _env_int("LIQUIDITY_RPA_FETCH_MODEL_QUERIES", 3))
        top_codes = _rank_specific_codes_from_market_items(ebay_items, max_codes=max_model_queries)
        refresh_queries: List[str] = top_codes if top_codes else [text_query]
        rpa_refresh_summary = _maybe_refresh_rpa_for_fetch(refresh_queries, force=False)
        if _rpa_daily_limit_reached(rpa_refresh_summary):
            return _finalize(
                _build_rpa_daily_limit_result(
                    fetched_payload=fetched,
                    errors_payload=errors,
                    hints_extra=["型番クエリのRPA更新中に上限へ到達しました。"],
                )
            )
        if isinstance(fetched.get("ebay"), dict):
            fetched["ebay"]["rpa_prefetch_queries"] = refresh_queries

    sold_first_plan = _build_ebay_sold_first_plan(
        query=text_query,
        ebay_items=ebay_items,
        query_specific_codes=query_specific_codes,
        active_count_hint=_to_int(ebay_info.get("raw_total"), -1) if isinstance(ebay_info, dict) else -1,
        timeout=timeout,
        settings=settings,
        min_sold_90d_count=liquidity_min_sold_90d,
        min_sell_through_90d=liquidity_min_sell_through_90d,
        liquidity_require_signal=liquidity_require_signal,
        min_profit_usd=min_profit,
        min_margin_rate=min_margin,
        marketplace_fee_rate=marketplace_fee_rate,
        payment_fee_rate=payment_fee_rate,
        international_shipping_usd=est_intl_shipping,
        customs_usd=est_customs,
        packaging_usd=est_packaging,
        fixed_fee_usd=fixed_fee_usd,
    )
    sold_first_summary = sold_first_plan.get("summary", {}) if isinstance(sold_first_plan, dict) else {}
    sold_first_codes = sold_first_plan.get("selected_codes", set()) if isinstance(sold_first_plan, dict) else set()
    sold_first_ceiling = (
        sold_first_plan.get("max_purchase_jpy_by_code", {}) if isinstance(sold_first_plan, dict) else {}
    )
    sold_first_signal_lookup = _build_sold_first_signal_lookup(
        sold_first_plan.get("signals", {}) if isinstance(sold_first_plan, dict) else {}
    )
    if sold_first_codes and query_specific_codes and _env_bool("MINER_FETCH_EBAY_SOLD_FIRST_FILTER_BY_QUERY_CODE", True):
        before_count = len(sold_first_codes)
        sold_first_codes = _filter_sold_first_codes_for_query(sold_first_codes, query_specific_codes)
        if isinstance(sold_first_summary, dict):
            sold_first_summary["query_code_filter_applied"] = True
            sold_first_summary["query_code_filter_before"] = int(before_count)
            sold_first_summary["query_code_filter_after"] = int(len(sold_first_codes))
            sold_first_summary["selected_codes"] = sorted(sold_first_codes)
            sold_first_summary["codes_passed"] = int(len(sold_first_codes))
    if isinstance(fetched.get("ebay"), dict):
        fetched["ebay"]["sold_first"] = sold_first_summary

    if sold_first_codes:
        ebay_items, sold_first_market_filter = _filter_items_by_query_codes(
            items=ebay_items,
            query_codes=sold_first_codes,
            allow_fallback_no_match=False,
        )
        if isinstance(fetched.get("ebay"), dict):
            fetched["ebay"]["sold_first_market_filter"] = sold_first_market_filter
        if not ebay_items:
            hints: List[str] = []
            hints.append("eBay売却先行フィルタ後に候補が0件でした。")
            hints.append("型番候補が細かすぎる可能性があります。クエリ幅を少し広げてください。")
            return _finalize(
                {
                    "query": text_query,
                    "market_site": market_site,
                    "source_sites": normalized_sources,
                    "fetched": fetched,
                    "created_count": 0,
                    "created_ids": [],
                    "created": [],
                    "errors": errors,
                    "skipped_duplicates": 0,
                    "skipped_low_match": 0,
                    "skipped_invalid_price": 0,
                    "skipped_unprofitable": 0,
                    "skipped_low_margin": 0,
                    "skipped_low_ev90": 0,
                    "skipped_low_liquidity": 0,
                    "skipped_liquidity_query_mismatch": 0,
                    "skipped_unreliable_liquidity_signal": 0,
                    "skipped_liquidity_unavailable": 0,
                    "skipped_source_variant_unresolved": 0,
                    "skipped_blocked": 0,
                    "skipped_group_cap": 0,
                    "skipped_ambiguous_model_title": 0,
                    "liquidity_unavailable_model_codes": [],
                    "low_match_reason_counts": {},
                    "low_match_samples": [],
                    "search_scope_done": False,
                    "hints": hints,
                    "applied_filters": applied_filters,
                    "liquidity_rpa_refresh": rpa_refresh_summary,
                    "query_cache_skip": False,
                    "query_cache_ttl_sec": 0,
                }
            )
    elif sold_first_required:
        hints: List[str] = []
        hints.append("eBay売却先行フィルタで通過型番が0件でした。")
        hints.append("カテゴリや検索語を変更するか、売却件数・利益閾値を調整してください。")
        return _finalize(
            {
                "query": text_query,
                "market_site": market_site,
                "source_sites": normalized_sources,
                "fetched": fetched,
                "created_count": 0,
                "created_ids": [],
                "created": [],
                "errors": errors,
                "skipped_duplicates": 0,
                "skipped_low_match": 0,
                "skipped_invalid_price": 0,
                "skipped_unprofitable": 0,
                "skipped_low_margin": 0,
                "skipped_low_ev90": 0,
                "skipped_low_liquidity": 0,
                "skipped_liquidity_query_mismatch": 0,
                "skipped_unreliable_liquidity_signal": 0,
                "skipped_liquidity_unavailable": 0,
                "skipped_source_variant_unresolved": 0,
                "skipped_blocked": 0,
                "skipped_group_cap": 0,
                "skipped_ambiguous_model_title": 0,
                "liquidity_unavailable_model_codes": [],
                "low_match_reason_counts": {},
                "low_match_samples": [],
                "search_scope_done": False,
                "hints": hints,
                "applied_filters": applied_filters,
                "liquidity_rpa_refresh": rpa_refresh_summary,
                "query_cache_skip": False,
                "query_cache_ttl_sec": 0,
            }
        )

    sold_first_source_backfill_summary: Dict[str, Any] = {"enabled": bool(sold_first_codes), "ran": False}
    if sold_first_codes:
        synthetic_market_items = [
            MarketItem(
                site="ebay",
                item_id=f"sold-first:{code}",
                title=code,
                item_url="",
                image_url="",
                price=0.0,
                shipping=0.0,
                currency="USD",
                condition="new",
                identifiers={},
                raw={},
            )
            for code in sorted(sold_first_codes)
        ]
        sold_first_items, sold_first_source_backfill_summary = _fetch_source_model_backfill_from_market(
            market_items=synthetic_market_items,
            source_sites=normalized_sources,
            timeout=timeout,
            cap_site=cap_site,
            require_in_stock=require_in_stock_flag,
        )
        jp_items.extend(sold_first_items)
        fetched["sold_first_source_backfill"] = sold_first_source_backfill_summary

    for source in normalized_sources:
        if _can_skip_source_fetch_after_preselection(
            sold_first_codes=sold_first_codes,
            jp_items=jp_items,
            cap_site=cap_site,
            source_sites=normalized_sources,
        ):
            fetched[source] = {
                "ok": True,
                "count": 0,
                "calls_made": 0,
                "cache_hits": 0,
                "network_calls": 0,
                "budget_remaining": -1,
                "stop_reason": "skipped_by_sold_first_preselection",
                "require_in_stock": bool(require_in_stock_flag),
            }
            continue
        items, info, fetch_error = _fetch_site_items_adaptive(
            site=source,
            query=text_query,
            cap_site=cap_site,
            timeout=timeout,
            require_in_stock=require_in_stock_flag,
        )
        items, source_query_code_filter = _filter_items_by_query_codes(
            items=items,
            query_codes=query_specific_codes,
            allow_fallback_no_match=(not strict_query_code_match),
        )
        if isinstance(info, dict):
            info["query_model_code_filter"] = source_query_code_filter
        fetched[source] = info
        jp_items.extend(items)
        if fetch_error:
            errors.append({"site": source, "message": fetch_error})

    if sold_first_ceiling:
        require_code_match_for_budget = _env_bool("MINER_FETCH_EBAY_SOLD_FIRST_REQUIRE_CODE_MATCH", False) or bool(
            query_specific_codes
        )
        jp_items, source_budget_filter = _filter_source_items_by_purchase_ceiling(
            items=jp_items,
            max_purchase_jpy_by_code=sold_first_ceiling,
            require_code_match=require_code_match_for_budget,
        )
        fetched["source_budget_filter"] = source_budget_filter

    required_done_sites = ["ebay"] + [site for site in normalized_sources if site != "ebay"]
    scope_done = False
    if not errors and required_done_sites:
        done_rows: List[Dict[str, Any]] = []
        missing_required = False
        for site in required_done_sites:
            info = fetched.get(site)
            if not isinstance(info, dict):
                missing_required = True
                break
            done_rows.append(info)
        if (not missing_required) and done_rows:
            scope_done = all(_is_site_scope_done(info) for info in done_rows)

    if not ebay_items or not jp_items:
        if rpa_refresh_enabled:
            rpa_refresh_summary = _maybe_refresh_rpa_for_fetch([text_query], force=False)
            if _rpa_daily_limit_reached(rpa_refresh_summary):
                return _finalize(
                    _build_rpa_daily_limit_result(
                        fetched_payload=fetched,
                        errors_payload=errors,
                        hints_extra=["候補不足時のRPA再取得中に上限へ到達しました。"],
                        search_scope_done=scope_done,
                    )
                )
        hints: List[str] = []
        if scope_done:
            hints.append("この検索ワードは現在設定の探索範囲を完走済みです。")
        if not ebay_items:
            hints.append("eBay側のヒットがありません。型番入りで再検索してください。")
        if not jp_items:
            if require_in_stock_flag:
                hints.append("日本側のヒットがありません。キーワードを少し広げるか、在庫フィルタを解除して再検索してください。")
            else:
                hints.append("日本側のヒットがありません。キーワードを少し広げて再検索してください。")
        return _finalize({
            "query": text_query,
            "market_site": market_site,
            "source_sites": normalized_sources,
            "fetched": fetched,
            "created_count": 0,
            "created_ids": [],
            "created": [],
            "errors": errors,
            "skipped_duplicates": 0,
            "skipped_low_match": 0,
            "skipped_invalid_price": 0,
            "skipped_unprofitable": 0,
            "skipped_low_margin": 0,
            "skipped_low_ev90": 0,
            "skipped_low_liquidity": 0,
            "skipped_liquidity_query_mismatch": 0,
            "skipped_unreliable_liquidity_signal": 0,
            "skipped_liquidity_unavailable": 0,
            "skipped_source_variant_unresolved": 0,
            "skipped_blocked": 0,
            "skipped_group_cap": 0,
            "skipped_ambiguous_model_title": 0,
            "liquidity_unavailable_model_codes": [],
            "low_match_reason_counts": {},
            "low_match_samples": [],
            "search_scope_done": scope_done,
            "hints": hints,
            "applied_filters": applied_filters,
            "liquidity_rpa_refresh": rpa_refresh_summary,
            "query_cache_skip": False,
            "query_cache_ttl_sec": 0,
        })

    analysis = _analyze_candidate_matches(
        jp_items=jp_items,
        ebay_items=ebay_items,
        min_score=min_score,
        allow_ambiguous_codes=(sold_first_codes if sold_first_codes else None),
    )
    candidate_matches = list(analysis.get("candidate_matches", []))
    skipped_low_match = _to_int(analysis.get("skipped_low_match"), 0)
    skipped_ambiguous_model_title = _to_int(analysis.get("skipped_ambiguous_model_title"), 0)
    low_match_reason_counts = dict(analysis.get("low_match_reason_counts", {}))
    low_match_samples = list(analysis.get("low_match_samples", []))

    model_backfill_summary: Dict[str, Any] = {"enabled": _env_bool("MINER_FETCH_MODEL_BACKFILL_ENABLED", True), "ran": False}
    skip_model_backfill_for_query = _should_skip_model_backfill_for_query(query_specific_codes)
    if skip_model_backfill_for_query:
        model_backfill_summary["skipped_reason"] = "specific_query_codes"
        model_backfill_summary["query_specific_code_count"] = len(query_specific_codes)
    model_conflict_count = _to_int(low_match_reason_counts.get("model_code_conflict"), 0)
    should_model_backfill = (
        bool(model_backfill_summary.get("enabled"))
        and (not skip_model_backfill_for_query)
        and skipped_low_match >= max(6, cap_candidates // 2)
        and model_conflict_count >= max(6, int(skipped_low_match * 0.4))
    )
    if should_model_backfill:
        backfill_items, model_backfill_summary = _fetch_ebay_model_backfill(
            source_items=jp_items,
            base_query=text_query,
            timeout=timeout,
            cap_site=cap_site,
        )
        if backfill_items:
            seen_market: set[Tuple[str, str]] = {_item_identity(item) for item in ebay_items}
            merged_ebay_items = list(ebay_items)
            added = 0
            for item in backfill_items:
                ident = _item_identity(item)
                if ident in seen_market:
                    continue
                seen_market.add(ident)
                merged_ebay_items.append(item)
                added += 1
            ebay_items = merged_ebay_items
            model_backfill_summary["unique_added_items"] = int(added)
            analysis = _analyze_candidate_matches(
                jp_items=jp_items,
                ebay_items=ebay_items,
                min_score=min_score,
                allow_ambiguous_codes=(sold_first_codes if sold_first_codes else None),
            )
            candidate_matches = list(analysis.get("candidate_matches", []))
            skipped_low_match = _to_int(analysis.get("skipped_low_match"), 0)
            skipped_ambiguous_model_title = _to_int(analysis.get("skipped_ambiguous_model_title"), 0)
            low_match_reason_counts = dict(analysis.get("low_match_reason_counts", {}))
            low_match_samples = list(analysis.get("low_match_samples", []))
    if isinstance(fetched.get("ebay"), dict):
        fetched["ebay"]["model_backfill"] = model_backfill_summary

    source_model_backfill_summary: Dict[str, Any] = {
        "enabled": _env_bool("MINER_FETCH_SOURCE_MODEL_BACKFILL_ENABLED", True),
        "ran": False,
    }
    if skip_model_backfill_for_query:
        source_model_backfill_summary["skipped_reason"] = "specific_query_codes"
        source_model_backfill_summary["query_specific_code_count"] = len(query_specific_codes)
    should_source_model_backfill = (
        bool(source_model_backfill_summary.get("enabled"))
        and (not skip_model_backfill_for_query)
        and skipped_low_match >= max(6, cap_candidates // 2)
        and model_conflict_count >= max(4, int(skipped_low_match * 0.25))
    )
    if should_source_model_backfill:
        source_backfill_items, source_model_backfill_summary = _fetch_source_model_backfill_from_market(
            market_items=ebay_items,
            source_sites=normalized_sources,
            timeout=timeout,
            cap_site=cap_site,
            require_in_stock=require_in_stock_flag,
        )
        if source_backfill_items:
            seen_source: set[Tuple[str, str]] = {_item_identity(item) for item in jp_items}
            merged_jp_items = list(jp_items)
            added_source = 0
            for item in source_backfill_items:
                ident = _item_identity(item)
                if ident in seen_source:
                    continue
                seen_source.add(ident)
                merged_jp_items.append(item)
                added_source += 1
            jp_items = merged_jp_items
            source_model_backfill_summary["unique_added_items"] = int(added_source)
            analysis = _analyze_candidate_matches(
                jp_items=jp_items,
                ebay_items=ebay_items,
                min_score=min_score,
                allow_ambiguous_codes=(sold_first_codes if sold_first_codes else None),
            )
            candidate_matches = list(analysis.get("candidate_matches", []))
            skipped_low_match = _to_int(analysis.get("skipped_low_match"), 0)
            skipped_ambiguous_model_title = _to_int(analysis.get("skipped_ambiguous_model_title"), 0)
            low_match_reason_counts = dict(analysis.get("low_match_reason_counts", {}))
            low_match_samples = list(analysis.get("low_match_samples", []))
    fetched["source_model_backfill"] = source_model_backfill_summary

    if rpa_refresh_enabled and not bool(rpa_refresh_summary.get("ran")):
        max_model_queries = max(1, _env_int("LIQUIDITY_RPA_FETCH_MODEL_QUERIES", 3))
        model_queries: List[str] = []
        seen_candidate_codes: set[str] = set()
        for row in candidate_matches:
            source = row.get("source")
            market = row.get("market")
            if not isinstance(source, MarketItem) or not isinstance(market, MarketItem):
                continue
            for code in _candidate_model_codes(source, market):
                text = str(code or "").strip().upper()
                if not text or text in seen_candidate_codes:
                    continue
                seen_candidate_codes.add(text)
                model_queries.append(text)
                if len(model_queries) >= max_model_queries:
                    break
            if len(model_queries) >= max_model_queries:
                break
        source_backfill_queries: List[str] = []
        source_backfill_rows = (
            source_model_backfill_summary.get("queries")
            if isinstance(source_model_backfill_summary, dict)
            else []
        )
        if isinstance(source_backfill_rows, list):
            for row in source_backfill_rows:
                if not isinstance(row, dict):
                    continue
                text = str(row.get("query", "") or "").strip().upper()
                if text:
                    source_backfill_queries.append(text)
                if len(source_backfill_queries) >= max_model_queries:
                    break
        merged_model_queries: List[str] = []
        seen_model_queries: set[str] = set()
        for code in [*model_queries, *source_backfill_queries]:
            text = str(code or "").strip().upper()
            if not text or text in seen_model_queries:
                continue
            seen_model_queries.add(text)
            merged_model_queries.append(text)
            if len(merged_model_queries) >= max_model_queries:
                break
        broad_query = bool((not re.search(r"\d", text_query)) and len(text_query.strip().split()) <= 3)
        if broad_query and merged_model_queries:
            refresh_queries = list(merged_model_queries)
        else:
            refresh_queries = [text_query, *merged_model_queries]
        rpa_refresh_summary = _maybe_refresh_rpa_for_fetch(refresh_queries, force=False)
        if _rpa_daily_limit_reached(rpa_refresh_summary):
            return _finalize(
                _build_rpa_daily_limit_result(
                    fetched_payload=fetched,
                    errors_payload=errors,
                    hints_extra=["候補型番からのRPA補完中に上限へ到達しました。"],
                )
            )

    # 同一eBay候補に対しては、現在の仕入れ最安（同率なら一致スコア高い方）を優先する。
    best_by_market: Dict[str, Dict[str, Any]] = {}
    for row in candidate_matches:
        market_key = _market_identity_key(row["market"])
        source_total = _to_float(row["source"].price, 0.0) + _to_float(row["source"].shipping, 0.0)
        current = best_by_market.get(market_key)
        if current is None:
            best_by_market[market_key] = row
            continue
        current_source_total = _to_float(current["source"].price, 0.0) + _to_float(current["source"].shipping, 0.0)
        if source_total + 1e-9 < current_source_total:
            best_by_market[market_key] = row
            continue
        if abs(source_total - current_source_total) <= 1e-9 and float(row["score"]) > float(current["score"]):
            best_by_market[market_key] = row
    candidate_matches = list(best_by_market.values())
    candidate_matches.sort(
        key=lambda row: (-float(row["score"]), _to_float(row["source"].price, 0.0) + _to_float(row["source"].shipping, 0.0))
    )
    existing_pairs = _existing_pairs(settings)
    existing_signatures = _existing_pair_signatures(settings)
    created_ids: List[int] = []
    created_summaries: List[Dict[str, Any]] = []
    skipped_duplicates = 0
    skipped_invalid_price = 0
    skipped_unprofitable = 0
    skipped_low_margin = 0
    skipped_below_sold_min = 0
    skipped_missing_sold_min = 0
    skipped_non_min_basis = 0
    skipped_missing_sold_sample = 0
    skipped_implausible_sold_min = 0
    skipped_source_variant_unresolved = 0
    skipped_low_ev90 = 0
    skipped_low_liquidity = 0
    skipped_liquidity_unavailable = 0
    skipped_liquidity_query_mismatch = 0
    skipped_unreliable_liquidity_signal = 0
    skipped_blocked = 0
    skipped_group_cap = 0
    liquidity_unavailable_model_codes: set[str] = set()
    seen_run_pairs: set[Tuple[str, str, str, str]] = set()
    seen_run_signatures: set[str] = set()
    group_max = max(1, min(12, _env_int("MINER_FETCH_MAX_PER_GROUP", 4)))
    group_counts: Dict[str, int] = {}
    ebay_active_count_hint = _to_int(ebay_info.get("raw_total"), -1) if isinstance(ebay_info, dict) else -1

    require_sold_min_basis = _env_bool("LIQUIDITY_REQUIRE_SOLD_MIN_PRICE", True)
    require_sold_sample_item_env = _env_bool("LIQUIDITY_REQUIRE_SOLD_SAMPLE_ITEM", True)
    # 90日最低成約価格を売値基準にする場合は、売却サンプルURLを常に必須にする。
    # （設定値がfalseでもここは強制）
    require_sold_sample_item = True if require_sold_min_basis else require_sold_sample_item_env
    liquidity_signal_cache_by_query: Dict[str, Dict[str, Any]] = {}
    liquidity_signal_reuse_sold_first = 0
    liquidity_signal_reuse_query_cache = 0
    source_variant_price_cache: Dict[Tuple[str, str, str], Tuple[float, Dict[str, Any]]] = {}

    for row in candidate_matches:
        if len(created_ids) >= cap_candidates:
            break
        source = row["source"]
        market = row["market"]
        score = float(row["score"])
        reason = str(row["reason"])
        signature = _pair_signature(source.title, market.title)
        if signature in blocked_pair_signatures:
            skipped_blocked += 1
            continue
        if signature in seen_run_signatures:
            skipped_duplicates += 1
            continue
        if signature in existing_signatures:
            skipped_duplicates += 1
            continue
        source_runtime_id = (source.item_id or source.item_url or source.title).strip().lower()
        market_runtime_id = (market.item_id or market.item_url or market.title).strip().lower()
        runtime_key = (source.site, source_runtime_id, market.site, market_runtime_id)
        if runtime_key in seen_run_pairs:
            skipped_duplicates += 1
            continue
        pair_key = (source.site, source.item_id, market.site, market.item_id)
        if all(pair_key) and pair_key in existing_pairs:
            skipped_duplicates += 1
            continue
        group_key = _candidate_group_key(source, market)
        if group_key and int(group_counts.get(group_key, 0)) >= group_max:
            skipped_group_cap += 1
            continue

        source_price_for_calc = _to_float(source.price, 0.0)
        source_shipping_for_calc = _to_float(source.shipping, 0.0)
        source_price_basis_type = "listing_price"
        source_variant_resolution: Dict[str, Any] = {
            "site": source.site,
            "applied": False,
            "ambiguous_source_model_codes": [],
            "target_model_code": "",
            "reason": "",
        }
        source_specific_codes = _specific_model_codes_in_title(source.title)
        source_specific_canon = _canonical_code_set(source_specific_codes)
        market_specific_canon = _canonical_code_set(_specific_model_codes_in_title(market.title))
        pair_common_specific = sorted(source_specific_canon & market_specific_canon)
        target_model_code = pair_common_specific[0] if pair_common_specific else ""
        source_variant_resolution["ambiguous_source_model_codes"] = sorted(source_specific_canon)
        source_variant_resolution["target_model_code"] = target_model_code
        if len(source_specific_canon) >= 2:
            if str(source.site or "").strip().lower() == "rakuten" and target_model_code:
                cache_key = (str(source.site or "").strip().lower(), str(source.item_url or "").strip(), target_model_code)
                resolved_price, resolved_info = source_variant_price_cache.get(cache_key, (-1.0, {}))
                if not resolved_info:
                    resolved_price, resolved_info = _resolve_rakuten_variant_price_jpy(
                        item_url=str(source.item_url or ""),
                        target_code=target_model_code,
                        timeout=timeout,
                    )
                    source_variant_price_cache[cache_key] = (resolved_price, resolved_info)
                if resolved_price > 0:
                    source_price_for_calc = resolved_price
                    source_price_basis_type = "rakuten_variant_model_price"
                    source_variant_resolution = {
                        **source_variant_resolution,
                        **(resolved_info if isinstance(resolved_info, dict) else {}),
                        "applied": True,
                    }
                else:
                    skipped_source_variant_unresolved += 1
                    source_variant_resolution = {
                        **source_variant_resolution,
                        **(resolved_info if isinstance(resolved_info, dict) else {}),
                        "applied": False,
                    }
                    continue
            else:
                skipped_source_variant_unresolved += 1
                source_variant_resolution["reason"] = "ambiguous_source_model_codes"
                continue

        liquidity_signal: Dict[str, Any] = {}
        liquidity_gate: Dict[str, Any] = {
            "pass": True,
            "reason": "liquidity_gate_disabled",
            "sold_90d_count": -1,
            "sell_through_90d": -1.0,
            "source": "",
        }
        liquidity_query = _preferred_liquidity_query(
            source=source,
            market=market,
            base_query=text_query,
            preferred_codes=(sold_first_codes if sold_first_codes else None),
        )
        if not liquidity_query:
            liquidity_query = text_query
        if not _liquidity_query_matches_pair(query=liquidity_query, source=source, market=market):
            skipped_liquidity_query_mismatch += 1
            continue
        liquidity_use_query_only = bool(sold_first_codes)
        if liquidity_gate_enabled:
            liquidity_key = _liquidity_query_key(liquidity_query)
            used_preloaded_signal = False
            if liquidity_use_query_only and liquidity_key:
                preloaded = sold_first_signal_lookup.get(liquidity_key)
                if isinstance(preloaded, dict) and preloaded:
                    liquidity_signal = preloaded
                    used_preloaded_signal = True
                    liquidity_signal_reuse_sold_first += 1
                else:
                    cached = liquidity_signal_cache_by_query.get(liquidity_key)
                    if isinstance(cached, dict) and cached:
                        liquidity_signal = cached
                        liquidity_signal_reuse_query_cache += 1
            if not liquidity_signal:
                liquidity_signal = get_liquidity_signal(
                    query=liquidity_query,
                    source_title=(liquidity_query if liquidity_use_query_only else source.title),
                    market_title=(liquidity_query if liquidity_use_query_only else market.title),
                    source_identifiers=({} if liquidity_use_query_only else source.identifiers),
                    market_identifiers=({} if liquidity_use_query_only else market.identifiers),
                    active_count_hint=ebay_active_count_hint,
                    timeout=timeout,
                    settings=settings,
                )
            if liquidity_use_query_only and liquidity_key and (not used_preloaded_signal):
                if isinstance(liquidity_signal, dict) and liquidity_signal:
                    liquidity_signal_cache_by_query[liquidity_key] = liquidity_signal
            signal_reliable, signal_reject_reason = _liquidity_signal_is_reliable_for_pair(
                signal=liquidity_signal,
                liquidity_query=liquidity_query,
                source=source,
                market=market,
            )
            if not signal_reliable:
                skipped_unreliable_liquidity_signal += 1
                if signal_reject_reason == "signal_rpa_query_model_mismatch":
                    skipped_liquidity_query_mismatch += 1
                continue
            liquidity_gate = evaluate_liquidity_gate(
                liquidity_signal,
                min_sold_90d_count=liquidity_min_sold_90d,
                min_sell_through_90d=liquidity_min_sell_through_90d,
                require_signal=liquidity_require_signal,
            )
            if not bool(liquidity_gate.get("pass")):
                if str(liquidity_gate.get("reason", "")).strip() == "liquidity_unavailable_required":
                    skipped_liquidity_unavailable += 1
                    for code in _candidate_model_codes(source, market):
                        if code:
                            liquidity_unavailable_model_codes.add(code)
                else:
                    skipped_low_liquidity += 1
                continue

        sale_price_basis_usd, sale_price_basis_type, sale_shipping_basis_usd = _sale_price_basis_from_signal(
            market, liquidity_signal
        )
        liquidity_meta = liquidity_signal.get("metadata") if isinstance(liquidity_signal.get("metadata"), dict) else {}
        sold_min_basis = _to_float(liquidity_meta.get("sold_price_min"), -1.0)
        sold_min_raw = _to_float(liquidity_meta.get("sold_price_min_raw"), sold_min_basis)
        sold_min_outlier = bool(liquidity_meta.get("sold_price_min_outlier"))
        if require_sold_min_basis:
            if strict_sold_min_basis:
                if not _is_strict_sold_min_basis_candidate(
                    sale_price_basis_type=sale_price_basis_type,
                    sold_min_basis=sold_min_basis,
                    sold_min_outlier=sold_min_outlier,
                ):
                    if sold_min_basis <= 0:
                        skipped_missing_sold_min += 1
                    else:
                        skipped_non_min_basis += 1
                    continue
                sale_price_basis_usd = sold_min_basis
                sale_price_basis_type = "sold_price_min_90d"
            else:
                if sale_price_basis_type == "sold_price_median_fallback_90d" and sale_price_basis_usd > 0:
                    sold_min_basis = sale_price_basis_usd
                elif sold_min_basis <= 0:
                    skipped_missing_sold_min += 1
                    continue
                sale_price_basis_usd = sold_min_basis
                if sale_price_basis_type != "sold_price_median_fallback_90d":
                    sale_price_basis_type = "sold_price_min_90d"
            sale_shipping_basis_usd = 0.0
        sale_total_usd = sale_price_basis_usd + sale_shipping_basis_usd
        purchase_total_jpy = source_price_for_calc + source_shipping_for_calc
        if sale_total_usd <= 0 or purchase_total_jpy <= 0:
            skipped_invalid_price += 1
            continue

        calc = calculate_profit(
            ProfitInput(
                sale_price_usd=sale_total_usd,
                purchase_price_jpy=source_price_for_calc,
                domestic_shipping_jpy=source_shipping_for_calc,
                international_shipping_usd=est_intl_shipping,
                customs_usd=est_customs,
                packaging_usd=est_packaging,
                marketplace_fee_rate=marketplace_fee_rate,
                payment_fee_rate=payment_fee_rate,
                fixed_fee_usd=fixed_fee_usd,
            ),
            settings=settings,
        )
        breakdown = calc["breakdown"]
        fx = calc["fx"]
        profit_usd = _to_float(breakdown.get("profit_usd"))
        margin_rate = _to_float(breakdown.get("margin_rate"))
        if profit_usd < min_profit:
            skipped_unprofitable += 1
            continue
        if margin_rate < min_margin:
            skipped_low_margin += 1
            continue
        fx_rate = _to_float(fx.get("rate"), 0.0)
        purchase_total_usd = (purchase_total_jpy / fx_rate) if fx_rate > 0 else 0.0
        if require_sold_min_basis and sold_min_basis > 0 and purchase_total_usd >= sold_min_basis:
            skipped_below_sold_min += 1
            continue
        implausible_sold_min, implausible_detail = _is_implausible_sold_min(
            sold_min_raw_usd=sold_min_raw,
            source_total_usd=purchase_total_usd,
            active_total_usd=market.price + market.shipping,
            sold_min_outlier_flag=sold_min_outlier,
        )
        if implausible_sold_min:
            skipped_implausible_sold_min += 1
            continue
        ev90 = estimate_ev90(
            profit_usd=profit_usd,
            purchase_total_usd=purchase_total_usd,
            liquidity_signal=liquidity_signal,
        )
        if not bool(ev90.get("pass", False)):
            skipped_low_ev90 += 1
            continue
        sold_sample = _liquidity_sold_sample(liquidity_signal)
        has_sold_sample_reference = _has_sold_sample_reference(sold_sample)
        if require_sold_sample_item and not has_sold_sample_reference:
            skipped_missing_sold_sample += 1
            continue
        market_title_display = str(sold_sample.get("title", "") or market.title or "").strip() or market.title
        market_item_id_display = market.item_id or None
        if str(sale_price_basis_type or "").strip().lower() == "sold_price_min_90d":
            market_url_display = str(sold_sample.get("item_url", "") or "").strip()
            market_image_display = (
                str(sold_sample.get("image_url", "") or "").strip()
                or str(market.image_url or "").strip()
            )
            sold_item_id = _ebay_item_id_from_url(market_url_display)
            if sold_item_id:
                market_item_id_display = sold_item_id
        else:
            market_url_display = str(sold_sample.get("item_url", "") or market.item_url or "").strip() or market.item_url
            market_image_display = str(sold_sample.get("image_url", "") or market.image_url or "").strip() or market.image_url
        payload = {
            "source_site": source.site,
            "market_site": market.site,
            "source_item_id": source.item_id or None,
            "market_item_id": market_item_id_display,
            "source_title": source.title,
            "market_title": market_title_display,
            "condition": "new",
            "match_level": _match_level(score, reason),
            "match_score": round(score, 4),
            "expected_profit_usd": round(profit_usd, 4),
            "expected_margin_rate": round(margin_rate, 6),
            "fx_rate": fx_rate,
            "fx_source": str(fx.get("source", "") or ""),
            "metadata": {
                "source_item_url": source.item_url,
                "market_item_url": market_url_display,
                "source_image_url": source.image_url,
                "market_image_url": market_image_display,
                "market_item_url_active": market.item_url,
                "market_image_url_active": market.image_url,
                "market_title_active": market.title,
                "market_item_id_active": market.item_id,
                "source_price_jpy": source.price,
                "source_shipping_jpy": source.shipping,
                "source_price_basis_jpy": source_price_for_calc,
                "source_shipping_basis_jpy": source_shipping_for_calc,
                "source_price_basis_type": source_price_basis_type,
                "source_total_jpy": purchase_total_jpy,
                "market_price_usd": market.price,
                "market_shipping_usd": market.shipping,
                "market_price_basis_usd": sale_price_basis_usd,
                "market_shipping_basis_usd": sale_shipping_basis_usd,
                "market_revenue_basis_usd": sale_total_usd,
                "market_price_basis_type": sale_price_basis_type,
                "calc_input": calc.get("input", {}),
                "calc_breakdown": calc.get("breakdown", {}),
                "calc_fx": calc.get("fx", {}),
                "match_reason": reason,
                "pair_signature": signature,
                "liquidity_sold_min_implausible_check": implausible_detail,
                "liquidity_query": liquidity_query,
                "source_identifiers": source.identifiers,
                "market_identifiers": market.identifiers,
                "source_currency": source.currency,
                "market_currency": market.currency,
                "source_condition": source.condition,
                "market_condition": market.condition,
                "source_variant_price_resolution": source_variant_resolution,
                "source_require_in_stock": bool(require_in_stock_flag),
                "source_stock_status": _source_stock_status(source),
                "ev90": ev90,
                "liquidity": {
                    **liquidity_signal,
                    "gate_enabled": bool(liquidity_gate_enabled),
                    "gate_required_signal": bool(liquidity_require_signal),
                    "gate_min_sold_90d_count": int(liquidity_min_sold_90d),
                    "gate_min_sell_through_90d": float(liquidity_min_sell_through_90d),
                    "gate_passed": bool(liquidity_gate.get("pass", False)),
                    "gate_reason": str(liquidity_gate.get("reason", "") or ""),
                },
            },
        }
        if sold_sample:
            payload["metadata"]["ebay_sold_item_url"] = sold_sample.get("item_url")
            payload["metadata"]["ebay_sold_image_url"] = sold_sample.get("image_url")
            payload["metadata"]["ebay_sold_title"] = sold_sample.get("title")
            payload["metadata"]["ebay_sold_price_usd"] = sold_sample.get("sold_price_usd")
            payload["metadata"]["ebay_sold_sample_reference_ok"] = bool(has_sold_sample_reference)
        created = create_miner_candidate(payload, settings=settings)
        candidate_id = int(created["id"])
        created_ids.append(candidate_id)
        created_summaries.append(
            {
                "id": candidate_id,
                "source_title": created["source_title"],
                "market_title": created["market_title"],
                "match_score": created["match_score"],
                "expected_profit_usd": created["expected_profit_usd"],
                "ev90_score_usd": (payload.get("metadata", {}).get("ev90", {}) or {}).get("score_usd"),
            }
        )
        seen_run_pairs.add(runtime_key)
        seen_run_signatures.add(signature)
        if group_key:
            group_counts[group_key] = int(group_counts.get(group_key, 0)) + 1
        if all(pair_key):
            existing_pairs.add(pair_key)
        existing_signatures.add(signature)

    hints: List[str] = []
    knowledge_applied_rows: List[str] = []
    for site_key, info in fetched.items():
        if not isinstance(info, dict):
            continue
        knowledge = info.get("knowledge", {})
        if not isinstance(knowledge, dict) or not bool(knowledge.get("applied")):
            continue
        category_name = str(knowledge.get("category_name", "") or "").strip()
        category_key = str(knowledge.get("category_key", "") or "").strip()
        label = category_name or category_key
        if not label:
            continue
        active_tags = knowledge.get("active_season_tags", [])
        tag_text = ""
        if isinstance(active_tags, list) and active_tags:
            tags = [str(v).strip() for v in active_tags if str(v).strip()]
            if tags:
                tag_text = f" (季節タグ: {','.join(tags[:3])})"
        knowledge_applied_rows.append(f"{site_key}:{label}{tag_text}")
    if knowledge_applied_rows:
        hints.append(f"カテゴリナレッジを適用して探索しました: {' / '.join(knowledge_applied_rows)}")
    if bool(model_backfill_summary.get("enabled")) and bool(model_backfill_summary.get("ran")):
        qn = _to_int(model_backfill_summary.get("query_count"), 0)
        added = _to_int(model_backfill_summary.get("unique_added_items"), _to_int(model_backfill_summary.get("added_items"), 0))
        hints.append(f"型番バックフィルでeBay再取得しました: queries={qn}, add={added}")
        if str(model_backfill_summary.get("reason", "") or "") == "partial_error":
            hints.append("型番バックフィルの一部クエリで取得エラーがありました。")
    if bool(rpa_refresh_summary.get("enabled")):
        if bool(rpa_refresh_summary.get("ran")):
            qn = int(rpa_refresh_summary.get("query_count", 0) or 0)
            rc = int(rpa_refresh_summary.get("returncode", 0) or 0)
            hints.append(f"Product Research(RPA)を実行しました: queries={qn}, rc={rc}")
            if _rpa_daily_limit_reached(rpa_refresh_summary):
                hints.append("Product Researchの日次上限に到達したため、RPA処理を停止しました。")
            elif rc != 0:
                hints.append("Product Research(RPA)が失敗しました。環境設定またはログを確認してください。")
        elif _rpa_daily_limit_reached(rpa_refresh_summary):
            hints.append("Product Researchの日次上限に到達しているため、RPAを再実行しませんでした。")
        elif str(rpa_refresh_summary.get("reason", "") or "") == "cooldown_skip":
            retry_after = int(rpa_refresh_summary.get("retry_after_sec", 0) or 0)
            hints.append(f"Product Research(RPA)はクールダウン中のためスキップしました（{retry_after}秒後に再実行可）。")
    if isinstance(fetched.get("ebay"), dict):
        sold_summary = fetched["ebay"].get("sold_first")
        if isinstance(sold_summary, dict):
            sold_summary["liquidity_signal_reuse_sold_first"] = int(liquidity_signal_reuse_sold_first)
            sold_summary["liquidity_signal_reuse_query_cache"] = int(liquidity_signal_reuse_query_cache)

    if len(created_ids) == 0:
        if scope_done:
            hints.append("この検索ワードは現在設定の探索範囲を完走済みです。")
        if skipped_duplicates > 0:
            hints.append("同一候補は既に取り込み済みです。別の型番やシリーズで検索してください。")
        if skipped_low_match >= max(10, cap_candidates):
            hints.append("一致スコア不足が多いため、型番/JAN入りキーワードにすると改善します。")
        if low_match_reason_counts:
            top_reason = sorted(low_match_reason_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]
            hints.append(f"一致不足の主因: {top_reason}")
        if skipped_ambiguous_model_title > 0:
            hints.append("複数型番が列挙された曖昧タイトルを除外しました。")
        if skipped_unprofitable > 0 and skipped_unprofitable >= max(3, cap_candidates // 3):
            hints.append("利益条件で除外されています。仕入れ価格が低い型番で再検索してください。")
        if skipped_missing_sold_min > 0:
            hints.append("90日最低成約価格が未取得の候補を除外しました。")
        if skipped_non_min_basis > 0:
            hints.append("90日最低成約価格基準ではない候補を除外しました。")
        if skipped_missing_sold_sample > 0:
            hints.append("90日売却済み商品の参照URL/画像が取得できない候補を除外しました。")
        if skipped_below_sold_min > 0:
            hints.append("仕入れ総額が90日最低成約価格を上回る候補を除外しました。")
        if skipped_implausible_sold_min > 0:
            hints.append("90日最低成約価格が異常に低い候補を除外しました。")
        if skipped_source_variant_unresolved > 0:
            hints.append("型番別価格を特定できない複数型番商品を除外しました。")
        if skipped_low_ev90 > 0:
            hints.append("EV90（90日期待値）で除外されています。回転率か利幅の高い商品に寄せてください。")
        if skipped_low_liquidity > 0:
            hints.append("90日売却流動性の閾値で除外されています。回転率の高い型番で再検索してください。")
        if skipped_liquidity_query_mismatch > 0:
            hints.append("流動性クエリの型番不一致候補を除外しました。")
        if skipped_unreliable_liquidity_signal > 0:
            hints.append("売却件数の根拠が不十分な流動性シグナルを除外しました。")
        if skipped_liquidity_unavailable > 0:
            hints.append("流動性データ未取得のため除外されています（LIQUIDITY_REQUIRE_SIGNAL=1）。")
        if skipped_blocked > 0:
            hints.append("否認済みブロックに該当しています。別商品の検索を推奨します。")
        if skipped_group_cap > 0:
            hints.append("同一モデル候補が多いため、重複レビュー負荷を抑える上限を適用しました。")

    return _finalize({
        "query": text_query,
        "market_site": market_site,
        "source_sites": normalized_sources,
        "fetched": fetched,
        "created_count": len(created_ids),
        "created_ids": created_ids,
        "created": created_summaries,
        "errors": errors,
        "skipped_duplicates": skipped_duplicates,
        "skipped_low_match": skipped_low_match,
        "skipped_invalid_price": skipped_invalid_price,
        "skipped_unprofitable": skipped_unprofitable,
        "skipped_low_margin": skipped_low_margin,
        "skipped_missing_sold_min": skipped_missing_sold_min,
        "skipped_non_min_basis": skipped_non_min_basis,
        "skipped_missing_sold_sample": skipped_missing_sold_sample,
        "skipped_below_sold_min": skipped_below_sold_min,
        "skipped_implausible_sold_min": skipped_implausible_sold_min,
        "skipped_source_variant_unresolved": skipped_source_variant_unresolved,
        "skipped_low_ev90": skipped_low_ev90,
        "skipped_low_liquidity": skipped_low_liquidity,
        "skipped_liquidity_query_mismatch": skipped_liquidity_query_mismatch,
        "skipped_unreliable_liquidity_signal": skipped_unreliable_liquidity_signal,
        "skipped_liquidity_unavailable": skipped_liquidity_unavailable,
        "skipped_blocked": skipped_blocked,
        "skipped_group_cap": skipped_group_cap,
        "skipped_ambiguous_model_title": skipped_ambiguous_model_title,
        "liquidity_unavailable_model_codes": sorted(liquidity_unavailable_model_codes),
        "low_match_reason_counts": low_match_reason_counts,
        "low_match_samples": low_match_samples,
        "search_scope_done": scope_done,
        "hints": hints,
        "applied_filters": applied_filters,
        "liquidity_rpa_refresh": rpa_refresh_summary,
        "query_cache_skip": False,
        "query_cache_ttl_sec": 0,
    })
