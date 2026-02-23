#!/usr/bin/env python3
"""Auto-miner active cycle candidates with precision-first strictness."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.env import load_dotenv
from reselling.coerce import env_bool as _env_bool
from reselling.coerce import env_float as _env_float
from reselling.coerce import env_int as _env_int
from reselling.coerce import to_float as _to_float
from reselling.coerce import to_int as _to_int
from reselling.time_utils import utc_iso as _now_iso
from reselling.live_miner_fetch import (
    MarketItem,
    _contains_out_of_stock_marker,
    _extract_codes,
    _extract_color_tags,
    _extract_variant_color_codes,
    _is_accessory_title,
    _is_new_listing,
    _is_specific_model_code,
    _match_score,
    _title_tokens,
)
from reselling.miner import auto_approve_miner_candidate, get_miner_candidate, reject_miner_candidate


IDENTIFIER_KEYS = ("jan", "upc", "ean", "gtin")

_MATCH_REASON_ISSUE_MAP: Dict[str, str] = {
    "accessory_mismatch": "accessories",
    "family_conflict": "model",
    "bundle_conflict": "accessories",
    "mod_conflict": "model",
    "variant_color_conflict": "color",
    "variant_color_missing_market": "color",
    "variant_color_missing_source": "color",
    "model_code_variant_color_missing_market": "color",
    "model_code_normalized_variant_color_missing_market": "color",
    "model_code_color_missing_market": "color",
    "model_code_normalized_color_missing_market": "color",
    "jan_exact_variant_color_missing_market": "color",
    "upc_exact_variant_color_missing_market": "color",
    "ean_exact_variant_color_missing_market": "color",
    "gtin_exact_variant_color_missing_market": "color",
    "jan_exact_color_missing_market": "color",
    "upc_exact_color_missing_market": "color",
    "ean_exact_color_missing_market": "color",
    "gtin_exact_color_missing_market": "color",
    "color_conflict": "color",
    "color_missing_market": "color",
    "model_code_conflict": "model",
    "model_code_partial": "model",
    "insufficient_tokens": "model",
}

_AUTO_PART_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "floor_mat": ("FLOOR MAT", "MAT", "マット", "フロアマット"),
    "headlight": ("HEADLIGHT", "HEAD LAMP", "ヘッドライト", "ヘッドランプ"),
    "taillight": ("TAIL LIGHT", "TAILLAMP", "TAIL LAMP", "テールライト", "テールランプ"),
    "air_filter": ("AIR FILTER", "CABIN FILTER", "AC FILTER", "エアコンフィルター", "フィルター"),
    "garnish": ("GARNISH", "ガーニッシュ"),
    "harness": ("HARNESS", "配線", "ハーネス"),
    "brake": ("BRAKE", "ブレーキ"),
}


def _is_color_missing_market_reason(reason: str) -> bool:
    key = str(reason or "").strip().lower()
    if not key:
        return False
    return "color_missing_market" in key


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    u = a | b
    if not u:
        return 0.0
    return len(a & b) / len(u)


def _extract_auto_part_tags(title: str) -> Set[str]:
    normalized = str(title or "")
    upper = normalized.upper()
    tags: Set[str] = set()
    for tag, keywords in _AUTO_PART_KEYWORDS.items():
        for raw_kw in keywords:
            kw = str(raw_kw or "").strip()
            if not kw:
                continue
            if kw.isascii():
                if kw in upper:
                    tags.add(tag)
                    break
            else:
                if kw in normalized:
                    tags.add(tag)
                    break
    return tags


def _normalize_identifiers(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in value.items():
        key = str(k or "").strip().lower()
        if not key:
            continue
        val = str(v or "").strip()
        if not val:
            continue
        out[key] = val
    return out


def _metadata_dict(candidate: Dict[str, Any]) -> Dict[str, Any]:
    metadata = candidate.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _candidate_item(candidate: Dict[str, Any], *, side: str) -> MarketItem:
    metadata = _metadata_dict(candidate)
    if side == "source":
        title = str(candidate.get("source_title", "") or "")
        site = str(candidate.get("source_site", "") or "")
        item_id = str(candidate.get("source_item_id", "") or "")
        item_url = str(metadata.get("source_item_url", "") or "")
        image_url = str(metadata.get("source_image_url", "") or "")
        price = _to_float(metadata.get("source_price_jpy"), 0.0)
        shipping = _to_float(metadata.get("source_shipping_jpy"), 0.0)
        currency = str(metadata.get("source_currency", "JPY") or "JPY")
        condition = str(metadata.get("source_condition", candidate.get("condition", "")) or "")
        identifiers = _normalize_identifiers(metadata.get("source_identifiers", {}))
    else:
        title = str(candidate.get("market_title", "") or "")
        site = str(candidate.get("market_site", "") or "")
        item_id = str(candidate.get("market_item_id", "") or "")
        item_url = str(metadata.get("market_item_url", "") or "")
        image_url = str(metadata.get("market_image_url", "") or "")
        price = _to_float(metadata.get("market_price_usd"), 0.0)
        shipping = _to_float(metadata.get("market_shipping_usd"), 0.0)
        currency = str(metadata.get("market_currency", "USD") or "USD")
        condition = str(metadata.get("market_condition", "NEW") or "NEW")
        identifiers = _normalize_identifiers(metadata.get("market_identifiers", {}))
    return MarketItem(
        site=site,
        item_id=item_id,
        title=title,
        item_url=item_url,
        image_url=image_url,
        price=price,
        shipping=shipping,
        currency=currency,
        condition=condition,
        identifiers=identifiers,
        raw={},
    )


def _identifier_exact(left: Dict[str, str], right: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    for key in IDENTIFIER_KEYS:
        lv = str(left.get(key, "") or "").strip()
        rv = str(right.get(key, "") or "").strip()
        if lv and rv and lv == rv:
            return key, lv
    return None, None


def evaluate_candidate(
    candidate: Dict[str, Any],
    *,
    min_profit_usd: float,
    min_margin_rate: float,
    min_ev90_usd: float,
    min_match_score: float,
    min_auto_approve_score: float,
    min_token_jaccard: float,
    max_score_drift: float,
) -> Tuple[str, List[str], str, Dict[str, Any]]:
    source = _candidate_item(candidate, side="source")
    market = _candidate_item(candidate, side="market")
    source_title = source.title
    market_title = market.title
    profit = _to_float(candidate.get("expected_profit_usd"))
    margin = _to_float(candidate.get("expected_margin_rate"))
    score = _to_float(candidate.get("match_score"))
    rematch_score, rematch_reason = _match_score(source, market)
    condition = str(candidate.get("condition", "") or "").lower()

    source_codes = set(_extract_codes(source_title))
    market_codes = set(_extract_codes(market_title))
    common_codes = sorted(source_codes & market_codes)
    common_specific_codes = [code for code in common_codes if _is_specific_model_code(code)]
    source_tokens = set(_title_tokens(source_title))
    market_tokens = set(_title_tokens(market_title))
    token_jaccard = _jaccard(source_tokens, market_tokens)

    source_colors = _extract_color_tags(source_title) | _extract_variant_color_codes(source_title)
    market_colors = _extract_color_tags(market_title) | _extract_variant_color_codes(market_title)
    has_color_conflict = bool(source_colors and market_colors and source_colors.isdisjoint(market_colors))
    source_auto_parts = _extract_auto_part_tags(source_title)
    market_auto_parts = _extract_auto_part_tags(market_title)
    has_auto_part_conflict = bool(
        source_auto_parts
        and market_auto_parts
        and source_auto_parts.isdisjoint(market_auto_parts)
    )
    has_auto_part_missing_market = bool(source_auto_parts and not market_auto_parts)
    has_auto_part_missing_source = bool(market_auto_parts and not source_auto_parts)
    id_key, id_value = _identifier_exact(source.identifiers, market.identifiers)
    metadata = _metadata_dict(candidate)
    liquidity = metadata.get("liquidity") if isinstance(metadata.get("liquidity"), dict) else {}
    liquidity_meta = liquidity.get("metadata") if isinstance(liquidity.get("metadata"), dict) else {}
    sold_90d = _to_int(liquidity.get("sold_90d_count"), -1)
    sold_price_min = _to_float(liquidity_meta.get("sold_price_min"), -1.0)
    sold_price_min_raw = _to_float(liquidity_meta.get("sold_price_min_raw"), sold_price_min)
    sold_price_min_outlier = bool(liquidity_meta.get("sold_price_min_outlier", False))
    pass_label = str(liquidity_meta.get("pass_label", "") or "").strip().lower()
    fetch_reason = str(metadata.get("match_reason", "") or "")
    ev90_meta = metadata.get("ev90") if isinstance(metadata.get("ev90"), dict) else {}
    ev90_score = _to_float(ev90_meta.get("score_usd")) if ev90_meta else 0.0
    ev90_present = bool(ev90_meta)

    issues: List[str] = []
    reasons: List[str] = []
    require_liquidity_signal = _env_bool("AUTO_MINER_REQUIRE_LIQUIDITY_SIGNAL", True)
    fallback_any_allow = _env_bool("AUTO_MINER_ALLOW_FALLBACK_ANY", True)
    fallback_min_sold_90d = max(0, _env_int("AUTO_MINER_FALLBACK_MIN_SOLD_90D", 5))
    fallback_min_profit_usd = _env_float("AUTO_MINER_FALLBACK_MIN_PROFIT_USD", 20.0)
    block_color_missing_market = _env_bool("AUTO_MINER_BLOCK_COLOR_MISSING_MARKET", True)

    if profit < min_profit_usd:
        issues.append("price")
        reasons.append(f"期待利益が低い ({profit:.2f} USD)")
    if margin < min_margin_rate:
        issues.append("price")
        reasons.append(f"粗利率が低い ({margin*100:.1f}%)")
    if ev90_present and ev90_score < min_ev90_usd:
        issues.append("price")
        reasons.append(f"EV90が低い ({ev90_score:.2f} USD)")
    if score < min_match_score:
        issues.append("model")
        reasons.append(f"一致スコア不足 ({score:.3f})")
    if rematch_score < min_match_score:
        issues.append("model")
        reasons.append(f"再評価スコア不足 ({rematch_score:.3f}, reason={rematch_reason})")
    if score - rematch_score > max_score_drift:
        issues.append("model")
        reasons.append(
            f"スコア乖離が大きい (stored={score:.3f}, rematch={rematch_score:.3f})"
        )
    if condition != "new":
        issues.append("condition")
        reasons.append(f"condition={condition} のため除外")
    if not _is_new_listing(source_title, source.condition) or not _is_new_listing(market_title, market.condition):
        issues.append("condition")
        reasons.append("タイトルに新品以外の兆候")
    if _contains_out_of_stock_marker(source_title) or _contains_out_of_stock_marker(market_title):
        issues.append("condition")
        reasons.append("在庫なし/欠品の兆候")
    if _is_accessory_title(source_title) or _is_accessory_title(market_title):
        issues.append("accessories")
        reasons.append("アクセサリ/部品候補")
    rematch_issue = _MATCH_REASON_ISSUE_MAP.get(rematch_reason)
    if rematch_issue:
        issues.append(rematch_issue)
        reasons.append(f"再評価で衝突検知 ({rematch_reason})")
    if block_color_missing_market and (
        _is_color_missing_market_reason(fetch_reason) or _is_color_missing_market_reason(rematch_reason)
    ):
        issues.append("color")
        reasons.append("eBay側の色情報欠損マッチは自動承認しない設定")
    if has_color_conflict:
        issues.append("color")
        reasons.append("色情報が不一致")
    if has_auto_part_conflict:
        issues.append("model")
        reasons.append(
            "自動車部位タグが不一致"
            f" (source={sorted(source_auto_parts)}, market={sorted(market_auto_parts)})"
        )
    if has_auto_part_missing_market:
        issues.append("model")
        reasons.append(
            "eBay側に自動車部位タグがなく同一性根拠が不足"
            f" (source={sorted(source_auto_parts)})"
        )
    if has_auto_part_missing_source:
        issues.append("model")
        reasons.append(
            "仕入側に自動車部位タグがなく同一性根拠が不足"
            f" (market={sorted(market_auto_parts)})"
        )
    if require_liquidity_signal and sold_90d < 0:
        issues.append("price")
        reasons.append("90日売却データ未取得(-1)のため自動承認対象外")
    if pass_label.startswith("fallback_any"):
        if not fallback_any_allow:
            issues.append("price")
            reasons.append("fallback_any は自動承認対象外")
        else:
            if sold_90d < fallback_min_sold_90d:
                issues.append("price")
                reasons.append(
                    f"fallback_any の売却件数不足 ({sold_90d} < {fallback_min_sold_90d})"
                )
            if profit < fallback_min_profit_usd:
                issues.append("price")
                reasons.append(
                    f"fallback_any の最低利益不足 ({profit:.2f} < {fallback_min_profit_usd:.2f} USD)"
                )
            if sold_price_min <= 0:
                issues.append("price")
                if sold_price_min_outlier and sold_price_min_raw > 0:
                    reasons.append("fallback_any だが最低成約価格が外れ値として除外")
                else:
                    reasons.append("fallback_any だが最低成約価格が未取得")

    confidence_tag = ""
    if id_key and rematch_score >= min_auto_approve_score:
        confidence_tag = f"{id_key}_exact"
    elif common_specific_codes and rematch_score >= min_auto_approve_score:
        confidence_tag = "specific_model_code"
    elif (
        rematch_reason == "token_overlap"
        and rematch_score >= min_auto_approve_score
        and token_jaccard >= min_token_jaccard
        and not has_color_conflict
    ):
        confidence_tag = "high_token_overlap"

    if not confidence_tag:
        issues.append("model")
        if common_codes and not common_specific_codes:
            reasons.append("一致コードが汎用すぎて同一性根拠として弱い")
        else:
            reasons.append("自動承認に必要な同一性根拠が不足")

    if issues:
        uniq_issues = list(dict.fromkeys(issues))
        reason = " / ".join(reasons[:4])
        return "reject", uniq_issues, reason, {
            "profit": profit,
            "margin": margin,
            "ev90_score": round(ev90_score, 4) if ev90_present else None,
            "score": score,
            "rematch_score": round(rematch_score, 4),
            "rematch_reason": rematch_reason,
            "fetch_match_reason": fetch_reason,
            "identifier_exact_key": id_key,
            "identifier_exact_value": id_value,
            "common_codes": common_codes,
            "common_specific_codes": common_specific_codes,
            "token_jaccard": round(token_jaccard, 4),
            "source_colors": sorted(source_colors),
            "market_colors": sorted(market_colors),
            "source_auto_parts": sorted(source_auto_parts),
            "market_auto_parts": sorted(market_auto_parts),
            "auto_part_conflict": bool(has_auto_part_conflict),
            "auto_part_missing_market": bool(has_auto_part_missing_market),
            "auto_part_missing_source": bool(has_auto_part_missing_source),
            "liquidity_sold_90d": sold_90d,
            "liquidity_pass_label": pass_label,
            "liquidity_sold_price_min": round(sold_price_min, 4) if sold_price_min > 0 else None,
            "liquidity_sold_price_min_raw": round(sold_price_min_raw, 4) if sold_price_min_raw > 0 else None,
            "liquidity_sold_price_min_outlier": bool(sold_price_min_outlier),
        }

    return "approve", [], f"同一商品かつ利益条件を満たす ({confidence_tag})", {
        "profit": profit,
        "margin": margin,
        "ev90_score": round(ev90_score, 4) if ev90_present else None,
        "score": score,
        "rematch_score": round(rematch_score, 4),
        "rematch_reason": rematch_reason,
        "fetch_match_reason": fetch_reason,
        "identifier_exact_key": id_key,
        "identifier_exact_value": id_value,
        "confidence_tag": confidence_tag,
        "common_codes": common_codes,
        "common_specific_codes": common_specific_codes,
        "token_jaccard": round(token_jaccard, 4),
        "source_colors": sorted(source_colors),
        "market_colors": sorted(market_colors),
        "source_auto_parts": sorted(source_auto_parts),
        "market_auto_parts": sorted(market_auto_parts),
        "auto_part_conflict": bool(has_auto_part_conflict),
        "auto_part_missing_market": bool(has_auto_part_missing_market),
        "auto_part_missing_source": bool(has_auto_part_missing_source),
        "liquidity_sold_90d": sold_90d,
        "liquidity_pass_label": pass_label,
        "liquidity_sold_price_min": round(sold_price_min, 4) if sold_price_min > 0 else None,
        "liquidity_sold_price_min_raw": round(sold_price_min_raw, 4) if sold_price_min_raw > 0 else None,
        "liquidity_sold_price_min_outlier": bool(sold_price_min_outlier),
    }


def load_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"manifest not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest must be object")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Auto review active cycle.")
    parser.add_argument(
        "--active-manifest",
        default=str(ROOT_DIR / "docs" / "miner_cycle_active.json"),
    )
    parser.add_argument(
        "--output",
        default=str(ROOT_DIR / "docs" / "miner_cycle_auto_miner_latest.json"),
    )
    parser.add_argument("--min-profit-usd", type=float, default=0.01)
    parser.add_argument("--min-margin-rate", type=float, default=0.03)
    parser.add_argument("--min-ev90-usd", type=float, default=0.0)
    parser.add_argument("--min-match-score", type=float, default=0.75)
    parser.add_argument("--min-auto-approve-score", type=float, default=0.90)
    parser.add_argument("--min-token-jaccard", type=float, default=0.62)
    parser.add_argument("--max-score-drift", type=float, default=0.25)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    auto_policy = {
        "require_liquidity_signal": _env_bool("AUTO_MINER_REQUIRE_LIQUIDITY_SIGNAL", True),
        "allow_fallback_any": _env_bool("AUTO_MINER_ALLOW_FALLBACK_ANY", True),
        "fallback_min_sold_90d": max(0, _env_int("AUTO_MINER_FALLBACK_MIN_SOLD_90D", 5)),
        "fallback_min_profit_usd": _env_float("AUTO_MINER_FALLBACK_MIN_PROFIT_USD", 20.0),
    }
    manifest = load_manifest(Path(args.active_manifest))
    candidate_ids = [int(v) for v in (manifest.get("selected_candidate_ids") or [])]
    if not candidate_ids:
        report = {
            "cycle_id": manifest.get("cycle_id"),
            "ran_at": _now_iso(),
            "dry_run": bool(args.dry_run),
            "thresholds": {
                "min_profit_usd": float(args.min_profit_usd),
                "min_margin_rate": float(args.min_margin_rate),
                "min_ev90_usd": float(args.min_ev90_usd),
                "min_match_score": float(args.min_match_score),
                "min_auto_approve_score": float(args.min_auto_approve_score),
                "min_token_jaccard": float(args.min_token_jaccard),
                "max_score_drift": float(args.max_score_drift),
                "auto_policy": auto_policy,
            },
            "counts": {"approve": 0, "reject": 0, "skipped": 0},
            "decisions": [],
            "note": "selected_candidate_ids is empty; nothing to review",
        }
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved auto miner report: {output_path}")
        print(f"cycle={manifest.get('cycle_id')} approve=0 reject=0 skipped=0 (empty batch)")
        return 0

    decisions: List[Dict[str, Any]] = []
    counts = {"approve": 0, "reject": 0, "skipped": 0}
    for candidate_id in candidate_ids:
        candidate = get_miner_candidate(candidate_id)
        if candidate is None:
            counts["skipped"] += 1
            decisions.append({"id": candidate_id, "decision": "skip", "reason": "not_found"})
            continue
        status = str(candidate.get("status") or "")
        if status != "pending":
            counts["skipped"] += 1
            decisions.append({"id": candidate_id, "decision": "skip", "reason": f"status={status}"})
            continue

        decision, issues, reason, metrics = evaluate_candidate(
            candidate,
            min_profit_usd=float(args.min_profit_usd),
            min_margin_rate=float(args.min_margin_rate),
            min_ev90_usd=float(args.min_ev90_usd),
            min_match_score=float(args.min_match_score),
            min_auto_approve_score=float(args.min_auto_approve_score),
            min_token_jaccard=float(args.min_token_jaccard),
            max_score_drift=float(args.max_score_drift),
        )
        if not args.dry_run:
            if decision == "approve":
                auto_approve_miner_candidate(
                    candidate_id,
                    cycle_id=str(manifest.get("cycle_id", "") or ""),
                    decision_reason=reason,
                    decision_metrics=metrics,
                )
            else:
                reject_miner_candidate(candidate_id, issue_targets=issues, reason_text=reason)

        counts[decision] += 1
        decisions.append(
            {
                "id": candidate_id,
                "decision": decision,
                "issues": issues,
                "reason": reason,
                "metrics": metrics,
                "source_title": candidate.get("source_title"),
                "market_title": candidate.get("market_title"),
            }
        )

    report = {
        "cycle_id": manifest.get("cycle_id"),
        "ran_at": _now_iso(),
        "dry_run": bool(args.dry_run),
        "thresholds": {
            "min_profit_usd": float(args.min_profit_usd),
            "min_margin_rate": float(args.min_margin_rate),
            "min_ev90_usd": float(args.min_ev90_usd),
            "min_match_score": float(args.min_match_score),
            "min_auto_approve_score": float(args.min_auto_approve_score),
            "min_token_jaccard": float(args.min_token_jaccard),
            "max_score_drift": float(args.max_score_drift),
            "auto_policy": auto_policy,
        },
        "counts": counts,
        "decisions": decisions,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved auto miner report: {output_path}")
    print(
        f"cycle={manifest.get('cycle_id')} approve={counts['approve']} "
        f"reject={counts['reject']} skipped={counts['skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
