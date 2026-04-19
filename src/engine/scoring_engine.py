"""
MintBot — Scoring Engine v5
═══════════════════════════════════════
Evidence-based scoring with lane-specific formulas and explanation generation.

Three scoring systems:
  1. Discovery Score  — for fresh/non-blue-chip collections
  2. Upcoming Score   — for upcoming mints
  3. Blue-Chip Impact — for blue-chip catalyst alerts

Every score includes human-readable reason bullets.
"""

import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger("mintbot.scoring")

# ═══════════════════════════════════════════════════════
#  Score Labels
# ═══════════════════════════════════════════════════════

def _label_from_score(score: int) -> tuple:
    """Convert numeric score to (label, color, trend_description)."""
    if score >= 75:
        return ("Strong Buy", "#22c55e")
    elif score >= 60:
        return ("Buy", "#4ade80")
    elif score >= 45:
        return ("Watch", "#f59e0b")
    elif score >= 30:
        return ("Risky", "#f97316")
    else:
        return ("Avoid", "#ef4444")


def _risk_label(score: int, owners: int, verified: bool, risk_flags: list) -> str:
    """Determine risk level from score and context."""
    flag_count = len(risk_flags)
    if flag_count >= 3:
        return "Critical"
    if score >= 65 and owners > 500 and verified:
        return "Low"
    if score >= 50 and flag_count == 0:
        return "Medium"
    if score >= 35:
        return "Medium-High"
    return "High"


# ═══════════════════════════════════════════════════════
#  1. DISCOVERY SCORE
# ═══════════════════════════════════════════════════════
# Used for: fresh_discovery, good_calls lanes
#
# Weights:
#   market_momentum   : 22%
#   social_momentum   : 18%
#   source_quality    : 18%
#   community_growth  : 12%
#   holder_quality    : 8%
#   originality       : 8%
#   trust_risk        : 8%
#   catalyst_strength : 6%

def score_discovery(
    col: Dict,
    news_items: List[Dict] = None,
    originality: Dict = None,
    risk_flags: List[str] = None,
) -> Dict:
    """Score a non-blue-chip collection for discovery ranking.

    Returns:
        {
            "score": int (0-100),
            "label": str,
            "label_color": str,
            "trend": "up" | "down" | "stable",
            "risk": str,
            "reasons": [str],
            "breakdown": {component: score},
        }
    """
    stats = col.get("_stats", {})
    mint_info = col.get("_mint_info", {})
    news_items = news_items or []
    originality = originality or {"label": "original", "confidence": 0.5}
    risk_flags = risk_flags or []

    breakdown = {}
    reasons = []

    # ── Market Momentum (0-22) ──
    vol_1d = stats.get("volume_1d", 0)
    vol_change = stats.get("volume_change_1d", 0)
    sales_1d = stats.get("sales_1d", 0)

    mm = 0
    if vol_1d > 100:
        mm = 18
    elif vol_1d > 50:
        mm = 15
    elif vol_1d > 10:
        mm = 12
    elif vol_1d > 1:
        mm = 8
    elif vol_1d > 0.1:
        mm = 4

    # Volume growth bonus
    if vol_change > 100:
        mm = min(22, mm + 4)
        reasons.append(f"24h volume surging +{vol_change:.0f}%")
    elif vol_change > 50:
        mm = min(22, mm + 2)
        reasons.append(f"24h volume up +{vol_change:.0f}%")
    elif vol_change < -30:
        mm = max(0, mm - 3)
        reasons.append(f"24h volume declining {vol_change:.0f}%")

    if vol_1d > 10:
        reasons.append(f"{vol_1d:.1f} ETH in 24h volume")

    breakdown["market_momentum"] = mm

    # ── Social Momentum (0-18) ──
    sm = 0
    high_conf_news = [n for n in news_items if n.get("confidence", 0) >= 0.6]
    any_news = len(news_items)

    if len(high_conf_news) >= 3:
        sm = 18
        reasons.append(f"{len(high_conf_news)} high-confidence sources active")
    elif len(high_conf_news) >= 2:
        sm = 14
    elif len(high_conf_news) >= 1:
        sm = 10
    elif any_news >= 2:
        sm = 6
    elif any_news >= 1:
        sm = 3

    # Check for bullish sentiment
    bullish = sum(1 for n in news_items if n.get("sentiment") == "bullish")
    if bullish >= 2:
        sm = min(18, sm + 2)
        reasons.append(f"{bullish} bullish signals from sources")

    breakdown["social_momentum"] = sm

    # ── Source Quality (0-18) ──
    sq = 0
    has_official = any(n.get("source_type") in ("official", "team") for n in news_items)
    has_marketplace = any(n.get("source_type") == "marketplace" for n in news_items)
    has_influencer = any(n.get("source_type") == "influencer" for n in news_items)

    if has_official:
        sq += 10
        reasons.append("Official source activity detected")
    if has_marketplace:
        sq += 5
    if has_influencer:
        sq += 4
    sq = min(18, sq)

    breakdown["source_quality"] = sq

    # ── Community Growth (0-12) ──
    owners = stats.get("num_owners", 0)
    cg = 0
    if owners > 5000:
        cg = 12
    elif owners > 1000:
        cg = 9
        reasons.append(f"{owners:,} holders — strong community")
    elif owners > 300:
        cg = 6
    elif owners > 50:
        cg = 3
    breakdown["community_growth"] = cg

    # ── Holder Quality (0-8) ──
    hq = 0
    has_twitter = bool(col.get("twitter"))
    has_discord = bool(col.get("discord"))
    verified = col.get("safelist", "") == "verified"
    if verified:
        hq += 4
    if has_twitter:
        hq += 2
    if has_discord:
        hq += 2
    hq = min(8, hq)
    if verified:
        reasons.append("Verified on OpenSea")
    breakdown["holder_quality"] = hq

    # ── Originality (0-8) ──
    orig_label = originality.get("label", "original")
    if orig_label == "original":
        orig_score = 8
    elif orig_label == "inspired":
        orig_score = 5
    elif orig_label == "derivative":
        orig_score = 2
        reasons.append("Derivative of existing archetype — lower originality")
    else:  # copycat
        orig_score = 0
        reasons.append("Low originality — appears to copy existing concepts")
    breakdown["originality"] = orig_score

    # ── Trust / Risk (0-8) ──
    tr = 8
    for flag in risk_flags[:2]:
        tr = max(0, tr - 3)
        reasons.append(f"⚠️ {flag}")
    breakdown["trust_risk"] = tr

    # ── Catalyst Strength (0-6) ──
    cs = 0
    if mint_info.get("is_minting"):
        cs = 6
        m24 = mint_info.get("mint_count_24h", 0)
        reasons.append(f"🟢 Active minting detected ({m24} in 24h)")
    elif sales_1d > 20:
        cs = 4
        reasons.append(f"{sales_1d} trades in 24h — active market")
    elif sales_1d > 5:
        cs = 2
    breakdown["catalyst_strength"] = cs

    # ── Total ──
    total = sum(breakdown.values())
    total = max(0, min(100, total))

    label, color = _label_from_score(total)

    # Trend direction
    if vol_change > 20:
        trend = "up"
    elif vol_change < -20:
        trend = "down"
    else:
        trend = "stable"

    risk = _risk_label(total, owners, verified, risk_flags)

    # Cap reasons at 4
    reasons = reasons[:4]

    return {
        "score": total,
        "label": label,
        "label_color": color,
        "trend": trend,
        "risk": risk,
        "reasons": reasons,
        "breakdown": breakdown,
    }


# ═══════════════════════════════════════════════════════
#  2. UPCOMING SCORE
# ═══════════════════════════════════════════════════════
# Used for: upcoming mints
#
# Weights:
#   announcement_quality   : 20%
#   source_confidence      : 20%
#   social_traction        : 15%
#   team_credibility       : 10%
#   originality            : 10%
#   launch_readiness       : 10%
#   community_interest     : 10%
#   risk_flags             : 5%

def score_upcoming(
    mint_info: Dict,
    news_items: List[Dict] = None,
    originality: Dict = None,
) -> Dict:
    """Score an upcoming mint collection.

    Returns same structure as score_discovery.
    """
    news_items = news_items or []
    originality = originality or {"label": "original", "confidence": 0.5}

    breakdown = {}
    reasons = []

    # ── Announcement Quality (0-20) ──
    aq = 0
    desc = (mint_info.get("description", "") or "").lower()
    has_price = bool(mint_info.get("mint_price"))
    has_date = bool(mint_info.get("published") or mint_info.get("mint_date"))
    has_link = bool(mint_info.get("link"))

    if has_price and has_date and has_link:
        aq = 20
        reasons.append("Complete mint details available")
    elif has_price and has_link:
        aq = 14
    elif has_link:
        aq = 8
    else:
        aq = 3
    breakdown["announcement_quality"] = aq

    # ── Source Confidence (0-20) ──
    sc = 0
    source = (mint_info.get("source", "") or "").lower()
    if "official" in source or any(n.get("source_type") == "official" for n in news_items):
        sc = 20
        reasons.append("Official or near-official source")
    elif "calendar" in source or "nftcalendar" in source:
        sc = 14
    elif "jina" in source:
        sc = 10
    else:
        sc = 6
    breakdown["source_confidence"] = sc

    # ── Social Traction (0-15) ──
    st = 0
    bullish_news = [n for n in news_items if n.get("sentiment") == "bullish"]
    if len(bullish_news) >= 2:
        st = 15
        reasons.append("Multiple bullish social signals")
    elif len(bullish_news) >= 1:
        st = 10
    elif len(news_items) >= 1:
        st = 5
    breakdown["social_traction"] = st

    # ── Team Credibility (0-10) ──
    tc = 5  # Default moderate
    # If we have news from official/team sources, boost
    if any(n.get("source_type") in ("official", "team") for n in news_items):
        tc = 10
    breakdown["team_credibility"] = tc

    # ── Originality (0-10) ──
    orig_label = originality.get("label", "original")
    if orig_label == "original":
        ol = 10
    elif orig_label == "inspired":
        ol = 7
    elif orig_label == "derivative":
        ol = 3
        reasons.append("Derivative concept — lower originality score")
    else:
        ol = 1
    breakdown["originality"] = ol

    # ── Launch Readiness (0-10) ──
    lr = 0
    if has_price:
        lr += 4
    if mint_info.get("chain"):
        lr += 3
    if len(desc) > 30:
        lr += 3
    lr = min(10, lr)
    breakdown["launch_readiness"] = lr

    # ── Community Interest (0-10) ──
    ci = min(10, len(news_items) * 3)
    breakdown["community_interest"] = ci

    # ── Risk Flags (0-5, higher = better) ──
    rf = 5
    if not has_link:
        rf -= 2
        reasons.append("⚠️ No direct mint link available")
    if orig_label in ("derivative", "copycat"):
        rf -= 2
    breakdown["risk_flags"] = max(0, rf)

    total = sum(breakdown.values())
    total = max(0, min(100, total))

    label, color = _label_from_score(total)

    return {
        "score": total,
        "label": label,
        "label_color": color,
        "trend": "stable",
        "risk": "Medium" if total >= 50 else "High",
        "reasons": reasons[:4],
        "breakdown": breakdown,
    }


# ═══════════════════════════════════════════════════════
#  3. BLUE-CHIP IMPACT SCORE
# ═══════════════════════════════════════════════════════
# Used for: blue_chip_alerts lane ONLY
# This is NOT "how good is this blue chip"
# This is "WHY should I care about this blue chip RIGHT NOW"
#
# Weights:
#   catalyst_significance  : 35%
#   source_credibility     : 20%
#   social_acceleration    : 15%
#   market_reaction        : 15%
#   sentiment_quality      : 10%
#   recency_decay          : 5%

# Catalyst types and their base importance weights
CATALYST_IMPORTANCE = {
    "launch": 30,
    "mint_live": 28,
    "partnership": 25,
    "game_product": 22,
    "token_utility": 20,
    "exploit_security": 30,
    "controversy": 25,
    "volume_spike": 18,
    "whale_activity": 15,
    "roadmap_update": 12,
    "art_reveal": 10,
    "allowlist": 8,
    "sellout": 20,
}


def score_bluechip_impact(
    col: Dict,
    catalysts: List[Dict] = None,
) -> Dict:
    """Score a blue-chip collection for impact/alert ranking.

    Only called when we believe there's news worth alerting on.

    Args:
        col: Collection data with _stats
        catalysts: List of catalyst news items matched to this collection

    Returns:
        {
            "impact": int (0-100),
            "label": str,
            "label_color": str,
            "direction": "positive" | "negative" | "neutral",
            "reasons": [str],
            "breakdown": {component: score}
        }
    """
    catalysts = catalysts or []
    stats = col.get("_stats", {})
    breakdown = {}
    reasons = []

    if not catalysts:
        return {
            "impact": 0,
            "label": "No Alert",
            "label_color": "#6b7280",
            "direction": "neutral",
            "reasons": ["No fresh catalysts detected"],
            "breakdown": {},
        }

    # ── Catalyst Significance (0-35) ──
    max_catalyst_weight = 0
    best_catalyst = None
    for cat in catalysts:
        cat_type = cat.get("catalyst", "")
        weight = CATALYST_IMPORTANCE.get(cat_type, 5)
        if weight > max_catalyst_weight:
            max_catalyst_weight = weight
            best_catalyst = cat

    cs = min(35, max_catalyst_weight)
    if best_catalyst:
        cat_label = best_catalyst.get("catalyst", "event").replace("_", " ").title()
        reasons.append(f"Catalyst: {cat_label}")
    breakdown["catalyst_significance"] = cs

    # ── Source Credibility (0-20) ──
    sc = 0
    official_count = sum(1 for c in catalysts if c.get("source_type") in ("official", "team"))
    influencer_count = sum(1 for c in catalysts if c.get("source_type") == "influencer")
    marketplace_count = sum(1 for c in catalysts if c.get("source_type") == "marketplace")

    if official_count >= 1:
        sc = 20
        reasons.append("Confirmed by official/team source")
    elif marketplace_count >= 1:
        sc = 15
    elif influencer_count >= 2:
        sc = 12
        reasons.append(f"{influencer_count} influencer sources")
    elif influencer_count >= 1:
        sc = 8
    else:
        sc = 4
    breakdown["source_credibility"] = sc

    # ── Social Acceleration (0-15) ──
    sa = min(15, len(catalysts) * 4)
    if len(catalysts) >= 3:
        reasons.append(f"Multi-source confirmation ({len(catalysts)} sources)")
    breakdown["social_acceleration"] = sa

    # ── Market Reaction (0-15) ──
    vol_change = stats.get("volume_change_1d", 0)
    mr = 0
    if abs(vol_change) > 50:
        mr = 15
        direction = "up" if vol_change > 0 else "down"
        reasons.append(f"Volume {direction} {abs(vol_change):.0f}%")
    elif abs(vol_change) > 20:
        mr = 10
    elif abs(vol_change) > 5:
        mr = 5
    breakdown["market_reaction"] = mr

    # ── Sentiment Quality (0-10) ──
    bullish = sum(1 for c in catalysts if c.get("sentiment") == "bullish")
    bearish = sum(1 for c in catalysts if c.get("sentiment") == "bearish")
    sq = 5  # neutral baseline
    if bullish > bearish:
        sq = min(10, 5 + bullish * 2)
    elif bearish > bullish:
        sq = max(0, 5 - bearish * 2)
    breakdown["sentiment_quality"] = sq

    # ── Recency Decay (0-5) ──
    # Newest catalyst should be recent
    rd = 5  # assume recent since we only process recent data
    breakdown["recency_decay"] = rd

    # Total
    total = sum(breakdown.values())
    total = max(0, min(100, total))

    # Direction
    if bullish > bearish:
        direction = "positive"
    elif bearish > bullish:
        direction = "negative"
    else:
        direction = "neutral"

    # Label
    if total >= 60:
        label = "Major Alert"
        color = "#ef4444" if direction == "negative" else "#22c55e"
    elif total >= 40:
        label = "Notable"
        color = "#f59e0b"
    elif total >= 20:
        label = "Minor"
        color = "#6b7280"
    else:
        label = "Noise"
        color = "#4b5563"

    return {
        "impact": total,
        "label": label,
        "label_color": color,
        "direction": direction,
        "reasons": reasons[:4],
        "breakdown": breakdown,
    }


# ═══════════════════════════════════════════════════════
#  Blue-Chip Alert Threshold
# ═══════════════════════════════════════════════════════

# Minimum impact score for a blue chip to appear in alerts
BLUECHIP_ALERT_THRESHOLD = 25


def should_alert_bluechip(impact_score: int) -> bool:
    """Check if a blue-chip impact score warrants showing an alert."""
    return impact_score >= BLUECHIP_ALERT_THRESHOLD


# ═══════════════════════════════════════════════════════
#  Freshness Decay
# ═══════════════════════════════════════════════════════

def freshness_modifier(hours_old: float) -> float:
    """Calculate freshness decay modifier (0.0 - 1.0).

    - 0-1h: 1.0 (full weight)
    - 1-6h: 0.8 decay
    - 6-12h: 0.5 decay
    - 12-24h: 0.3 decay
    - 24h+: 0.1 decay
    """
    if hours_old <= 1:
        return 1.0
    elif hours_old <= 6:
        return 0.8
    elif hours_old <= 12:
        return 0.5
    elif hours_old <= 24:
        return 0.3
    else:
        return 0.1


def score_with_freshness(base_score: int, hours_old: float) -> int:
    """Apply freshness modifier to a score."""
    modifier = freshness_modifier(hours_old)
    return max(0, min(100, int(base_score * modifier)))
