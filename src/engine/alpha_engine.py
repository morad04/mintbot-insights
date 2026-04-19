"""
MintBot — Alpha Engine v1
═══════════════════════════════════════
NFT Opportunity Scoring Framework.

8 intelligence layers → weighted composite → decision engine.
Every collection gets: Opportunity Score, Confidence, Risk, Timing, Action.

This is NOT a stats dashboard. This is a trading decision engine.
"""

import logging
import math
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Tuple
from collections import Counter

logger = logging.getLogger("mintbot.alpha")


# ═══════════════════════════════════════════════════════
#  Data Models
# ═══════════════════════════════════════════════════════

@dataclass
class LayerResult:
    """Result from a single intelligence layer."""
    name: str = ""
    score: int = 0          # 0-100 (or negative for risk)
    confidence: int = 0     # 0-100 — how much data backs this score
    evidence: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class OpportunityCard:
    """Complete decision output for one collection."""
    # Identity
    slug: str = ""
    name: str = ""
    chain: str = ""
    chain_label: str = ""
    image: str = ""
    contract: str = ""
    verified: bool = False
    owners: int = 0

    # Scores
    opportunity_score: int = 0
    confidence: int = 0
    risk_score: int = 0
    grade: str = "D"        # A / B / C / D / AVOID

    # Decision
    action: str = "WATCH"   # BUY NOW / WATCH CLOSELY / TOO EARLY / TOO LATE / AVOID / EXIT
    action_color: str = "#f59e0b"
    signal_type: str = ""   # Early Accumulation, Breakout Setup, etc.
    timing: str = ""        # Optimal / Too Early / Late / Dangerous
    time_horizon: str = ""  # 1h-6h, 6h-24h, multi-day

    # Debug trace
    debug: Dict = field(default_factory=dict)

    # Layer breakdown
    layers: Dict[str, Dict] = field(default_factory=dict)

    # Evidence
    top_reasons: List[str] = field(default_factory=list)
    why_now: List[str] = field(default_factory=list)

    # Entry / Exit
    entry_zone: str = ""
    invalidation: str = ""
    exit_logic: str = ""

    # Market data
    floor_price: float = 0.0
    floor_symbol: str = "ETH"
    volume_1d: float = 0.0
    volume_7d: float = 0.0
    market_cap: float = 0.0
    total_listings: int = 0
    listings_near_floor: int = 0

    # Links
    opensea_url: str = ""
    explorer_url: str = ""
    twitter: str = ""
    discord: str = ""
    website: str = ""

    # Metadata
    data_freshness: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class MarketRegime:
    """Current NFT market conditions."""
    regime: str = "CHOP"    # RISK_ON / RISK_OFF / ROTATION / CHOP
    regime_label: str = "Low Conviction"
    confidence: int = 50
    blue_chip_vol_change: float = 0.0
    market_breadth: float = 0.0  # % of collections with positive vol change
    floor_trend: float = 0.0
    evidence: List[str] = field(default_factory=list)
    weight_adjustments: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class DashboardHeader:
    """Global dashboard summary."""
    regime: Dict = field(default_factory=dict)
    best_opportunity: Dict = field(default_factory=dict)
    highest_conviction: Dict = field(default_factory=dict)
    best_early_entry: Dict = field(default_factory=dict)
    most_dangerous: Dict = field(default_factory=dict)
    best_post_mint: Dict = field(default_factory=dict)
    total_analyzed: int = 0
    alpha_count: int = 0
    updated_at: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


# ═══════════════════════════════════════════════════════
#  Layer Weights
# ═══════════════════════════════════════════════════════

LAYER_WEIGHTS = {
    "smart_money":    0.20,
    "early_momentum": 0.25,
    "supply_shock":   0.15,
    "volume_anomaly": 0.15,
    "mint_quality":   0.10,
    "holder_quality": 0.10,
    "narrative":      0.05,
    # risk is applied as subtraction, NOT included in positive sum
}

RISK_WEIGHT = 0.10  # Reduced from 0.15 — less aggressive penalty

ACTION_COLORS = {
    "BUY NOW": "#22c55e",
    "WATCH CLOSELY": "#f59e0b",
    "TOO EARLY": "#60a5fa",
    "TOO LATE": "#ef4444",
    "AVOID": "#ef4444",
    "EXIT": "#f97316",
}

# Recalibrated to match realistic score distribution
GRADE_THRESHOLDS = [
    (60, 0, "A"),
    (45, 0, "B"),
    (30, 0, "C"),
    (15, 0, "D"),
]


# ═══════════════════════════════════════════════════════
#  Layer 1: Smart Money
# ═══════════════════════════════════════════════════════

def score_smart_money(
    sales: List[Any],
    wallet_db: Dict[str, Dict],
) -> LayerResult:
    """Detect high-quality wallet accumulation.

    When wallet_db is empty (cold start), returns low confidence.
    """
    result = LayerResult(name="smart_money", confidence=0)

    if not sales:
        result.evidence.append("No recent sales data")
        return result

    if not wallet_db:
        result.confidence = 10
        result.evidence.append("Wallet DB bootstrapping — insufficient history")
        return result

    now = int(time.time())
    score = 0
    smart_buyers = []
    smart_sellers = []
    recent_window = 7200  # 2 hours

    for sale in sales:
        buyer = sale.buyer if hasattr(sale, 'buyer') else sale.get("buyer", "")
        seller = sale.seller if hasattr(sale, 'seller') else sale.get("seller", "")
        ts = sale.timestamp if hasattr(sale, 'timestamp') else sale.get("timestamp", 0)

        buyer_intel = wallet_db.get(buyer, {})
        seller_intel = wallet_db.get(seller, {})

        # Smart buyer detection
        if buyer_intel.get("tier") in ("smart", "active"):
            win_rate = buyer_intel.get("win_rate", 0)
            if win_rate >= 60 and (now - ts) < recent_window:
                smart_buyers.append({
                    "address": buyer[:8] + "..." + buyer[-4:] if len(buyer) > 12 else buyer,
                    "win_rate": win_rate,
                    "ago": _seconds_to_label(now - ts),
                })

        # Smart seller detection (distribution warning)
        if seller_intel.get("tier") == "smart":
            if (now - ts) < recent_window:
                smart_sellers.append(seller)

    # Score calculation
    if len(smart_buyers) >= 3:
        score += 75
        result.evidence.append(
            f"{len(smart_buyers)} smart wallets accumulated in last 2h "
            f"(highest win rate: {max(b['win_rate'] for b in smart_buyers):.0f}%)"
        )
    elif len(smart_buyers) >= 2:
        score += 55
        result.evidence.append(
            f"{len(smart_buyers)} quality wallets buying — cluster signal"
        )
    elif len(smart_buyers) >= 1:
        score += 30
        b = smart_buyers[0]
        result.evidence.append(
            f"Wallet {b['address']} ({b['win_rate']:.0f}% win rate) bought {b['ago']}"
        )

    # Distribution penalty
    if smart_sellers:
        penalty = min(40, len(smart_sellers) * 20)
        score -= penalty
        result.evidence.append(
            f"⚠️ {len(smart_sellers)} smart wallets SELLING — distribution risk"
        )

    result.score = max(0, min(100, score))
    result.confidence = min(80, 20 + len(wallet_db) // 10)
    return result


# ═══════════════════════════════════════════════════════
#  Layer 2: Early Momentum (Before Breakout)
# ═══════════════════════════════════════════════════════

def score_early_momentum(
    stats: Dict,
    sales: List[Any],
    prev_snapshot: Optional[Dict] = None,
) -> LayerResult:
    """Detect rising demand BEFORE floor reprices."""
    result = LayerResult(name="early_momentum", confidence=40)
    score = 0

    now = int(time.time())
    vol_1d = stats.get("volume_1d", 0)
    vol_7d = stats.get("volume_7d", 0)
    vol_30d = stats.get("volume_30d", 0)
    sales_1d = stats.get("sales_1d", 0)
    sales_7d = stats.get("sales_7d", 0)
    floor = stats.get("floor_price", 0)

    # Unique buyers in last 1h and 6h from sales data
    buyers_1h = set()
    buyers_6h = set()
    for sale in sales:
        ts = sale.timestamp if hasattr(sale, 'timestamp') else sale.get("timestamp", 0)
        buyer = sale.buyer if hasattr(sale, 'buyer') else sale.get("buyer", "")
        if not buyer:
            continue
        age = now - ts
        if age < 3600:
            buyers_1h.add(buyer)
        if age < 21600:
            buyers_6h.add(buyer)

    unique_1h = len(buyers_1h)
    unique_6h = len(buyers_6h)

    # Buyer acceleration
    baseline_hourly = unique_6h / 6 if unique_6h > 0 else 0
    if baseline_hourly > 0 and unique_1h > 0:
        acceleration = unique_1h / baseline_hourly
        if acceleration > 3:
            score += 40
            result.evidence.append(
                f"Unique buyers up {acceleration:.1f}x vs 6h avg "
                f"({unique_1h} in 1h vs {baseline_hourly:.1f}/h baseline)"
            )
        elif acceleration > 2:
            score += 25
            result.evidence.append(
                f"Buyer acceleration {acceleration:.1f}x — rising demand"
            )
        elif acceleration > 1.5:
            score += 15

    # Volume vs floor lag (pre-breakout signal)
    daily_avg_7d = vol_7d / 7 if vol_7d > 0 else 0
    if daily_avg_7d > 0 and vol_1d > 0:
        vol_ratio = vol_1d / daily_avg_7d
        if vol_ratio > 2:
            # DENOMINATOR GUARD: cap bonus when volume base is tiny
            vol_lag_cap = 30 if vol_7d >= 1.0 else 10
            if prev_snapshot:
                prev_floor = prev_snapshot.get("floor_price", 0)
                if prev_floor > 0:
                    floor_change_pct = ((floor - prev_floor) / prev_floor) * 100
                    if floor_change_pct < 5 and vol_ratio > 2:
                        bonus = min(vol_lag_cap, 30)
                        score += bonus
                        result.evidence.append(
                            f"24h volume {vol_ratio:.1f}x normal but floor hasn't repriced "
                            f"({floor_change_pct:+.1f}%) — pre-breakout setup"
                        )
                        if vol_7d >= 1.0:
                            result.confidence = max(result.confidence, 65)
                    elif floor_change_pct > 20:
                        score -= 20
                        result.evidence.append(
                            f"Floor already moved +{floor_change_pct:.0f}% — late entry risk"
                        )

    # Sales velocity vs baseline
    daily_avg_sales_7d = sales_7d / 7 if sales_7d > 0 else 0
    if daily_avg_sales_7d > 0 and sales_1d > daily_avg_sales_7d * 2:
        velocity = sales_1d / daily_avg_sales_7d
        # DENOMINATOR GUARD: cap bonus when sales base is tiny
        if sales_7d >= 5:
            score += 20
        elif sales_1d >= 3:
            score += 10  # Reduced bonus for tiny baseline
        else:
            score += 5   # Minimal bonus — could be noise
        result.evidence.append(
            f"{sales_1d} sales today vs {daily_avg_sales_7d:.0f}/day avg — "
            f"{velocity:.1f}x velocity"
        )

    # Raw unique buyer signal (require meaningful count)
    if unique_1h >= 5:
        score += 10
        result.evidence.append(f"{unique_1h} unique buyers in last hour")
    elif unique_1h >= 3 and unique_6h >= 5:
        score += 5

    result.score = max(0, min(100, score))
    if prev_snapshot:
        result.confidence = min(80, result.confidence + 20)
    return result


# ═══════════════════════════════════════════════════════
#  Layer 3: Supply Shock / Floor Pressure
# ═══════════════════════════════════════════════════════

def score_supply_shock(
    floor_depth: Dict,
    prev_snapshot: Optional[Dict] = None,
) -> LayerResult:
    """Detect thinning supply that precedes a floor move."""
    result = LayerResult(name="supply_shock", confidence=50)
    score = 0

    is_thin = floor_depth.get("is_thin", False)
    within_5 = floor_depth.get("listings_within_5pct", 0)
    within_10 = floor_depth.get("listings_within_10pct", 0)
    total = floor_depth.get("total_listings", 0)
    depth_ratio = floor_depth.get("floor_depth_ratio", 0)

    # Critical thinness
    if is_thin:
        score += 35
        result.evidence.append(
            f"Floor is THIN — only {within_10} listings within 10% of floor"
        )
    if within_5 <= 2 and total > 0:
        score += 20
        result.evidence.append(
            f"Critical: only {within_5} listings within 5% — one sweep moves the floor"
        )
    elif within_5 <= 5:
        score += 10

    # Listing removal detection (vs previous snapshot)
    if prev_snapshot:
        prev_listings = prev_snapshot.get("listings_count", 0)
        if prev_listings > 0 and total > 0:
            listing_change = ((total - prev_listings) / prev_listings) * 100
            if listing_change < -30:
                score += 25
                result.evidence.append(
                    f"Listings dropped {listing_change:.0f}% since last check — "
                    f"mass delisting / sweeps"
                )
                result.confidence = max(result.confidence, 70)
            elif listing_change < -15:
                score += 15
                result.evidence.append(
                    f"Listings down {listing_change:.0f}% — supply tightening"
                )
            elif listing_change > 20:
                score -= 15
                result.evidence.append(
                    f"Listings increased +{listing_change:.0f}% — supply rising (bearish)"
                )

    # Depth ratio
    if depth_ratio < 0.1 and total > 10:
        score += 15
        result.evidence.append(
            f"Floor depth ratio {depth_ratio:.2f} — very clustered supply"
        )

    # Maker concentration warning
    top_maker = floor_depth.get("top_maker_pct", 0)
    if top_maker > 50:
        score -= 10
        result.evidence.append(
            f"⚠️ Single maker controls {top_maker:.0f}% of near-floor listings"
        )

    result.score = max(0, min(100, score))
    return result


# ═══════════════════════════════════════════════════════
#  Layer 4: Volume Anomaly
# ═══════════════════════════════════════════════════════

def score_volume_anomaly(stats: Dict) -> LayerResult:
    """Detect unusual volume acceleration across timeframes."""
    result = LayerResult(name="volume_anomaly", confidence=60)
    score = 0

    vol_1d = stats.get("volume_1d", 0)
    vol_7d = stats.get("volume_7d", 0)
    vol_30d = stats.get("volume_30d", 0)
    sales_1d = stats.get("sales_1d", 0)
    floor = stats.get("floor_price", 0)

    daily_avg_7d = vol_7d / 7 if vol_7d > 0 else 0
    daily_avg_30d = vol_30d / 30 if vol_30d > 0 else 0

    # ── DENOMINATOR GUARD: absolute volume floor ──
    # Ratio spikes on tiny baselines are noise, not signal.
    # vol_7d < 1.0 ETH = tiny collection, cap ratio bonuses at +10
    is_tiny_volume = vol_7d < 1.0
    ratio_cap = 10 if is_tiny_volume else 100  # hard cap for tiny collections

    # Volume vs 7d baseline
    if daily_avg_7d > 0 and vol_1d > 0:
        ratio_7d = vol_1d / daily_avg_7d
        if ratio_7d > 5:
            bonus = min(35, ratio_cap)
            score += bonus
            result.evidence.append(
                f"24h volume {ratio_7d:.1f}x the 7-day average — extreme spike"
            )
            if is_tiny_volume:
                result.evidence.append(
                    f"⚠️ Low absolute volume ({vol_7d:.2f} ETH/wk) — ratio may be misleading"
                )
        elif ratio_7d > 3:
            score += min(25, ratio_cap)
            result.evidence.append(
                f"24h volume {ratio_7d:.1f}x normal (7d baseline)"
            )
        elif ratio_7d > 2:
            score += min(15, ratio_cap)
            result.evidence.append(
                f"Volume {ratio_7d:.1f}x vs 7d average"
            )
        elif ratio_7d > 1.5:
            score += min(8, ratio_cap)

    # Volume vs 30d baseline (same guard)
    if daily_avg_30d > 0 and vol_1d > 0:
        ratio_30d = vol_1d / daily_avg_30d
        if ratio_30d > 5:
            score += min(25, ratio_cap)
        elif ratio_30d > 3:
            score += min(15, ratio_cap)
        elif ratio_30d > 2:
            score += min(10, ratio_cap)

    # Anti-wash: check avg sale size vs floor
    if sales_1d > 0 and floor > 0:
        avg_sale_size = vol_1d / sales_1d
        if avg_sale_size > floor * 5:
            score -= 15
            result.evidence.append(
                f"⚠️ Avg sale {avg_sale_size:.4f} ETH is {avg_sale_size/floor:.0f}x floor "
                f"— likely outlier/wash"
            )
            result.confidence -= 15

    # Sustained acceleration check
    if vol_7d > 0 and vol_30d > 0:
        weekly_share = vol_7d / vol_30d
        if weekly_share > 0.5:  # Last 7d = >50% of last 30d volume
            score += 10
            result.evidence.append(
                "Volume accelerating week-over-week — sustained momentum"
            )

    # Zero volume penalty
    if vol_1d == 0:
        score = 0
        result.evidence.append("No 24h volume — dead market")
        result.confidence = 80  # high confidence it's dead

    # Absolute volume bonus: reward collections with real liquidity
    if vol_1d >= 10:
        score += 5
    elif vol_1d >= 1:
        score += 2

    result.score = max(0, min(100, score))
    return result


# ═══════════════════════════════════════════════════════
#  Layer 5: Mint / Primary Market Edge
# ═══════════════════════════════════════════════════════

def score_mint_quality(
    mint_info: Dict,
    stats: Dict,
    drop_details: Optional[Dict] = None,
) -> LayerResult:
    """Evaluate live mints for secondary market potential."""
    result = LayerResult(name="mint_quality", confidence=50)

    if not mint_info.get("is_minting"):
        result.score = 0
        result.confidence = 80
        result.evidence.append("Not currently minting")
        return result

    score = 20  # base for active mint
    m1h = mint_info.get("mint_count_1h", 0)
    m6h = mint_info.get("mint_count_6h", 0)
    m24h = mint_info.get("mint_count_24h", 0)
    floor = stats.get("floor_price", 0)

    # Mint velocity
    if m1h > 50:
        score += 20
        result.evidence.append(f"{m1h} mints in last hour — very high velocity")
    elif m1h > 20:
        score += 15
        result.evidence.append(f"{m1h} mints in last hour — strong velocity")
    elif m1h > 5:
        score += 10
        result.evidence.append(f"{m1h} mints in last hour")

    # Supply pressure from MCP
    if drop_details:
        supply_pct = drop_details.get("supply_pct", 0)
        if supply_pct > 80:
            score += 15
            result.evidence.append(
                f"Supply {supply_pct:.0f}% minted — scarcity approaching"
            )
            result.confidence += 10
        elif supply_pct > 50:
            score += 10

        # Price check
        mint_price = drop_details.get("active_price", 0)
        if mint_price > 0 and floor > 0:
            if floor > mint_price * 1.2:
                score += 20
                result.evidence.append(
                    f"Secondary floor {floor:.4f} ETH is {floor/mint_price:.1f}x mint price "
                    f"— strong post-mint demand"
                )
            elif floor < mint_price:
                score -= 15
                result.evidence.append(
                    f"⚠️ Trading BELOW mint price — weak demand"
                )

    result.score = max(0, min(100, score))
    return result


# ═══════════════════════════════════════════════════════
#  Layer 6: Holder Quality
# ═══════════════════════════════════════════════════════

def score_holder_quality(
    stats: Dict,
    sales: List[Any],
    floor_depth: Dict,
) -> LayerResult:
    """Assess holder base stability and sell pressure."""
    result = LayerResult(name="holder_quality", confidence=50)
    score = 50  # neutral baseline

    owners = stats.get("num_owners", 0)
    total_listings = floor_depth.get("total_listings", 0)
    sales_1d = stats.get("sales_1d", 0)

    # Listing ratio (listed / owners)
    if owners > 0 and total_listings > 0:
        listing_ratio = total_listings / owners
        if listing_ratio < 0.05:
            score += 30
            result.evidence.append(
                f"Only {listing_ratio*100:.1f}% listed — very low sell pressure"
            )
        elif listing_ratio < 0.10:
            score += 20
            result.evidence.append(
                f"{listing_ratio*100:.1f}% supply listed — healthy holder conviction"
            )
        elif listing_ratio < 0.20:
            score += 10
        elif listing_ratio > 0.40:
            score -= 20
            result.evidence.append(
                f"⚠️ {listing_ratio*100:.0f}% of collection listed — heavy sell pressure"
            )

    # Seller concentration (repeat sellers in recent sales)
    seller_counts: Counter = Counter()
    for sale in sales:
        seller = sale.seller if hasattr(sale, 'seller') else sale.get("seller", "")
        if seller:
            seller_counts[seller] += 1

    heavy_sellers = sum(1 for c in seller_counts.values() if c >= 3)
    if heavy_sellers > 3:
        score -= 15
        result.evidence.append(
            f"⚠️ {heavy_sellers} wallets dumping (3+ sells each)"
        )
    elif heavy_sellers == 0 and len(seller_counts) > 5:
        score += 10
        result.evidence.append("Distributed selling — no single dumper")

    # Daily turnover
    if owners > 0 and sales_1d > 0:
        turnover = sales_1d / owners
        if turnover < 0.01:
            score += 10
            result.evidence.append("Low daily turnover — diamond hand holder base")
        elif turnover > 0.10:
            score -= 10
            result.evidence.append(
                f"High turnover ({turnover*100:.1f}% of holders traded today)"
            )

    result.score = max(0, min(100, score))
    return result


# ═══════════════════════════════════════════════════════
#  Layer 7: Narrative / Social
# ═══════════════════════════════════════════════════════

def score_narrative(
    col_data: Dict,
    news_items: List[Dict],
) -> LayerResult:
    """Social confirmation — LOW weight, intentionally.

    Only rewarded when real on-chain activity confirms it.
    """
    result = LayerResult(name="narrative", confidence=30)
    score = 5  # baseline — absence of social isn't disqualifying

    if not news_items:
        result.evidence.append("No social signals detected")
        return result

    real_tweets = [n for n in news_items if n.get("is_real_tweet") or n.get("source_type") == "influencer"]
    bullish = [n for n in news_items if n.get("sentiment") == "bullish"]

    if len(real_tweets) >= 3 and len(bullish) >= 2:
        score = 80
        result.evidence.append(
            f"{len(real_tweets)} real sources active, {len(bullish)} bullish"
        )
        result.confidence = 60
    elif len(real_tweets) >= 2:
        score = 50
        result.evidence.append(f"{len(real_tweets)} sources discussing this collection")
    elif len(real_tweets) >= 1:
        score = 30
    elif len(news_items) >= 1:
        score = 15

    # Verified collection bonus
    if col_data.get("safelist") == "verified":
        score = min(100, score + 10)

    result.score = max(0, min(100, score))
    return result


# ═══════════════════════════════════════════════════════
#  Layer 8: Risk Adjustment (penalty only)
# ═══════════════════════════════════════════════════════

def score_risk(
    stats: Dict,
    floor_depth: Dict,
    sales: List[Any],
    col_data: Dict,
) -> LayerResult:
    """Pure penalty layer. Can only reduce the total score.

    Returns negative score from -40 to 0.
    Only penalizes for REAL dangers, not noisy metadata gaps.
    """
    result = LayerResult(name="risk", confidence=70)
    penalty = 0

    owners = stats.get("num_owners", 0)
    total_listings = floor_depth.get("total_listings", 0)
    floor = stats.get("floor_price", 0)
    vol_1d = stats.get("volume_1d", 0)
    vol_change = stats.get("volume_change_1d", 0)

    # ── REAL dangers only ──

    # Extreme liquidity trap (very few holders)
    if 0 < owners < 20:
        penalty += 15
        result.evidence.append(
            f"Only {owners} holders — severe liquidity trap"
        )

    # Dead market — zero volume AND zero sales
    if vol_1d == 0 and stats.get("sales_1d", 0) == 0:
        penalty += 15
        result.evidence.append("Zero 24h volume and sales — dead market")

    # Heavy sell pressure (>50% of owners listing)
    if owners > 50 and total_listings > 0:
        listing_ratio = total_listings / owners
        if listing_ratio > 0.5:
            penalty += 10
            result.evidence.append(
                f"{listing_ratio*100:.0f}% of collection listed — extreme sell pressure"
            )

    # Post-pump exhaustion (volume crashed >60%)
    if vol_change < -60:
        penalty += 10
        result.evidence.append(
            f"Volume crashed {vol_change:.0f}% — post-pump exhaustion"
        )

    # Wash detection: same wallets buying and selling
    buyer_set = set()
    seller_set = set()
    for sale in sales:
        buyer = sale.buyer if hasattr(sale, 'buyer') else sale.get("buyer", "")
        seller = sale.seller if hasattr(sale, 'seller') else sale.get("seller", "")
        if buyer:
            buyer_set.add(buyer)
        if seller:
            seller_set.add(seller)

    if buyer_set and seller_set:
        overlap = buyer_set & seller_set
        if len(overlap) > 0:
            overlap_pct = len(overlap) / max(len(buyer_set), 1) * 100
            if overlap_pct > 40:
                penalty += 15
                result.evidence.append(
                    f"⚠️ {len(overlap)} wallets both buying AND selling — "
                    f"possible wash trading ({overlap_pct:.0f}% overlap)"
                )
            elif overlap_pct > 25:
                penalty += 8

    # ── Cap at -40 (not -100) ──
    result.score = -min(40, penalty)
    return result


# ═══════════════════════════════════════════════════════
#  Market Regime Detection
# ═══════════════════════════════════════════════════════

def detect_market_regime(
    collection_stats: List[Dict],
) -> MarketRegime:
    """Determine current NFT market conditions from collection data."""
    regime = MarketRegime()

    if not collection_stats:
        regime.evidence.append("Insufficient data for regime detection")
        return regime

    vol_changes = []
    positive_count = 0
    total_volume = 0

    for s in collection_stats:
        vc = s.get("volume_change_1d", 0)
        vol_changes.append(vc)
        if vc > 0:
            positive_count += 1
        total_volume += s.get("volume_1d", 0)

    breadth = positive_count / max(len(collection_stats), 1)
    avg_vol_change = sum(vol_changes) / max(len(vol_changes), 1) if vol_changes else 0

    regime.market_breadth = round(breadth * 100, 1)
    regime.blue_chip_vol_change = round(avg_vol_change, 1)

    if avg_vol_change > 20 and breadth > 0.60:
        regime.regime = "RISK_ON"
        regime.regime_label = "Risk On — Momentum Favored"
        regime.confidence = 75
        regime.weight_adjustments = {"early_momentum": 0.05, "narrative": 0.03, "risk": -0.03}
        regime.evidence.append(f"Blue-chip volume up {avg_vol_change:.0f}%, {breadth*100:.0f}% breadth")
    elif avg_vol_change < -20 and breadth < 0.40:
        regime.regime = "RISK_OFF"
        regime.regime_label = "Risk Off — Safety First"
        regime.confidence = 75
        regime.weight_adjustments = {"holder_quality": 0.05, "risk": 0.05, "early_momentum": -0.05}
        regime.evidence.append(f"Volume declining {avg_vol_change:.0f}%, only {breadth*100:.0f}% positive")
    elif abs(avg_vol_change) < 10 and breadth > 0.40 and breadth < 0.60:
        regime.regime = "ROTATION"
        regime.regime_label = "Rotation — Sector Picks"
        regime.confidence = 55
        regime.weight_adjustments = {"supply_shock": 0.05, "smart_money": 0.03}
        regime.evidence.append("Mixed signals — sector rotation likely")
    else:
        regime.regime = "CHOP"
        regime.regime_label = "Low Conviction — Selective Only"
        regime.confidence = 45
        regime.evidence.append("No clear market direction — high conviction only")

    return regime


# ═══════════════════════════════════════════════════════
#  Composite Scoring & Decision Engine
# ═══════════════════════════════════════════════════════

def compute_opportunity(
    slug: str,
    col_data: Dict,
    stats: Dict,
    sales: List[Any],
    floor_depth: Dict,
    mint_info: Dict,
    drop_details: Optional[Dict],
    news_items: List[Dict],
    wallet_db: Dict,
    prev_snapshot: Optional[Dict],
    regime: Optional[MarketRegime] = None,
) -> OpportunityCard:
    """Run all 8 layers and produce a complete decision card."""

    # Run all layers
    layers = {
        "smart_money": score_smart_money(sales, wallet_db),
        "early_momentum": score_early_momentum(stats, sales, prev_snapshot),
        "supply_shock": score_supply_shock(floor_depth, prev_snapshot),
        "volume_anomaly": score_volume_anomaly(stats),
        "mint_quality": score_mint_quality(mint_info, stats, drop_details),
        "holder_quality": score_holder_quality(stats, sales, floor_depth),
        "narrative": score_narrative(col_data, news_items),
        "risk": score_risk(stats, floor_depth, sales, col_data),
    }

    # ── Dynamic weight redistribution ──
    # When a layer is N/A (cold start), redistribute its weight
    # to other active layers instead of wasting 30% of the score.
    weights = dict(LAYER_WEIGHTS)

    # Detect N/A layers (confidence <= 10 = no data)
    na_layers = set()
    redistributable = 0
    if layers["smart_money"].confidence <= 10:
        na_layers.add("smart_money")
        redistributable += weights["smart_money"]
        weights["smart_money"] = 0
    if not mint_info.get("is_minting") and layers["mint_quality"].score == 0:
        na_layers.add("mint_quality")
        redistributable += weights["mint_quality"]
        weights["mint_quality"] = 0
    if layers["narrative"].confidence <= 30 and layers["narrative"].score <= 5:
        na_layers.add("narrative")
        redistributable += weights["narrative"]
        weights["narrative"] = 0

    # Redistribute to active layers proportionally
    active_layers = [k for k in weights if k not in na_layers and weights[k] > 0]
    if active_layers and redistributable > 0:
        share = redistributable / len(active_layers)
        for k in active_layers:
            weights[k] += share

    # Apply regime adjustments
    if regime and regime.weight_adjustments:
        for layer_name, adj in regime.weight_adjustments.items():
            if layer_name in weights:
                weights[layer_name] = max(0, weights[layer_name] + adj)

    # Normalize active weights to sum to 1.0
    active_sum = sum(v for v in weights.values())
    if active_sum > 0:
        for k in weights:
            weights[k] /= active_sum

    # Weighted score (only from active layers)
    positive_score = 0
    for layer_name, weight in weights.items():
        layer = layers.get(layer_name)
        if layer and layer_name != "risk":
            positive_score += layer.score * weight

    # Risk penalty (capped, reduced weight)
    risk_layer = layers["risk"]
    risk_penalty = abs(risk_layer.score) * RISK_WEIGHT

    raw_score = positive_score - risk_penalty
    opportunity_score = max(0, min(100, int(raw_score)))

    # Confidence — simpler, don't over-penalize cold start
    active_confidences = [
        l.confidence for name, l in layers.items()
        if name not in na_layers and name != "risk"
    ]
    avg_confidence = (
        sum(active_confidences) / len(active_confidences)
        if active_confidences else 30
    )
    confidence = max(20, min(100, int(avg_confidence)))

    # Timing
    momentum = layers["early_momentum"].score
    volume = layers["volume_anomaly"].score
    supply = layers["supply_shock"].score

    timing = _determine_timing(momentum, volume, supply, risk_layer.score)

    # Signal type
    signal_type = _determine_signal_type(layers, mint_info)

    # Time horizon
    time_horizon = _determine_time_horizon(layers, mint_info)

    # Action — pass stats for evidence gate checks
    action = _determine_action(
        opportunity_score, confidence, timing,
        risk_layer.score, mint_info, regime, stats,
    )

    # Grade
    grade = _determine_grade(opportunity_score, confidence)

    # Build evidence
    all_evidence = []
    for layer in layers.values():
        all_evidence.extend(layer.evidence)
    # Sort by importance (warnings first, then positive signals)
    warnings = [e for e in all_evidence if "⚠️" in e]
    positives = [e for e in all_evidence if "⚠️" not in e and e]
    top_reasons = (positives[:4] + warnings[:2])[:5]
    why_now = positives[:3]

    # Entry / Exit logic
    floor_price = stats.get("floor_price", 0)
    floor_symbol = stats.get("floor_symbol", "ETH")
    entry_zone, invalidation, exit_logic = _compute_entry_exit(
        floor_price, floor_symbol, floor_depth,
        opportunity_score, timing, layers, stats,
    )

    card = OpportunityCard(
        slug=slug,
        name=col_data.get("name", slug),
        chain=col_data.get("chain", ""),
        chain_label=col_data.get("chain_label", ""),
        image=col_data.get("image", ""),
        contract=col_data.get("contract", ""),
        verified=col_data.get("safelist") == "verified",
        owners=stats.get("num_owners", 0),

        opportunity_score=opportunity_score,
        confidence=confidence,
        risk_score=abs(risk_layer.score),
        grade=grade,

        action=action,
        action_color=ACTION_COLORS.get(action, "#6b7280"),
        signal_type=signal_type,
        timing=timing,
        time_horizon=time_horizon,

        layers={name: layer.to_dict() for name, layer in layers.items()},

        top_reasons=top_reasons,
        why_now=why_now,

        entry_zone=entry_zone,
        invalidation=invalidation,
        exit_logic=exit_logic,

        floor_price=floor_price,
        floor_symbol=floor_symbol,
        volume_1d=stats.get("volume_1d", 0),
        volume_7d=stats.get("volume_7d", 0),
        market_cap=stats.get("market_cap", 0),
        total_listings=floor_depth.get("total_listings", 0),
        listings_near_floor=floor_depth.get("listings_within_10pct", 0),

        opensea_url=col_data.get("opensea_url", ""),
        explorer_url=col_data.get("explorer_url", ""),
        twitter=col_data.get("twitter", ""),
        discord=col_data.get("discord", ""),
        website=col_data.get("website", ""),

        data_freshness=time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),

        debug={
            "raw_positive_score": round(positive_score, 1),
            "risk_penalty": round(risk_penalty, 1),
            "effective_weights": {k: round(v, 3) for k, v in weights.items()},
            "na_layers": list(na_layers),
            "layer_scores": {name: l.score for name, l in layers.items()},
            "decision_trace": (
                f"score={opportunity_score}, risk={risk_layer.score}, "
                f"timing={timing}, na={list(na_layers)} → {action}"
            ),
        },
    )

    return card


# ═══════════════════════════════════════════════════════
#  Decision Helpers
# ═══════════════════════════════════════════════════════

def _determine_timing(
    momentum: int, volume: int, supply: int, risk: int,
) -> str:
    if risk < -50:
        return "DANGEROUS"
    if momentum > 60 and volume < 40:
        return "TOO EARLY"
    if momentum > 50 and supply > 40 and volume < 60:
        return "OPTIMAL"
    if momentum > 40 and supply > 30:
        return "OPTIMAL"
    if volume > 70 and momentum < 30:
        return "LATE"
    if volume > 50 and momentum < 20:
        return "LATE"
    return "DEVELOPING"


def _determine_signal_type(layers: Dict, mint_info: Dict) -> str:
    scores = {name: l.score for name, l in layers.items()}

    if mint_info.get("is_minting") and scores.get("mint_quality", 0) > 50:
        return "Post-Mint Runner"
    if scores.get("smart_money", 0) > 60:
        if scores.get("early_momentum", 0) > 50:
            return "Smart Money Rotation"
        return "Early Accumulation"
    if scores.get("supply_shock", 0) > 60 and scores.get("early_momentum", 0) > 40:
        return "Breakout Setup"
    if scores.get("volume_anomaly", 0) > 70 and scores.get("early_momentum", 0) < 30:
        return "Exhausted Pump"
    if scores.get("risk", 0) < -50:
        return "Liquidity Trap"
    if scores.get("early_momentum", 0) > 60:
        return "Early Momentum"
    if scores.get("volume_anomaly", 0) > 50:
        return "Volume Spike"
    if scores.get("holder_quality", 0) > 70:
        return "Strong Holder Base"
    return "Developing Signal"


def _determine_time_horizon(layers: Dict, mint_info: Dict) -> str:
    momentum = layers.get("early_momentum", LayerResult()).score
    volume = layers.get("volume_anomaly", LayerResult()).score

    if mint_info.get("is_minting"):
        return "15m – 2h"
    if momentum > 70 or volume > 70:
        return "1h – 6h"
    if momentum > 40:
        return "6h – 24h"
    return "Multi-day"


def _determine_action(
    score: int, confidence: int, timing: str,
    risk: int, mint_info: Dict, regime: Optional[MarketRegime],
    stats: Optional[Dict] = None,
) -> str:
    """Evidence-gated action classification.

    CRITICAL: Score alone is insufficient. Collections must pass hard
    absolute evidence gates before earning BUY NOW or top Alpha.
    This prevents denominator abuse (e.g., 0.52 ETH volume getting BUY NOW).
    """
    stats = stats or {}
    vol_1d = stats.get("volume_1d", 0)
    sales_1d = stats.get("sales_1d", 0)
    num_owners = stats.get("num_owners", 0)

    # Hard disqualifiers — only for REAL danger
    if risk < -30:
        return "AVOID"
    if timing == "DANGEROUS":
        return "AVOID"

    # ── EVIDENCE GATES ──
    # BUY NOW requires ABSOLUTE proof of real activity, not just ratios
    buy_now_eligible = (
        vol_1d >= 1.0          # Min 1 ETH 24h volume (absolute liquidity)
        and sales_1d >= 3      # Min 3 sales (not single-trade anomaly)
        and confidence >= 45   # Reasonable data confidence
        and num_owners >= 20   # Not a private collection
    )

    # BUY NOW — strong multi-signal convergence + evidence gate
    if buy_now_eligible:
        if score >= 55 and timing in ("OPTIMAL", "DEVELOPING"):
            return "BUY NOW"
        if score >= 50 and timing == "OPTIMAL":
            return "BUY NOW"

    # TOO EARLY — signal exists but not confirmed
    if timing == "TOO EARLY" and score >= 30:
        return "TOO EARLY"

    # TOO LATE — already pumped
    if timing == "LATE" and score >= 25:
        return "TOO LATE"

    # WATCH CLOSELY — developing signal, worth monitoring
    # But also gate: need minimal evidence to even be WATCH
    if score >= 25:
        if vol_1d >= 0.05 or sales_1d >= 1:
            return "WATCH CLOSELY"
        # Extremely thin evidence — downgrade
        return "WATCH CLOSELY" if score >= 40 else "AVOID"

    # Anything below 15 is truly dead/bad
    if score < 15:
        return "AVOID"

    return "WATCH CLOSELY"


def _determine_grade(score: int, confidence: int) -> str:
    for min_score, min_conf, grade in GRADE_THRESHOLDS:
        if score >= min_score:
            return grade
    return "AVOID"


def _compute_entry_exit(
    floor: float, symbol: str, floor_depth: Dict,
    score: int, timing: str, layers: Dict, stats: Dict,
) -> Tuple[str, str, str]:
    """Generate entry zone, invalidation, and exit logic."""
    if floor <= 0:
        return ("Unknown — no floor data", "N/A", "N/A")

    # Entry zone
    if timing in ("OPTIMAL", "DEVELOPING"):
        entry = f"{floor:.6f} {symbol} (current floor)"
    elif timing == "TOO EARLY":
        target = floor * 0.95
        entry = f"Wait for confirmation — target {target:.6f} {symbol}"
    else:
        entry = f"Not recommended at current floor {floor:.6f} {symbol}"

    # Invalidation
    drop_pct = 0.30 if score >= 70 else 0.20
    invalid_price = floor * (1 - drop_pct)
    within_10 = floor_depth.get("listings_within_10pct", 0)
    if within_10 <= 3:
        invalidation = f"Floor drops below {invalid_price:.6f} {symbol} or listings spike above 20"
    else:
        invalidation = f"Floor drops below {invalid_price:.6f} {symbol} ({drop_pct*100:.0f}% drawdown)"

    # Exit logic
    supply_score = layers.get("supply_shock", LayerResult()).score
    if supply_score > 60:
        profit_target = floor * 1.5
        exit_logic = (
            f"Take profit at {profit_target:.6f} {symbol} (+50%) "
            f"or when listings increase >40%"
        )
    else:
        profit_target = floor * 1.3
        exit_logic = (
            f"Take profit at {profit_target:.6f} {symbol} (+30%) "
            f"or on volume deceleration"
        )

    return (entry, invalidation, exit_logic)


def _seconds_to_label(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    return f"{seconds // 3600}h ago"


# ═══════════════════════════════════════════════════════
#  Dashboard Header Builder
# ═══════════════════════════════════════════════════════

def build_dashboard_header(
    cards: List[OpportunityCard],
    regime: MarketRegime,
) -> DashboardHeader:
    """Build the global dashboard summary from scored cards."""
    header = DashboardHeader(
        regime=regime.to_dict(),
        total_analyzed=len(cards),
        updated_at=time.strftime("%H:%M:%S"),
    )

    if not cards:
        return header

    # Sort by opportunity score
    sorted_cards = sorted(cards, key=lambda c: c.opportunity_score, reverse=True)

    # Best opportunity
    if sorted_cards:
        best = sorted_cards[0]
        header.best_opportunity = {
            "name": best.name, "slug": best.slug,
            "score": best.opportunity_score, "grade": best.grade,
            "action": best.action, "chain_label": best.chain_label,
        }

    # Highest conviction (highest confidence among top scores)
    conviction = sorted(
        [c for c in cards if c.opportunity_score >= 50],
        key=lambda c: c.confidence, reverse=True,
    )
    if conviction:
        hc = conviction[0]
        header.highest_conviction = {
            "name": hc.name, "slug": hc.slug,
            "score": hc.opportunity_score, "confidence": hc.confidence,
            "grade": hc.grade, "chain_label": hc.chain_label,
        }

    # Best early entry
    early = [c for c in cards if c.action == "TOO EARLY" and c.opportunity_score >= 45]
    early.sort(key=lambda c: c.opportunity_score, reverse=True)
    if early:
        e = early[0]
        header.best_early_entry = {
            "name": e.name, "slug": e.slug,
            "score": e.opportunity_score, "grade": e.grade,
            "chain_label": e.chain_label,
        }

    # Most dangerous
    dangerous = [c for c in cards if c.action == "AVOID"]
    dangerous.sort(key=lambda c: c.risk_score, reverse=True)
    if dangerous:
        d = dangerous[0]
        header.most_dangerous = {
            "name": d.name, "slug": d.slug,
            "risk_score": d.risk_score, "signal_type": d.signal_type,
            "chain_label": d.chain_label,
        }

    # Best post-mint flip
    mints = [c for c in cards if "Mint" in c.signal_type and c.opportunity_score >= 40]
    mints.sort(key=lambda c: c.opportunity_score, reverse=True)
    if mints:
        m = mints[0]
        header.best_post_mint = {
            "name": m.name, "slug": m.slug,
            "score": m.opportunity_score, "grade": m.grade,
            "chain_label": m.chain_label,
        }

    # Alpha count
    header.alpha_count = sum(
        1 for c in cards if c.action in ("BUY NOW", "WATCH CLOSELY")
    )

    return header
