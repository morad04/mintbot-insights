"""
MintBot — AI Insights Engine v7 (Alpha)
═══════════════════════════════════════
NFT alpha engine with predictive scoring.

Pipeline orchestrator that coordinates:
  - opensea_stats         : production-grade OpenSea API client (rate-limited, validated)
  - alpha_engine          : 8-layer intelligence scoring + decision engine
  - collection_registry   : identity, classification, blue-chip management
  - scoring_engine        : legacy discovery / upcoming / blue-chip impact scoring
  - news_pipeline         : Jina Reader Twitter scraping + normalization

Output Lanes:
  Alpha      — Top opportunities with BUY/WATCH/AVOID/EXIT decisions
  Minting    — Collections with live mints
  Watch      — Collections to monitor closely
  Avoid      — Flagged traps and exhausted pumps
  Blue Chips — Blue chips with fresh catalysts
  Upcoming   — Future mints

Data Sources:
  - OpenSea API v2  : collections, stats, sales, listings, events
  - OpenSea MCP     : mint stage details, pricing, eligibility
  - Jina Reader     : real Twitter/X scraping (zero API keys)
  - NFT Calendars   : upcoming mints from RSS feeds
"""

import asyncio
import aiohttp
import time
import re
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from .collection_registry import (
    is_blue_chip, classify_collection, get_all_blue_chip_slugs,
    detect_originality, detect_risk_flags, match_text_to_collection,
    BLUE_CHIP_REGISTRY, CLASS_BLUE_CHIP, CLASS_MINTING_NOW,
)
from .scoring_engine import (
    score_discovery, score_upcoming, score_bluechip_impact,
    should_alert_bluechip,
)
from .news_pipeline import (
    scrape_twitter_via_jina, match_news_to_collections,
    filter_noise, sentiment_emoji, catalyst_emoji,
)
from .tempo_scanner import scan_tempo_mints
from .opensea_stats import (
    OpenSeaClient, CollectionStats, MintActivity,
    compute_floor_depth, FloorDepth,
)
from .alpha_engine import (
    compute_opportunity, detect_market_regime, build_dashboard_header,
    OpportunityCard, MarketRegime,
)

logger = logging.getLogger("mintbot.insights")

# ═══════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════

CACHE_TTL = 300          # 5 min
BG_INTERVAL = 300        # 5 min background refresh
REQUEST_TIMEOUT = 15     # seconds per HTTP request
MAX_PER_LANE = 20        # max results per lane
ENRICH_CONCURRENCY = 3   # parallel OpenSea API requests (reduced to avoid 429 storms)
ALPHA_DEEP_LIMIT = 40    # collections to fetch sales+listings for (reduced from 80)

OPENSEA_API_BASE = "https://api.opensea.io/api/v2"
NULL_ADDR = "0x0000000000000000000000000000000000000000"

CHAINS = ["ethereum", "base", "apechain"]
CHAIN_LABELS = {"ethereum": "ETH", "base": "BASE", "apechain": "APE", "tempo": "TEMPO"}
CHAIN_EXPLORERS = {
    "ethereum": "https://etherscan.io/address/",
    "base": "https://basescan.org/address/",
    "apechain": "https://apescan.io/address/",
    "tempo": "https://explore.mainnet.tempo.xyz/address/",
}

# ── Upcoming Mint Sources ──
# PRIMARY: OpenSea Drops API + MCP drop details
# DISCOVERY: Top collections by volume + new collections (created_date)

# ═══════════════════════════════════════════════════════
#  Cache
# ═══════════════════════════════════════════════════════

class _InsightsCache:
    def __init__(self):
        self.data: Optional[Dict] = None
        self.ts: float = 0
        self.refreshing: bool = False

    @property
    def valid(self) -> bool:
        return self.data is not None and (time.time() - self.ts) < CACHE_TTL

    def set(self, payload: Dict):
        self.data = payload
        self.ts = time.time()
        self.refreshing = False

_cache = _InsightsCache()

# ── Progress tracking for dashboard loading bar ──
_progress: Dict = {
    "running": False,
    "step": "",
    "pct": 0,
    "detail": "",
    "started_at": 0,
    "elapsed": 0,
}

def _set_progress(step: str, pct: int, detail: str = ""):
    """Update pipeline progress for the frontend loading bar."""
    _progress["running"] = True
    _progress["step"] = step
    _progress["pct"] = min(pct, 100)
    _progress["detail"] = detail
    if _progress["started_at"] == 0:
        _progress["started_at"] = time.time()
    _progress["elapsed"] = round(time.time() - _progress["started_at"], 1)

def _finish_progress():
    _progress["running"] = False
    _progress["pct"] = 100
    _progress["step"] = "Done"
    _progress["detail"] = ""
    _progress["started_at"] = 0

def get_progress() -> Dict:
    """Get current pipeline progress (called by server API)."""
    return dict(_progress)

# Shared OpenSeaClient instance (reused across pipeline runs)
_os_client: Optional[OpenSeaClient] = None

def _get_os_client() -> OpenSeaClient:
    """Get or create the shared OpenSeaClient instance."""
    global _os_client
    if _os_client is None:
        _os_client = OpenSeaClient()
        logger.info("[INSIGHTS] v6 OpenSeaClient initialized")
    return _os_client


# ═══════════════════════════════════════════════════════
#  OpenSea Data Fetchers (via OpenSeaClient)
# ═══════════════════════════════════════════════════════

def _info_to_dict(info, source: str = "volume") -> Dict:
    """Convert CollectionInfo dataclass to the legacy dict format."""
    return {
        "slug": info.slug,
        "name": info.name,
        "image": info.image,
        "banner": info.banner,
        "description": info.description,
        "contract": info.contract,
        "chain": info.chain,
        "chain_label": info.chain_label,
        "twitter": info.twitter,
        "discord": info.discord,
        "website": info.website,
        "opensea_url": info.opensea_url,
        "explorer_url": info.explorer_url,
        "safelist": info.safelist,
        "category_os": info.category,
        "_source": source,
    }


async def _fetch_top_collections_v6(client: OpenSeaClient, chain: str, limit: int = 100) -> List[Dict]:
    """Fetch top collections by volume using OpenSeaClient."""
    infos = await client.fetch_top_collections(chain, "one_day_volume", limit)
    return [_info_to_dict(info, "volume") for info in infos]


async def _fetch_new_collections_v6(client: OpenSeaClient, chain: str, limit: int = 50) -> List[Dict]:
    """Fetch newest collections using OpenSeaClient."""
    infos = await client.fetch_top_collections(chain, "created_date", limit)
    return [_info_to_dict(info, "new") for info in infos]


async def _fetch_blue_chips_v6(client: OpenSeaClient) -> List[Dict]:
    """Fetch blue-chip collection details using OpenSeaClient.
    
    Validation strategy: Registry BCs are TRUSTED by default.
    We only reject a BC if the stats API *successfully* returns
    a provably low owner count (< 100). If the API fails (429/timeout),
    we keep the BC — better to show a real BC than to incorrectly filter it.
    """
    # Batch fetch all BC info in parallel (semaphore-bounded)
    sem = asyncio.Semaphore(3)
    async def fetch_one(bc):
        slug = bc["slug"]
        async with sem:
            info = await client.fetch_collection(slug)
            await asyncio.sleep(0.3)
            if info:
                d = _info_to_dict(info, "blue_chip")
                d["_is_blue_chip"] = True
                if not d["opensea_url"]:
                    d["opensea_url"] = f"https://opensea.io/collection/{slug}"
                return d
            return None
    
    fetch_tasks = [fetch_one(bc) for bc in BLUE_CHIP_REGISTRY]
    raw_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    results = [r for r in raw_results if isinstance(r, dict)]
    
    # Enrich with stats — parallel, semaphore-bounded
    # NOTE: We do NOT filter blue chips by owner count here.
    # Registry BCs are trusted. The stats API often returns incorrect
    # low counts under rate limits (DeGods "85 owners" etc.)
    async def enrich_one(d):
        async with sem:
            try:
                stats = await client.fetch_stats(d["slug"])
                await asyncio.sleep(0.2)
                if stats:
                    d["_stats"] = stats.to_dict()
                return d
            except Exception:
                return d
    
    enrich_tasks = [enrich_one(d) for d in results]
    enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)
    validated = [r for r in enriched if isinstance(r, dict)]
    
    logger.info(f"[INSIGHTS] Fetched {len(validated)} blue-chip collections "
                f"(rejected {len(results) - len(validated)} junk slugs)")
    return validated


async def _batch_enrich_collections(
    client: OpenSeaClient,
    collections: List[Dict],
    max_items: int = 40,
    fetch_mcp: bool = True,
) -> List[Dict]:
    """Batch-enrich collections with stats + mint activity using concurrency.

    Uses OpenSeaClient.batch_enrich() with semaphore-bounded parallelism
    instead of sequential O(N×0.4s) requests.
    """
    slugs = [c["slug"] for c in collections[:max_items] if c.get("slug")]
    slug_to_col = {c["slug"]: c for c in collections[:max_items] if c.get("slug")}

    # Batch fetch stats + mint activity for all slugs in parallel
    enrichments = await client.batch_enrich(
        slugs,
        concurrency=ENRICH_CONCURRENCY,
        fetch_mcp_for_minting=fetch_mcp,
    )

    enriched = []
    for slug in slugs:
        col = slug_to_col.get(slug)
        if not col:
            continue
        data = enrichments.get(slug, {})
        col["_stats"] = data.get("_stats", {})
        col["_mint_info"] = data.get("_mint_info", {"is_minting": False})
        col["_drop_details"] = data.get("_drop_details", None)
        col["_originality"] = detect_originality(col)
        col["_risk_flags"] = detect_risk_flags(col)
        col["_matched_news"] = []
        enriched.append(col)

    return enriched


# ═══════════════════════════════════════════════════════
#  Upcoming Mints — Multi-Source Pipeline
# ═══════════════════════════════════════════════════════

ME_CHAIN_MAP = {
    "solana": "SOL", "ethereum": "ETH", "1": "ETH",
    "8453": "BASE", "137": "MATIC", "56": "BNB",
    "43114": "AVAX", "80094": "BERACHAIN",
}




# ═══════════════════════════════════════════════════════
#  OpenSea Minting Discovery (API-only, no scraping)
# ═══════════════════════════════════════════════════════

async def _discover_opensea_minting(client) -> Dict[str, List[Dict]]:
    """Discover actively minting + upcoming collections via OpenSea API + MCP.
    
    Strategy:
      1. MCP get_upcoming_drops → SeaDrop mints with full pricing/stages
      2. REST API: fetch new + trending collections across chains
      3. MCP get_drop_details on top candidates → verify mintStatus
      4. Combine all sources, deduplicate
    
    Returns dict with 'minting' and 'upcoming' lists for lane routing.
    """
    minting_items = []
    upcoming_items = []
    seen_slugs = set()
    
    # ── Source 1: MCP get_upcoming_drops (SeaDrop collections) ──
    try:
        mcp_drops = await client.fetch_upcoming_drops_mcp(limit=50)
        for drop in mcp_drops:
            slug = drop.get("collectionSlug", "")
            if not slug or slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            
            name = drop.get("collectionName", slug)
            chain = drop.get("chain", "ethereum")
            image = drop.get("collectionImageUrl", "")
            mint_status = drop.get("mintStatus", "")
            active_stage = drop.get("activeStage", {})
            next_stage = drop.get("nextStage", {})
            
            # Price from active or next stage
            price_info = (active_stage or next_stage or {}).get("price", {})
            price_val = price_info.get("token", {}).get("unit", 0)
            price_sym = price_info.get("token", {}).get("symbol", "ETH")
            price_usd = price_info.get("usd", 0)
            stage_label = (active_stage or next_stage or {}).get("label", "")
            
            price_str = f"{price_val} {price_sym}" if price_val > 0 else "Free"
            if price_usd > 0:
                price_str += f" (${price_usd:.2f})"
            
            os_url = f"https://opensea.io/collection/{slug}"
            
            # Build reasons
            reasons = []
            if stage_label:
                reasons.append(f"🎯 Stage: {stage_label}")
            if price_val > 0:
                reasons.append(f"💎 Price: {price_str}")
            reasons.append(f"🔗 Chain: {CHAIN_LABELS.get(chain, chain.upper())}")
            reasons.append(f"Drop type: {drop.get('dropType', 'SeaDrop')}")
            reasons.append("Verified via OpenSea MCP")
            
            item = {
                "slug": slug,
                "name": name,
                "contract": drop.get("contractAddress", ""),
                "chain": chain,
                "chain_label": CHAIN_LABELS.get(chain, chain.upper()),
                "image": image,
                "description": "",
                "source_name": "OpenSea MCP",
                "catalyst": "Live Mint" if mint_status == "MINTING" else "Upcoming Drop",
                "confidence": "high",
                "category": "minting" if mint_status == "MINTING" else "upcoming",
                "date": "Minting Now" if mint_status == "MINTING" else (
                    "Minting Soon" if mint_status == "MINTING_SOON" else "Upcoming"
                ),
                "price": price_str,
                "link": os_url,
                "opensea_url": os_url,
                "primary_link": os_url,
                "primary_link_label": "View on OpenSea",
                "reasons": reasons,
                "is_opensea_drop": True,
                "_category": "minting" if mint_status in ("MINTING", "MINTING_SOON") else "upcoming",
                "_mint_status": mint_status,
            }
            
            if mint_status in ("MINTING", "MINTING_SOON"):
                minting_items.append(item)
            else:
                upcoming_items.append(item)
    except Exception as e:
        logger.warning(f"[DISCOVER] MCP upcoming drops failed: {e}")
    
    logger.info(f"[DISCOVER] MCP drops: {len(minting_items)} minting, {len(upcoming_items)} upcoming")
    
    # ── Source 2: Collect ALL candidates from multiple sources ──
    # Gather slugs from SSR, REST API (paginated), watchlist
    candidate_slugs = []  # ordered list — preserves priority
    candidate_map = {}    # slug -> collection info for building items
    
    # 2a. Watchlist — guaranteed MCP check for user-curated slugs
    _set_progress("Loading watchlist", 10, "Reading watchlist.json")
    try:
        import pathlib
        watchlist_path = pathlib.Path(__file__).resolve().parents[2] / "watchlist.json"
        if watchlist_path.exists():
            with open(watchlist_path) as f:
                wl = json.load(f)
            for s in wl.get("slugs", []):
                if s and s not in seen_slugs and s not in candidate_map:
                    candidate_slugs.append(s)
                    candidate_map[s] = {"source": "WATCHLIST"}
            logger.info(f"[DISCOVER] Watchlist: {len(wl.get('slugs', []))} slugs loaded")
    except Exception as e:
        logger.debug(f"[DISCOVER] Watchlist load failed: {e}")
    
    # 2b. SSR scrape opensea.io/drops for featured drops slugs
    _set_progress("Scraping drops page", 15, "Fetching opensea.io/drops")
    try:
        session = await client._ensure_session()
        ssr_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "text/html",
        }
        async with session.get(
            "https://opensea.io/drops", headers=ssr_headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 200:
                html = await resp.text()
                import re
                ssr_slugs = re.findall(r'collection/([a-z0-9][a-z0-9-]+)', html)
                ssr_unique = list(dict.fromkeys(ssr_slugs))
                SKIP_PATTERNS = ("shell", "wrapper", "token-", "usdt-")
                for s in ssr_unique:
                    if s not in seen_slugs and not any(p in s for p in SKIP_PATTERNS):
                        if s not in candidate_map:
                            candidate_slugs.append(s)
                            candidate_map[s] = {"source": "SSR"}
                logger.info(f"[DISCOVER] SSR: {len([s for s in ssr_unique if s in candidate_map])} new slugs from drops page")
    except Exception as e:
        logger.debug(f"[DISCOVER] SSR scrape failed: {e}")
    
    # 2c. REST API: deep paginated scan across chains + orderings
    _set_progress("REST API scan", 20, "Fetching collections from 4 chains")
    PAGINATED_QUERIES = [
        # (chain, ordering, max_pages) — 50 per page
        ("ethereum", "created_date", 4),    # 200 newest ETH collections
        ("ethereum", "one_day_volume", 4),   # 200 by daily volume
        ("ethereum", "seven_day_volume", 2), # 100 by weekly volume
        ("base", "created_date", 4),         # 200 newest BASE collections 
        ("base", "one_day_volume", 4),       # 200 by daily volume
        ("matic", "created_date", 2),        # 100 newest POLY collections
        ("matic", "one_day_volume", 2),      # 100 by daily volume
        ("arbitrum", "created_date", 2),     # 100 newest ARB collections
    ]
    
    rest_tasks = [
        client.fetch_collections_paginated(chain, order, pages)
        for chain, order, pages in PAGINATED_QUERIES
    ]
    rest_results = await asyncio.gather(*rest_tasks, return_exceptions=True)
    for res in rest_results:
        if isinstance(res, list):
            for col in res:
                if col.slug not in seen_slugs and col.slug not in candidate_map:
                    candidate_slugs.append(col.slug)
                    candidate_map[col.slug] = {"source": "REST", "col": col}
    
    logger.info(f"[DISCOVER] Total candidates for MCP check: {len(candidate_slugs)} "
                f"(SSR: {sum(1 for v in candidate_map.values() if v.get('source')=='SSR')}, "
                f"REST: {sum(1 for v in candidate_map.values() if v.get('source')=='REST')})")
    
    # ── Source 3: Batch MCP verification (single session, fast) ──
    _set_progress("MCP verification", 30, f"Checking {len(candidate_slugs)} candidates via MCP")
    
    def _mcp_progress(checked, total, found):
        pct = 30 + int(50 * checked / max(total, 1))  # 30-80% range
        _set_progress("MCP verification", pct, f"Checked {checked}/{total} — found {found} minting")
    
    mcp_results = await client.batch_check_mint_status_mcp(
        candidate_slugs, concurrency=8, on_progress=_mcp_progress
    )
    
    # Build items from confirmed minting collections
    for slug, status in mcp_results.items():
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        
        mint_status = status.get("mintStatus", "")
        price_val = status.get("active_price", 0)
        price_sym = status.get("active_symbol", "ETH")
        price_str = f"{price_val} {price_sym}" if price_val > 0 else "Free" if price_val == 0 else "TBA"
        
        # Get collection info from REST cache or fetch it
        info = candidate_map.get(slug, {})
        col = info.get("col")
        source = info.get("source", "MCP")
        
        if col:
            name = col.name
            chain = col.chain
            chain_label = col.chain_label or col.chain.upper()
            image = col.image
            description = (col.description or "")[:300]
            contract = col.contract
            os_url = col.opensea_url or f"https://opensea.io/collection/{slug}"
        else:
            # SSR-sourced slug — need to fetch collection info
            try:
                col_info = await client.fetch_collection(slug)
                if col_info:
                    name = col_info.name
                    chain = col_info.chain
                    chain_label = col_info.chain_label or col_info.chain.upper()
                    image = col_info.image
                    description = (col_info.description or "")[:300]
                    contract = col_info.contract
                else:
                    name = slug.replace("-", " ").title()
                    chain = "ethereum"
                    chain_label = "ETH"
                    image = ""
                    description = ""
                    contract = ""
            except Exception:
                name = slug.replace("-", " ").title()
                chain = "ethereum"
                chain_label = "ETH"
                image = ""
                description = ""
                contract = ""
            os_url = f"https://opensea.io/collection/{slug}"
        
        reasons = []
        if status.get("active_stage"):
            reasons.append(f"🎯 Stage: {status['active_stage']}")
        if price_val > 0:
            reasons.append(f"💎 Price: {price_str}")
        reasons.append(f"🔗 Chain: {chain_label}")
        reasons.append(f"📊 {status.get('stages', 0)} mint stages")
        reasons.append("Verified via OpenSea MCP")
        
        item = {
            "slug": slug,
            "name": name,
            "contract": contract,
            "chain": chain,
            "chain_label": chain_label,
            "image": image,
            "description": description,
            "source_name": f"OpenSea {'Drops' if source == 'SSR' else 'Discovery'}",
            "catalyst": "Live Mint" if mint_status == "MINTING" else "Minting Soon",
            "confidence": "high",
            "category": "minting",
            "date": "Minting Now" if mint_status == "MINTING" else "Minting Soon",
            "price": price_str,
            "link": os_url,
            "opensea_url": os_url,
            "primary_link": os_url,
            "primary_link_label": "View on OpenSea",
            "reasons": reasons,
            "is_opensea_drop": True,
            "_category": "minting" if mint_status == "MINTING" else "upcoming",
            "_mint_status": mint_status,
        }
        
        if mint_status == "MINTING":
            minting_items.append(item)
        else:
            upcoming_items.append(item)
        
        logger.info(f"[DISCOVER] MCP confirmed: {name} ({slug}) = {mint_status}")
    
    logger.info(f"[DISCOVER] Total: {len(minting_items)} minting, {len(upcoming_items)} upcoming"
                f" (from {len(candidate_slugs)} candidates)")
    
    return {"minting": minting_items, "upcoming": upcoming_items}


# ═══════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════

def _time_ago(ts: int) -> str:
    if not ts:
        return ""
    diff = int(time.time()) - ts
    if diff < 60:
        return f"{diff}s ago"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h ago"
    return f"{diff // 86400}d ago"


# ═══════════════════════════════════════════════════════
#  Output Builder
# ═══════════════════════════════════════════════════════

def _build_card(col: Dict, scoring_result: Dict, lane: str) -> Dict:
    """Build a clean card object for API output.

    Every card includes: score, label, trend, risk, reasons[], links, metrics.
    """
    stats = col.get("_stats", {})
    mint_info = col.get("_mint_info", {})
    originality = col.get("_originality", {})
    risk_flags = col.get("_risk_flags", [])

    floor = stats.get("floor_price", 0)
    floor_sym = stats.get("floor_symbol", "ETH")

    # Build links
    links = {}
    if col.get("opensea_url"):
        links["marketplace"] = col["opensea_url"]
    if col.get("explorer_url"):
        links["explorer"] = col["explorer_url"]
    if col.get("twitter"):
        links["twitter"] = f"https://x.com/{col['twitter']}"
    if col.get("discord"):
        links["discord"] = col["discord"]
    if col.get("website"):
        links["website"] = col["website"]

    # Build source items from matched news
    source_items = []
    for news in col.get("_matched_news", []):
        source_items.append({
            "title": news.get("title", "")[:200],
            "source_type": news.get("source_type", "unknown"),
            "sentiment": news.get("sentiment", "neutral"),
            "catalyst": news.get("catalyst", ""),
            "confidence": news.get("confidence", 0),
            "direct_url": news.get("direct_url", ""),
            "fallback_url": news.get("fallback_url", ""),
            "published_at": news.get("published", ""),
            "score_effect": news.get("score_effect", 0),
        })

    # ── State-based primary link routing ──
    opensea_url = col.get("opensea_url", "") or f"https://opensea.io/collection/{col.get('slug','')}"
    website = col.get("website", "")
    
    if lane == "minting_now":
        # Minting: prefer official website/mint page, then OpenSea
        primary_link = website or opensea_url
        primary_link_label = "Mint Page" if website else "View on OpenSea"
    elif lane == "upcoming":
        # Upcoming: link to source
        primary_link = col.get("link", "") or website or opensea_url
        primary_link_label = "View Source"
    elif lane == "bluechip_alert":
        # Blue chip: OpenSea
        primary_link = opensea_url
        primary_link_label = "View on OpenSea"
    else:
        # Discovery / Good Calls: OpenSea collection page
        primary_link = opensea_url
        primary_link_label = "View on OpenSea"

    card = {
        "slug": col.get("slug", ""),
        "name": col.get("name", "Unknown"),
        "image": col.get("image", ""),
        "lane": lane,
        "chain": col.get("chain", "ethereum"),
        "chain_label": col.get("chain_label", "ETH"),
        "contract": col.get("contract", ""),
        "is_blue_chip": col.get("_is_blue_chip", False),
        "is_minting": mint_info.get("is_minting", False),
        "last_mint_ago": mint_info.get("last_mint_ago", ""),
        "mint_count_24h": mint_info.get("mint_count_24h", 0),
        # State-based link
        "primary_link": primary_link,
        "primary_link_label": primary_link_label,
        # Scoring
        "score": scoring_result.get("score", scoring_result.get("impact", 0)),
        "score_label": scoring_result.get("label", ""),
        "score_label_color": scoring_result.get("label_color", "#6b7280"),
        "trend": scoring_result.get("trend", scoring_result.get("direction", "stable")),
        "risk": scoring_result.get("risk", "Medium"),
        "reasons": scoring_result.get("reasons", []),
        "breakdown": scoring_result.get("breakdown", {}),
        # Metrics — Primary
        "floor_price": floor,
        "floor_symbol": floor_sym,
        "volume_1d": stats.get("volume_1d", 0),
        "volume_change_1d": stats.get("volume_change_1d", 0),
        "sales_1d": stats.get("sales_1d", 0),
        "num_owners": stats.get("num_owners", 0),
        "market_cap": stats.get("market_cap", 0),
        "total_volume": stats.get("total_volume", 0),
        # Metrics — Extended (7d/30d)
        "volume_7d": stats.get("volume_7d", 0),
        "volume_change_7d": stats.get("volume_change_7d", 0),
        "volume_30d": stats.get("volume_30d", 0),
        "sales_7d": stats.get("sales_7d", 0),
        "sales_30d": stats.get("sales_30d", 0),
        "avg_price": stats.get("avg_price", 0),
        "avg_price_1d": stats.get("avg_price_1d", 0),
        "total_sales": stats.get("total_sales", 0),
        # Identity
        "safelist": col.get("safelist", ""),
        "category_os": col.get("category_os", ""),
        "twitter": col.get("twitter", ""),
        "discord": col.get("discord", ""),
        "website": col.get("website", ""),
        "opensea_url": opensea_url,
        "explorer_url": col.get("explorer_url", ""),
        # Originality
        "originality": originality.get("label", ""),
        "originality_reasons": originality.get("reasons", []),
        # Risk
        "risk_flags": risk_flags[:3],
        # Links & sources
        "links": links,
        "source_items": source_items,
        # Freshness
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }

    # ── MCP Drop Details (mint stages, supply progress) ──
    drop_details = col.get("_drop_details")
    if drop_details:
        card["mint_stages"] = drop_details.get("stages", [])
        card["active_stage"] = drop_details.get("active_stage")
        card["active_price"] = drop_details.get("active_price", 0)
        card["active_symbol"] = drop_details.get("active_symbol", "ETH")
        card["supply_minted"] = drop_details.get("total_supply", 0)
        card["supply_max"] = drop_details.get("max_supply", 0)
        card["supply_pct"] = drop_details.get("supply_pct", 0)
        card["drop_type"] = drop_details.get("drop_type", "")
    else:
        card["mint_stages"] = []
        card["active_stage"] = None
        card["active_price"] = 0
        card["active_symbol"] = "ETH"
        card["supply_minted"] = 0
        card["supply_max"] = 0
        card["supply_pct"] = 0
        card["drop_type"] = ""

    # Backward compat: old-style scores/verdict for frontend
    bd = scoring_result.get("breakdown", {})
    card["scores"] = {
        "total": card["score"],
        "volume": bd.get("market_momentum", 0),
        "momentum": bd.get("social_momentum", bd.get("social_acceleration", 0)),
        "mint_activity": bd.get("catalyst_strength", bd.get("catalyst_significance", 0)),
        "community": bd.get("community_growth", bd.get("source_credibility", 0)),
    }
    card["verdict"] = card["score_label"]
    card["verdict_color"] = card["score_label_color"]
    card["analysis"] = " • ".join(card["reasons"]) if card["reasons"] else ""

    return card


def _build_bluechip_alert_card(col: Dict, impact_result: Dict) -> Dict:
    """Build a blue-chip alert card (different from discovery cards)."""
    card = _build_card(col, impact_result, "bluechip_alert")
    card["impact"] = impact_result.get("impact", 0)
    card["impact_label"] = impact_result.get("label", "")
    card["impact_direction"] = impact_result.get("direction", "neutral")
    return card


# ═══════════════════════════════════════════════════════
#  MAIN PIPELINE
# ═══════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════
#  Alpha Intelligence Layer
# ═══════════════════════════════════════════════════════

async def _run_alpha_scoring(
    client: OpenSeaClient,
    enriched_collections: List[Dict],
    news_items: List[Dict],
    enriched_bc: List[Dict],
) -> Dict:
    """Run the 8-layer alpha engine on enriched collections.

    Steps:
    1. Fetch sales + listings for top candidates (by volume)
    2. Load previous snapshots from DB
    3. Load wallet intel from DB
    4. Run alpha scoring on each collection
    5. Store new snapshots + sales_log
    6. Detect market regime
    7. Build dashboard header
    8. Return alpha cards + header
    """
    t_start = time.time()

    # Sort by volume to prioritize deep-dive on most active
    candidates = sorted(
        enriched_collections,
        key=lambda c: c.get("_stats", {}).get("volume_1d", 0),
        reverse=True,
    )[:ALPHA_DEEP_LIMIT]

    logger.info(f"[ALPHA] Deep-analyzing {len(candidates)} collections...")

    # Load wallet DB and snapshots from SQLite
    wallet_db = {}
    snapshots_db = {}
    try:
        from src.database import db
        if db._db:
            async with db.db.execute(
                "SELECT * FROM wallet_intel WHERE tier IN ('smart', 'active') LIMIT 500"
            ) as cursor:
                rows = await cursor.fetchall()
                wallet_db = {dict(r)["address"]: dict(r) for r in rows}

            # Get latest snapshot per slug
            slugs_str = ",".join(f"'{c['slug']}'" for c in candidates)
            if slugs_str:
                async with db.db.execute(
                    f"SELECT * FROM collection_snapshots "
                    f"WHERE slug IN ({slugs_str}) "
                    f"ORDER BY captured_at DESC"
                ) as cursor:
                    rows = await cursor.fetchall()
                    for r in rows:
                        d = dict(r)
                        slug = d["slug"]
                        if slug not in snapshots_db:
                            snapshots_db[slug] = d
    except Exception as e:
        logger.debug(f"[ALPHA] DB load failed (non-fatal): {e}")

    logger.info(
        f"[ALPHA] Loaded {len(wallet_db)} smart wallets, "
        f"{len(snapshots_db)} previous snapshots"
    )

    # Fetch sales + listings for candidates (semaphore-bounded)
    sem = asyncio.Semaphore(3)  # Lighter concurrency for deeper calls
    sales_map: Dict[str, list] = {}
    listings_map: Dict[str, list] = {}

    async def _fetch_deep(slug: str):
        async with sem:
            try:
                sales = await client.fetch_sales(slug, limit=50)
                sales_map[slug] = sales
            except Exception as e:
                logger.debug(f"[ALPHA] Sales fetch failed for {slug}: {e}")
                sales_map[slug] = []

            try:
                listings = await client.fetch_listings(slug, limit=100)
                listings_map[slug] = listings
            except Exception as e:
                logger.debug(f"[ALPHA] Listings fetch failed for {slug}: {e}")
                listings_map[slug] = []

    await asyncio.gather(
        *[_fetch_deep(c["slug"]) for c in candidates],
        return_exceptions=True,
    )

    logger.info(
        f"[ALPHA] Deep data fetched: {sum(len(v) for v in sales_map.values())} sales, "
        f"{sum(len(v) for v in listings_map.values())} listings"
    )

    # Run alpha scoring on each candidate
    alpha_cards: List[OpportunityCard] = []
    new_snapshots = []
    new_sales = []

    for col in candidates:
        slug = col["slug"]
        stats = col.get("_stats", {})
        mint_info = col.get("_mint_info", {})
        drop_details = col.get("_drop_details")
        matched_news = col.get("_matched_news", [])
        sales = sales_map.get(slug, [])
        listings = listings_map.get(slug, [])

        # Compute floor depth from listings
        depth = compute_floor_depth(listings)

        # Get previous snapshot
        prev = snapshots_db.get(slug)

        # Run the 8-layer alpha engine
        card = compute_opportunity(
            slug=slug,
            col_data=col,
            stats=stats,
            sales=sales,
            floor_depth=depth.to_dict(),
            mint_info=mint_info,
            drop_details=drop_details,
            news_items=matched_news,
            wallet_db=wallet_db,
            prev_snapshot=prev,
        )
        alpha_cards.append(card)

        # Prepare snapshot for storage
        now_ts = int(time.time())
        buyers_1h = set()
        buyers_6h = set()
        for s in sales:
            ts = s.timestamp if hasattr(s, 'timestamp') else s.get('timestamp', 0)
            buyer = s.buyer if hasattr(s, 'buyer') else s.get('buyer', '')
            if buyer:
                if now_ts - ts < 3600:
                    buyers_1h.add(buyer)
                if now_ts - ts < 21600:
                    buyers_6h.add(buyer)

        new_snapshots.append({
            "slug": slug,
            "floor_price": stats.get("floor_price", 0),
            "volume_1d": stats.get("volume_1d", 0),
            "sales_1d": stats.get("sales_1d", 0),
            "listings_count": depth.total_listings,
            "listings_near_floor": depth.listings_within_10pct,
            "unique_buyers_1h": len(buyers_1h),
            "unique_buyers_6h": len(buyers_6h),
            "num_owners": stats.get("num_owners", 0),
            "captured_at": now_ts,
        })

        # Prepare sales for wallet tracking
        for s in sales:
            ts = s.timestamp if hasattr(s, 'timestamp') else s.get('timestamp', 0)
            buyer = s.buyer if hasattr(s, 'buyer') else s.get('buyer', '')
            seller = s.seller if hasattr(s, 'seller') else s.get('seller', '')
            price = s.price if hasattr(s, 'price') else s.get('price', 0)
            tx = s.tx_hash if hasattr(s, 'tx_hash') else s.get('tx_hash', '')
            token = s.token_id if hasattr(s, 'token_id') else s.get('token_id', '')
            new_sales.append({
                "slug": slug, "buyer": buyer, "seller": seller,
                "price": price, "token_id": token,
                "tx_hash": tx, "sold_at": ts,
            })

    # Store snapshots + sales to DB (fire and forget)
    try:
        from src.database import db
        if db._db:
            for snap in new_snapshots:
                await db.db.execute(
                    "INSERT INTO collection_snapshots "
                    "(slug, floor_price, volume_1d, sales_1d, listings_count, "
                    "listings_near_floor, unique_buyers_1h, unique_buyers_6h, "
                    "num_owners, captured_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (snap["slug"], snap["floor_price"], snap["volume_1d"],
                     snap["sales_1d"], snap["listings_count"], snap["listings_near_floor"],
                     snap["unique_buyers_1h"], snap["unique_buyers_6h"],
                     snap["num_owners"], snap["captured_at"]),
                )

            # Batch insert sales (skip duplicates by tx_hash)
            for sale in new_sales[:500]:  # Cap at 500 per cycle
                if sale["buyer"] and sale["sold_at"] > 0:
                    await db.db.execute(
                        "INSERT OR IGNORE INTO sales_log "
                        "(slug, buyer, seller, price, token_id, tx_hash, sold_at) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (sale["slug"], sale["buyer"], sale["seller"],
                         sale["price"], sale["token_id"], sale["tx_hash"],
                         sale["sold_at"]),
                    )

            # Update wallet_intel for observed buyers/sellers
            wallet_activity: Dict[str, Dict] = {}
            for sale in new_sales:
                for addr, role in [(sale["buyer"], "buy"), (sale["seller"], "sell")]:
                    if not addr:
                        continue
                    if addr not in wallet_activity:
                        wallet_activity[addr] = {"buys": 0, "sells": 0}
                    if role == "buy":
                        wallet_activity[addr]["buys"] += 1
                    else:
                        wallet_activity[addr]["sells"] += 1

            now_ts = int(time.time())
            for addr, act in wallet_activity.items():
                await db.db.execute(
                    "INSERT INTO wallet_intel (address, total_buys, total_sells, "
                    "last_seen, tier, updated_at) VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(address) DO UPDATE SET "
                    "total_buys = total_buys + ?, total_sells = total_sells + ?, "
                    "last_seen = ?, updated_at = ?, "
                    "tier = CASE "
                    "  WHEN total_buys + ? > 20 AND total_sells + ? > 10 THEN 'active' "
                    "  WHEN total_buys + ? > 50 THEN 'smart' "
                    "  ELSE tier END",
                    (addr, act["buys"], act["sells"], now_ts, "unknown", now_ts,
                     act["buys"], act["sells"], now_ts, now_ts,
                     act["buys"], act["sells"], act["buys"]),
                )

            await db.db.commit()
            logger.info(
                f"[ALPHA] Stored {len(new_snapshots)} snapshots, "
                f"{len(new_sales)} sales, {len(wallet_activity)} wallet updates"
            )
    except Exception as e:
        logger.debug(f"[ALPHA] DB storage failed (non-fatal): {e}")

    # Detect market regime from blue chip stats
    bc_stats = [c.get("_stats", {}) for c in enriched_bc if c.get("_stats")]
    regime = detect_market_regime(bc_stats)

    # Sort by opportunity score
    alpha_cards.sort(key=lambda c: c.opportunity_score, reverse=True)

    # Build dashboard header
    header = build_dashboard_header(alpha_cards, regime)

    t_elapsed = time.time() - t_start
    buy_count = sum(1 for c in alpha_cards if c.action == "BUY NOW")
    watch_count = sum(1 for c in alpha_cards if c.action == "WATCH CLOSELY")
    avoid_count = sum(1 for c in alpha_cards if c.action == "AVOID")
    logger.info(
        f"[ALPHA] Scoring complete in {t_elapsed:.1f}s — "
        f"{buy_count} BUY, {watch_count} WATCH, {avoid_count} AVOID | "
        f"Regime: {regime.regime}"
    )

    return {
        "alpha_cards": [c.to_dict() for c in alpha_cards],
        "dashboard_header": header.to_dict(),
        "regime": regime.to_dict(),
    }


async def run_analysis(force: bool = False) -> Dict:
    """Run the complete v7 intelligence pipeline (alpha engine).

    v7: Alpha engine integration.
    Runs legacy lanes + new 8-layer alpha scoring in parallel.
    """

    if _cache.valid and not force:
        return _cache.data
    if _cache.refreshing:
        return _cache.data or {"status": "loading", "message": "Analysis in progress..."}

    _cache.refreshing = True
    _progress["started_at"] = 0  # reset timer
    _set_progress("Initializing", 2, "Starting pipeline...")
    t_start = time.time()
    logger.info("[INSIGHTS] Starting v7 alpha pipeline...")

    client = _get_os_client()

    try:
        async with aiohttp.ClientSession(
            headers={"User-Agent": "MintBot/6.0"},
            connector=aiohttp.TCPConnector(limit=10, ttl_dns_cache=300),
        ) as session:

            # ────────────────────────────────────────
            # Step 1: Parallel data fetch (collection lists + news)
            # ────────────────────────────────────────
            chain_vol_tasks = [_fetch_top_collections_v6(client, ch, 50) for ch in CHAINS]
            chain_new_tasks = [_fetch_new_collections_v6(client, ch, 25) for ch in CHAINS]
            bluechip_task = _fetch_blue_chips_v6(client)
            twitter_task = scrape_twitter_via_jina(session)
            tempo_task = scan_tempo_mints(session)
            opensea_drops_task = client.fetch_drops()  # PRIMARY: featured drops
            discover_task = _discover_opensea_minting(client)  # DISCOVERY: find active mints across chains

            all_tasks = chain_vol_tasks + chain_new_tasks + [bluechip_task, twitter_task, tempo_task, opensea_drops_task, discover_task]
            _set_progress("Fetching data", 5, "Parallel fetch: collections, news, drops, discovery")
            results = await asyncio.gather(*all_tasks, return_exceptions=True)

            n = len(CHAINS)
            discovery_collections = []

            # Volume collections
            for i, res in enumerate(results[:n]):
                if isinstance(res, list):
                    discovery_collections.extend(res)

            # New collections
            for i, res in enumerate(results[n:n*2]):
                if isinstance(res, list):
                    discovery_collections.extend(res)

            blue_chips_raw = results[n*2] if isinstance(results[n*2], list) else []
            twitter_news_raw = results[n*2+1] if isinstance(results[n*2+1], list) else []
            tempo_data = results[n*2+2] if isinstance(results[n*2+2], dict) else {}
            opensea_drops = results[n*2+3] if isinstance(results[n*2+3], list) else []
            _discover_result = results[n*2+4] if isinstance(results[n*2+4], dict) else {}
            discovered_minting = _discover_result.get("minting", [])
            discovered_upcoming = _discover_result.get("upcoming", [])

            # Extract Tempo results
            tempo_minting = tempo_data.get("minting_now", [])
            tempo_recent = tempo_data.get("recently_minted", [])
            tempo_stats = tempo_data.get("stats", {})

            # ── Cross-validate OpenSea drops with real mint events ──
            # The drops API returns STALE is_minting status — collections 
            # marked is_minting=True may have already ended. We verify by 
            # checking for actual null-address mint events in the last 12h.
            os_drops_minting = []
            os_drops_upcoming = []
            NULL_ADDR = "0x0000000000000000000000000000000000000000"
            
            for drop in opensea_drops:
                slug = drop.get("collection_slug", "")
                
                # Check for recent mint events (null-address transfers)
                has_recent_mints = False
                drop_stats_vol = 0
                drop_stats_owners = 0
                try:
                    # Check recent events for real mints
                    cutoff = int((datetime.now(timezone.utc) - timedelta(hours=12)).timestamp())
                    events_data = await client._request(
                        f"events/collection/{slug}",
                        params={"event_type": "transfer", "after": str(cutoff), "limit": "50"}
                    )
                    if events_data:
                        events_list = events_data.get("asset_events", [])
                        mint_count = sum(
                            1 for e in events_list
                            if e.get("from_address", "") == NULL_ADDR
                            or e.get("seller", "") == NULL_ADDR
                        )
                        has_recent_mints = mint_count > 0
                        drop["_recent_mint_count"] = mint_count
                    
                    # Also get stats to check volume/owners
                    stats = await client.fetch_stats(slug)
                    if stats:
                        sd = stats.to_dict()
                        drop_stats_vol = sd.get("total_volume", 0) or 0
                        drop_stats_owners = sd.get("num_owners", 0) or 0
                    
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.debug(f"[INSIGHTS] Drop validation failed for {slug}: {e}")
                
                if has_recent_mints:
                    # VERIFIED: actual mints happening now
                    os_drops_minting.append(drop)
                elif drop.get("is_minting") and drop_stats_vol < 0.5 and drop_stats_owners < 50:
                    # API says minting, low volume → might be very early, keep as minting
                    os_drops_minting.append(drop)
                elif not drop.get("is_minting") and drop_stats_vol < 1.0 and drop_stats_owners < 100:
                    # Not yet minting, low volume → genuinely upcoming
                    os_drops_upcoming.append(drop)
                else:
                    # High volume or many owners + no recent mints → COMPLETED, skip
                    logger.info(f"[INSIGHTS] Drop '{slug}' classified as COMPLETED "
                                f"(vol={drop_stats_vol:.2f}, owners={drop_stats_owners}, "
                                f"recent_mints={drop.get('_recent_mint_count', 0)})")

            logger.info(
                f"[INSIGHTS] Fetched {len(discovery_collections)} discovery + "
                f"{len(blue_chips_raw)} blue chips + {len(twitter_news_raw)} tweets + "
                f"{len(tempo_minting)} Tempo active + "
                f"{len(tempo_recent)} Tempo recent + "
                f"{len(opensea_drops)} OpenSea drops "
                f"({len(os_drops_minting)} verified minting, "
                f"{len(os_drops_upcoming)} upcoming, "
                f"{len(opensea_drops) - len(os_drops_minting) - len(os_drops_upcoming)} completed)"
            )

            # ────────────────────────────────────────
            # Step 2: Separate blue chips from discovery
            # ────────────────────────────────────────
            bc_slugs = set(get_all_blue_chip_slugs())

            # Remove blue chips from discovery pipeline
            clean_discovery = [c for c in discovery_collections if c["slug"] not in bc_slugs]

            # Deduplicate discovery by slug
            seen = {}
            for c in clean_discovery:
                slug = c["slug"]
                if slug not in seen:
                    seen[slug] = c
            clean_discovery = list(seen.values())

            # ────────────────────────────────────────
            # Step 3: Batch enrich (v6: concurrent, not sequential)
            # ────────────────────────────────────────
            t_enrich = time.time()
            enriched_discovery = await _batch_enrich_collections(
                client, clean_discovery, max_items=60, fetch_mcp=True,
            )

            # Enrich blue chips (also batched)
            enriched_bc = await _batch_enrich_collections(
                client, blue_chips_raw, max_items=20, fetch_mcp=False,
            )
            # Blue chips get special originality/risk treatment
            for col in enriched_bc:
                col["_originality"] = {"label": "original", "confidence": 1.0, "reasons": []}
                col["_risk_flags"] = []

            enrich_time = time.time() - t_enrich
            diag = client.get_diagnostics()
            logger.info(
                f"[INSIGHTS] Batch enrichment: {len(enriched_discovery)} discovery + "
                f"{len(enriched_bc)} BC in {enrich_time:.1f}s "
                f"({diag['total_requests']} reqs, {diag['error_rate']}% errors)"
            )

            # ────────────────────────────────────────
            # Step 4: Match news to collections
            # ────────────────────────────────────────
            all_collections_for_match = enriched_discovery + enriched_bc
            twitter_news = match_news_to_collections(twitter_news_raw, all_collections_for_match)
            twitter_news = filter_noise(twitter_news)

            # Distribute matched news items to their collections
            for item in twitter_news:
                matched_slug = item.get("collection_match")
                if matched_slug:
                    for col in all_collections_for_match:
                        if col["slug"] == matched_slug:
                            col["_matched_news"].append(item)
                            break

            # ────────────────────────────────────────
            # Step 5: Score and classify into lanes
            # ────────────────────────────────────────

            # Score discovery collections
            for col in enriched_discovery:
                col["_scoring"] = score_discovery(
                    col,
                    news_items=col.get("_matched_news", []),
                    originality=col.get("_originality"),
                    risk_flags=col.get("_risk_flags"),
                )

            # Score blue chips for impact
            for col in enriched_bc:
                matched_news = col.get("_matched_news", [])
                col["_impact"] = score_bluechip_impact(col, catalysts=matched_news)

            # ── Lane 1: MINTING NOW ──
            # STRICT: Only collections with REAL null-address mints in freshness window.
            # Secondary trading does NOT count. No is_active_launch fallback.
            minting_now_raw = []
            for c in enriched_discovery:
                mint_info = c.get("_mint_info", {})
                # REQUIRE: true null-address mints, not just transfers
                if not mint_info.get("is_true_mint", False):
                    continue
                # REQUIRE: at least some mints in 24h
                if mint_info.get("mint_count_24h", 0) <= 0:
                    continue
                # REQUIRE: mint confidence is not "none"
                if mint_info.get("mint_confidence", "none") == "none":
                    continue
                minting_now_raw.append(c)
            
            # Also include blue chips that are truly minting (same strict rules)
            for bc in enriched_bc:
                bc_mint = bc.get("_mint_info", {})
                if bc_mint.get("is_true_mint") and bc_mint.get("mint_count_24h", 0) > 0:
                    bc["_scoring"] = score_discovery(bc)
                    minting_now_raw.append(bc)
            
            minting_now_raw.sort(
                key=lambda x: x.get("_mint_info", {}).get("mint_count_24h", 0),
                reverse=True,
            )
            minting_now = [_build_card(c, c["_scoring"], "minting_now")
                           for c in minting_now_raw[:MAX_PER_LANE]]

            # ── Merge OpenSea Drops (Minting Now) — ENRICHED ──
            # Each drop gets full stats + collection details from OpenSea API
            for drop in os_drops_minting:
                slug = drop.get("collection_slug", "")
                drop_name = drop.get("collection_name", "OpenSea Drop")
                drop_chain = drop.get("chain", "ethereum").upper()
                
                # Enrich with real stats
                drop_stats = {}
                drop_extra = {}
                try:
                    stats = await client.fetch_stats(slug)
                    if stats:
                        drop_stats = stats.to_dict()
                    await asyncio.sleep(0.3)
                    info = await client.fetch_collection(slug)
                    if info:
                        drop_extra = {
                            "description": (info.description or "")[:200],
                            "twitter": info.twitter or "",
                            "discord": info.discord or "",
                            "total_supply": getattr(info, 'total_supply', None),
                        }
                except Exception as e:
                    logger.debug(f"[INSIGHTS] Drop enrichment failed for {slug}: {e}")
                
                # Build a rich minting card
                os_url = drop.get("opensea_url") or f"https://opensea.io/collection/{slug}"
                floor = drop_stats.get("floor_price", 0) or 0
                vol_1d = drop_stats.get("volume_1d", 0) or 0
                owners = drop_stats.get("num_owners", 0) or 0
                sales_1d = drop_stats.get("sales_1d", 0) or 0
                
                reasons = []
                if vol_1d > 0:
                    reasons.append(f"24h volume: {vol_1d:.2f} ETH across {sales_1d} sales")
                if owners > 0:
                    reasons.append(f"{owners:,} unique owners")
                if drop_extra.get("twitter"):
                    reasons.append(f"Twitter: @{drop_extra['twitter']}")
                reasons.append(f"Verified OpenSea Drop on {drop_chain}")
                
                minting_now.insert(0, {
                    "slug": slug,
                    "name": drop_name,
                    "contract": drop.get("contract_address", ""),
                    "chain_label": drop_chain,
                    "image": drop.get("image_url", ""),
                    "source_name": "OpenSea Drop",
                    "catalyst": "Live Mint",
                    "score": 100,
                    "score_label": "High Confidence",
                    "score_label_color": "#22c55e",
                    "is_opensea_drop": True,
                    "is_minting": True,
                    # Enriched data
                    "num_owners": owners,
                    "floor_price": floor,
                    "floor_symbol": drop_stats.get("floor_symbol", "ETH"),
                    "volume_1d": vol_1d,
                    "sales_1d": sales_1d,
                    "supply": drop_extra.get("total_supply"),
                    "description": drop_extra.get("description", ""),
                    "reasons": reasons,
                    "primary_link": os_url,
                    "primary_link_label": "View on OpenSea",
                    "opensea_url": os_url,
                })

            # ── Merge Tempo Minting Now ──
            for tempo_card in tempo_minting:
                tempo_card["is_tempo"] = True
                minting_now.append(tempo_card)

            # ── Merge OpenSea Discovery Minting (MCP-verified) ──
            existing_minting_slugs = {c.get("slug", "") for c in minting_now}
            for disc_item in discovered_minting:
                slug = disc_item.get("slug", "")
                if slug and slug not in existing_minting_slugs:
                    existing_minting_slugs.add(slug)
                    minting_now.append(disc_item)
            logger.info(f"[INSIGHTS] Minting lane: {len(minting_now)} total after MCP merge")


            # ── Lane 2: FRESH DISCOVERY (non-BC, non-minting, by score) ──
            minting_slugs = {c["slug"] for c in minting_now_raw}
            # Track slugs from drops too
            for drop in os_drops_minting:
                slug = drop.get("collection_slug")
                if slug:
                    minting_slugs.add(slug)

            discovery_pool = [c for c in enriched_discovery
                              if c["slug"] not in minting_slugs
                              and c.get("_stats", {}).get("volume_1d", 0) > 0]
            discovery_pool.sort(
                key=lambda x: x["_scoring"]["score"],
                reverse=True,
            )
            fresh_discovery = [_build_card(c, c["_scoring"], "fresh_discovery")
                               for c in discovery_pool[:MAX_PER_LANE]]

            # ── Lane 3: GOOD CALLS (strong buy signals, min evidence) ──
            seen_slugs = minting_slugs | {c["slug"] for c in discovery_pool[:MAX_PER_LANE]}
            good_call_pool = [c for c in enriched_discovery
                              if c["slug"] not in seen_slugs
                              and c["_scoring"]["score"] >= 30
                              and c.get("_stats", {}).get("num_owners", 0) > 20]
            # Also include from discovery pool that have high scores + evidence
            for c in discovery_pool:
                if (c["slug"] not in seen_slugs
                    and c["_scoring"]["score"] >= 45
                    and len(c.get("_matched_news", [])) >= 1):
                    good_call_pool.append(c)
            # Deduplicate
            gc_seen = set()
            gc_unique = []
            for c in good_call_pool:
                if c["slug"] not in gc_seen:
                    gc_seen.add(c["slug"])
                    gc_unique.append(c)
            gc_unique.sort(key=lambda x: x["_scoring"]["score"], reverse=True)
            good_calls = [_build_card(c, c["_scoring"], "good_call")
                          for c in gc_unique[:MAX_PER_LANE]]

            # ── Lane 4: UPCOMING MINTS — OpenSea-first, quality-gated ──
            upcoming_cards = []
            
            # PRIMARY: MCP-discovered upcoming drops + OpenSea Drops
            for disc_upcoming in discovered_upcoming:
                slug = disc_upcoming.get("slug", "")
                if slug:
                    upcoming_cards.append(disc_upcoming)

            # SECONDARY: OpenSea Drops (is_minting=False) — enriched with real data
            for drop in os_drops_upcoming:
                slug = drop.get("collection_slug", "")
                drop_name = drop.get("collection_name", slug or "OpenSea Drop")
                drop_chain = drop.get("chain", "ethereum").upper()
                os_url = drop.get("opensea_url") or f"https://opensea.io/collection/{slug}"
                
                # Enrich with collection details
                desc = ""
                supply = None
                twitter = ""
                project_url = ""
                try:
                    info = await client.fetch_collection(slug)
                    if info:
                        desc = (info.description or "")[:250]
                        twitter = info.twitter or ""
                        project_url = info.website or ""
                        supply = info.total_supply or None
                    await asyncio.sleep(0.3)
                except Exception:
                    pass
                
                upcoming_cards.append({
                    "slug": slug,
                    "name": drop_name,
                    "contract": drop.get("contract_address", ""),
                    "chain": drop_chain,
                    "chain_label": drop_chain,
                    "image": drop.get("image_url", ""),
                    "date": "Upcoming Drop",
                    "price": drop.get("mint_price", "") or "TBA",
                    "source_name": "OpenSea Drop",
                    "link": os_url,
                    "description": desc,
                    "supply": supply,
                    "score": 95,
                    "score_label": "High Confidence",
                    "score_label_color": "#22c55e",
                    "risk": "Low",
                    "is_opensea_drop": True,
                    "reasons": [
                        "Featured on OpenSea drops page",
                        f"Twitter: @{twitter}" if twitter else "",
                        f"Project: {project_url}" if project_url else "",
                    ],
                })

            # Record upcoming slugs for dedup
            upcoming_slugs = {c["slug"] for c in upcoming_cards if c.get("slug")}
            # Also exclude blue chip slugs and minting slugs — these are NOT upcoming
            exclude_slugs = upcoming_slugs | bc_slugs | minting_slugs

            logger.info(f"[INSIGHTS] Upcoming: {len(upcoming_cards)} quality cards "
                        f"(OpenSea Drops: {len(os_drops_upcoming)})")

            # ── Lane 5: BLUE-CHIP ALERTS (show ALL tracked blue chips) ──
            bluechip_alerts = []
            for col in enriched_bc:
                impact = col.get("_impact", {})
                card = _build_bluechip_alert_card(col, impact)
                # Mark catalyzed ones with higher priority
                if should_alert_bluechip(impact.get("impact", 0)):
                    card["has_catalyst"] = True
                else:
                    card["has_catalyst"] = False
                bluechip_alerts.append(card)

            # Catalyzed ones first, then by volume
            bluechip_alerts.sort(
                key=lambda x: (x.get("has_catalyst", False), x.get("impact", 0)),
                reverse=True,
            )

            # ────────────────────────────────────────
            # Step 6: ALPHA ENGINE (new v7)
            # ────────────────────────────────────────
            t_alpha = time.time()
            _set_progress("Scoring", 85, "Running alpha engine on all collections")
            try:
                alpha_result = await _run_alpha_scoring(
                    client, enriched_discovery, twitter_news, enriched_bc,
                )
                alpha_cards = alpha_result.get("alpha_cards", [])
                dashboard_header = alpha_result.get("dashboard_header", {})
                regime = alpha_result.get("regime", {})
                alpha_time = time.time() - t_alpha
                logger.info(f"[INSIGHTS] Alpha engine completed in {alpha_time:.1f}s")
            except Exception as e:
                logger.exception(f"[INSIGHTS] Alpha engine failed (non-fatal): {e}")
                alpha_cards = []
                dashboard_header = {}
                regime = {}

            # ────────────────────────────────────────
            # Step 7: Build structured news feed
            # ────────────────────────────────────────
            structured_news = []
            for item in twitter_news:
                structured_news.append({
                    "title": item.get("title", ""),
                    "direct_url": item.get("direct_url", ""),
                    "fallback_url": item.get("fallback_url", ""),
                    "source": item.get("source", ""),
                    "source_label": item.get("source_label", ""),
                    "source_type": item.get("source_type", "unknown"),
                    "sentiment": item.get("sentiment", "neutral"),
                    "catalyst": item.get("catalyst", ""),
                    "catalyst_emoji": catalyst_emoji(item.get("catalyst", "")),
                    "sentiment_emoji": sentiment_emoji(item.get("sentiment", "neutral")),
                    "confidence": item.get("confidence", 0),
                    "score_effect": item.get("score_effect", 0),
                    "collection_match": item.get("collection_name", ""),
                    "collection_slug": item.get("collection_match", ""),
                    "published": item.get("published", ""),
                    "is_real_tweet": item.get("is_real_tweet", False),
                })

            # ────────────────────────────────────────
            # Step 8: Summary + payload
            # ────────────────────────────────────────
            t_elapsed = time.time() - t_start
            real_tweets = sum(1 for n in structured_news if n.get("is_real_tweet"))

            total_vol = sum(
                c.get("volume_1d", 0)
                for c in minting_now + fresh_discovery + good_calls
            )

            # Add Tempo recently minted to discovery
            for tempo_card in tempo_recent[:5]:
                tempo_card["is_tempo"] = True
                fresh_discovery.append(tempo_card)

            # Alpha-specific summary
            buy_count = sum(1 for c in alpha_cards if c.get("action") == "BUY NOW")
            watch_count = sum(1 for c in alpha_cards if c.get("action") == "WATCH CLOSELY")
            avoid_count = sum(1 for c in alpha_cards if c.get("action") == "AVOID")

            summary_parts = []
            if buy_count:
                summary_parts.append(f"🎯 {buy_count} BUY NOW signals")
            if watch_count:
                summary_parts.append(f"👀 {watch_count} collections to WATCH")
            if avoid_count:
                summary_parts.append(f"⚠️ {avoid_count} flagged AVOID")
            if minting_now:
                summary_parts.append(f"🟢 {len(minting_now)} live mints")
            if upcoming_cards:
                summary_parts.append(f"🚀 {len(upcoming_cards)} upcoming")
            if bluechip_alerts:
                summary_parts.append(f"👑 {len(bluechip_alerts)} BC alerts")
            if real_tweets:
                summary_parts.append(f"🐦 {real_tweets} live sources")
            regime_label = regime.get("regime_label", "")
            if regime_label:
                summary_parts.append(f"Market: {regime_label}")

            summary = ". ".join(summary_parts) + "." if summary_parts else "Analyzing..."

            payload = {
                "status": "ok",
                "engine_version": "v7-alpha",
                # Alpha engine output (new)
                "alpha_cards": alpha_cards,
                "dashboard_header": dashboard_header,
                "regime": regime,
                # Legacy lanes (backward compat)
                "minting_now": minting_now,
                "fresh_discovery": fresh_discovery,
                "good_calls": good_calls,
                "upcoming": upcoming_cards,
                "bluechip_alerts": bluechip_alerts,
                # Structured news feed
                "news": structured_news,
                # Backward compat aliases
                "trending": fresh_discovery,
                "twitter_news": structured_news,
                # Summary
                "market_summary": summary,
                "stats": {
                    "total_collections_analyzed": len(enriched_discovery) + len(enriched_bc),
                    "alpha_buy_count": buy_count,
                    "alpha_watch_count": watch_count,
                    "alpha_avoid_count": avoid_count,
                    "minting_now_count": len(minting_now),
                    "discovery_count": len(fresh_discovery),
                    "good_calls_count": len(good_calls),
                    "upcoming_count": len(upcoming_cards),
                    "bluechip_alerts_count": len(bluechip_alerts),
                    "real_tweets": real_tweets,
                    "news_total": len(structured_news),
                    "blue_chips_tracked": len(enriched_bc),
                    "analysis_time_ms": round(t_elapsed * 1000),
                    "data_sources": ["OpenSea API v2", "OpenSea MCP", "Alpha Engine v1", "Jina Reader (X/Twitter)", "NFT Calendars", "Tempo RPC (on-chain)"],
                    "chains_scanned": list(CHAIN_LABELS.values()),
                    "trending_count": len(fresh_discovery),
                    "twitter_feeds": len(structured_news),
                },
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

            _cache.set(payload)
            _finish_progress()

            # Log diagnostics
            diag = client.get_diagnostics()
            logger.info(
                f"[INSIGHTS] v7 pipeline complete in {t_elapsed:.1f}s — "
                f"Alpha: {buy_count} BUY / {watch_count} WATCH / {avoid_count} AVOID | "
                f"{len(minting_now)} minting, {len(fresh_discovery)} discovery, "
                f"{len(bluechip_alerts)} BC alerts, {real_tweets} tweets | "
                f"Regime: {regime.get('regime', 'N/A')} | "
                f"API: {diag['total_requests']} reqs ({diag['error_rate']}% err)"
            )
            return payload

    except Exception as e:
        logger.exception(f"[INSIGHTS] Pipeline failed: {e}")
        _cache.refreshing = False
        _finish_progress()
        return {
            "status": "error",
            "message": str(e)[:200],
            "minting_now": [], "fresh_discovery": [], "good_calls": [],
            "upcoming": [], "bluechip_alerts": [], "news": [],
            "trending": [], "twitter_news": [],
            "market_summary": "Analysis temporarily unavailable.",
            "stats": {},
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        _cache.refreshing = False


# ═══════════════════════════════════════════════════════
#  Background Loop
# ═══════════════════════════════════════════════════════

_bg_task: Optional[asyncio.Task] = None

async def _background_loop():
    while True:
        try:
            await run_analysis()
        except Exception as e:
            logger.exception(f"[INSIGHTS] Background loop error: {e}")
        await asyncio.sleep(BG_INTERVAL)


def start_background(loop=None):
    global _bg_task
    if _bg_task and not _bg_task.done():
        return
    if loop is None:
        loop = asyncio.get_event_loop()
    _bg_task = loop.create_task(_background_loop())
    logger.info("[INSIGHTS] v7 alpha background loop started (interval=5m)")


# Aliases for server.py compatibility
start_background_task = start_background

def get_cached() -> Optional[Dict]:
    return _cache.data if _cache.valid else None
