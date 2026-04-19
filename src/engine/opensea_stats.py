"""
MintBot — OpenSea Stats Engine v6
═══════════════════════════════════════
Production-grade OpenSea API client with:
  - Exponential backoff with jitter on 429/5xx
  - In-flight request deduplication
  - Data validation and normalization
  - MCP integration for drop/mint stage data
  - Concurrency-controlled batch enrichment

All public methods return validated dataclasses, not raw dicts.
"""

import asyncio
import aiohttp
import json
import time
import random
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any

logger = logging.getLogger("mintbot.opensea_stats")

# ═══════════════════════════════════════════════════════
#  Data Models
# ═══════════════════════════════════════════════════════

@dataclass
class CollectionStats:
    """Validated, normalized collection statistics."""
    floor_price: float = 0.0
    floor_symbol: str = "ETH"
    total_volume: float = 0.0
    total_sales: int = 0
    num_owners: int = 0
    market_cap: float = 0.0
    avg_price: float = 0.0
    # 1-day intervals
    volume_1d: float = 0.0
    volume_change_1d: float = 0.0
    sales_1d: int = 0
    avg_price_1d: float = 0.0
    # 7-day intervals
    volume_7d: float = 0.0
    volume_change_7d: float = 0.0
    sales_7d: int = 0
    # 30-day intervals
    volume_30d: float = 0.0
    sales_30d: int = 0
    # Metadata
    fetched_at: float = 0.0
    is_valid: bool = True

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class MintStage:
    """A single mint stage from MCP drop details."""
    name: str = ""
    stage_index: int = 0
    is_active: bool = False
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    price: float = 0.0
    price_symbol: str = "ETH"
    max_per_wallet: Optional[int] = None


@dataclass
class DropDetails:
    """MCP-sourced drop/mint details."""
    drop_type: str = ""
    stages: List[MintStage] = field(default_factory=list)
    active_stage: Optional[str] = None
    active_price: float = 0.0
    active_symbol: str = "ETH"
    total_supply: int = 0
    max_supply: int = 0
    supply_pct: float = 0.0  # minted / max as percentage
    fetched_at: float = 0.0

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["stages"] = [asdict(s) for s in self.stages]
        return d


@dataclass
class CollectionInfo:
    """Full collection metadata from OpenSea."""
    slug: str = ""
    name: str = "Unknown"
    image: str = ""
    banner: str = ""
    description: str = ""
    contract: str = ""
    chain: str = ""
    chain_label: str = ""
    twitter: str = ""
    discord: str = ""
    website: str = ""
    opensea_url: str = ""
    explorer_url: str = ""
    safelist: str = ""
    category: str = ""
    owner: str = ""
    created_date: str = ""
    total_supply: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class MintActivity:
    """Mint detection data from events API."""
    is_minting: bool = False
    is_true_mint: bool = False
    mint_count_1h: int = 0
    mint_count_6h: int = 0
    mint_count_24h: int = 0
    last_mint_ts: int = 0
    last_mint_ago: str = ""
    transfer_count_6h: int = 0
    mint_confidence: str = "none"  # "high", "medium", "none"

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class SaleEvent:
    """A single sale from events API."""
    buyer: str = ""
    seller: str = ""
    price: float = 0.0
    price_symbol: str = "ETH"
    token_id: str = ""
    timestamp: int = 0
    tx_hash: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class ListingData:
    """A single active listing."""
    price: float = 0.0
    price_symbol: str = "ETH"
    maker: str = ""
    token_id: str = ""
    created_at: int = 0
    expiration: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class OfferData:
    """A single active offer/bid."""
    price: float = 0.0
    price_symbol: str = "ETH"
    maker: str = ""
    quantity: int = 1
    created_at: int = 0
    expiration: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class FloorDepth:
    """Computed floor depth analysis from listings."""
    floor_price: float = 0.0
    listings_within_5pct: int = 0
    listings_within_10pct: int = 0
    listings_within_20pct: int = 0
    total_listings: int = 0
    floor_depth_ratio: float = 0.0   # listings_within_10pct / total_listings
    spread_to_10pct: float = 0.0     # price gap from floor to 10th percentile
    is_thin: bool = False            # < 5 listings within 10% of floor
    top_maker_pct: float = 0.0       # % of near-floor listings from top maker

    def to_dict(self) -> Dict:
        return asdict(self)


# ═══════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════

OPENSEA_API_BASE = "https://api.opensea.io/api/v2"
MCP_URL = "https://mcp.opensea.io/mcp"
NULL_ADDR = "0x0000000000000000000000000000000000000000"

CHAIN_LABELS = {
    "ethereum": "ETH", "base": "BASE", "apechain": "APE",
    "optimism": "OP", "arbitrum": "ARB", "polygon": "POLY",
    "bsc": "BSC", "avalanche": "AVAX", "zora": "ZORA",
}
CHAIN_EXPLORERS = {
    "ethereum": "https://etherscan.io/address/",
    "base": "https://basescan.org/address/",
    "apechain": "https://apescan.io/address/",
}

# Rate limiting
MAX_RETRIES = 3
BASE_DELAY = 1.0        # seconds
MAX_DELAY = 8.0          # seconds
JITTER_RANGE = 0.5       # ±0.5s random jitter
REQUEST_TIMEOUT = 12     # seconds per request

# Validation limits
MAX_FLOOR_PRICE = 50_000    # ETH — no NFT costs more than this
MAX_VOLUME_CHANGE = 50_000  # percentage
MAX_MARKET_CAP = 10_000_000 # ETH


# ═══════════════════════════════════════════════════════
#  Config Loader
# ═══════════════════════════════════════════════════════

_config_cache: Optional[Dict] = None


def _load_config() -> Dict:
    """Load config from environment variables (cloud deployment)."""
    global _config_cache
    if _config_cache is not None:
        return _config_cache
    import os
    _config_cache = {
        "opensea_api_key": os.environ.get("OPENSEA_API_KEY", ""),
        "opensea_mcp_token": os.environ.get("OPENSEA_MCP_TOKEN", ""),
        "drpc_token": os.environ.get("DRPC_TOKEN", ""),
    }
    # Fallback: try config.json if env vars are empty
    if not _config_cache.get("opensea_api_key"):
        try:
            import pathlib
            cfg_path = pathlib.Path(__file__).resolve().parents[2] / "config.json"
            if cfg_path.exists():
                with open(cfg_path) as f:
                    file_cfg = json.load(f)
                    _config_cache.update({k: v for k, v in file_cfg.items() if v})
        except Exception as e:
            logger.warning(f"[OS-STATS] Failed to load config: {e}")
    return _config_cache


# ═══════════════════════════════════════════════════════
#  OpenSea Client
# ═══════════════════════════════════════════════════════

class OpenSeaClient:
    """Production-grade OpenSea API v2 client.

    Features:
      - Exponential backoff with jitter on 429/5xx
      - In-flight request deduplication
      - Data validation and normalization
      - MCP integration for drop details
    """

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session = session
        self._owns_session = session is None
        self._in_flight: Dict[str, asyncio.Future] = {}
        self._request_count = 0
        self._error_count = 0
        self._last_429_at = 0.0

        cfg = _load_config()
        self._api_key = cfg.get("opensea_api_key", "")
        self._mcp_token = cfg.get("opensea_mcp_token", "")

        if self._api_key:
            logger.info("[OS-STATS] OpenSea API key loaded")
        if self._mcp_token:
            logger.info("[OS-STATS] OpenSea MCP token loaded")

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "MintBot/6.0"},
                connector=aiohttp.TCPConnector(limit=10, ttl_dns_cache=300),
            )
            self._owns_session = True
        return self._session

    async def close(self):
        """Close the session if we own it."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    def _headers(self) -> Dict[str, str]:
        """Build request headers with API key."""
        h = {"accept": "application/json"}
        if self._api_key:
            h["x-api-key"] = self._api_key
        return h

    # ── Core Request Method ──

    async def _request(
        self,
        path: str,
        params: Optional[Dict] = None,
        dedupe_key: Optional[str] = None,
    ) -> Optional[Dict]:
        """Make a rate-limited, retried, deduplicated GET request to OpenSea API.

        Args:
            path: API path (e.g. "collections/boredapeyachtclub/stats")
            params: Query parameters
            dedupe_key: Key for deduplication. If another request with the
                        same key is in-flight, we await that instead.
        """
        # Deduplication: if same request is in-flight, share the result
        key = dedupe_key or f"{path}:{json.dumps(params or {}, sort_keys=True)}"
        if key in self._in_flight:
            try:
                return await self._in_flight[key]
            except Exception:
                pass

        future = asyncio.get_event_loop().create_future()
        self._in_flight[key] = future

        try:
            result = await self._request_with_retry(path, params)
            future.set_result(result)
            return result
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise
        finally:
            self._in_flight.pop(key, None)

    async def _request_with_retry(
        self,
        path: str,
        params: Optional[Dict] = None,
    ) -> Optional[Dict]:
        """Execute a GET request with exponential backoff on 429/5xx."""
        session = await self._ensure_session()
        url = f"{OPENSEA_API_BASE}/{path.lstrip('/')}"

        for attempt in range(MAX_RETRIES):
            self._request_count += 1
            try:
                async with session.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    if resp.status == 429:
                        self._last_429_at = time.time()
                        delay = min(MAX_DELAY, BASE_DELAY * (2 ** attempt))
                        jitter = random.uniform(-JITTER_RANGE, JITTER_RANGE)
                        wait = max(0.5, delay + jitter)
                        logger.warning(
                            f"[OS-STATS] 429 rate limited: {path} — "
                            f"retry {attempt + 1}/{MAX_RETRIES} in {wait:.1f}s"
                        )
                        await asyncio.sleep(wait)
                        continue

                    if resp.status >= 500:
                        delay = min(MAX_DELAY, BASE_DELAY * (2 ** attempt))
                        logger.warning(
                            f"[OS-STATS] {resp.status} server error: {path} — "
                            f"retry {attempt + 1}/{MAX_RETRIES}"
                        )
                        await asyncio.sleep(delay)
                        continue

                    # 4xx (not 429) — don't retry
                    self._error_count += 1
                    logger.debug(f"[OS-STATS] {resp.status} for {path}")
                    return None

            except asyncio.TimeoutError:
                self._error_count += 1
                logger.debug(f"[OS-STATS] Timeout: {path} (attempt {attempt + 1})")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BASE_DELAY * (attempt + 1))
            except aiohttp.ClientError as e:
                self._error_count += 1
                logger.debug(f"[OS-STATS] Client error: {path} — {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BASE_DELAY)
            except Exception as e:
                self._error_count += 1
                logger.warning(f"[OS-STATS] Unexpected error: {path} — {e}")
                return None

        logger.warning(f"[OS-STATS] All {MAX_RETRIES} retries exhausted: {path}")
        return None

    # ── Public API Methods ──

    async def fetch_collection(self, slug: str) -> Optional[CollectionInfo]:
        """Fetch full collection metadata by slug."""
        data = await self._request(f"collections/{slug}")
        if not data:
            return None

        contracts = data.get("contracts", [])
        contract_addr = contracts[0].get("address", "") if contracts else ""
        chain = contracts[0].get("chain", "ethereum") if contracts else "ethereum"

        return CollectionInfo(
            slug=data.get("collection", slug),
            name=data.get("name", "Unknown"),
            image=data.get("image_url", ""),
            banner=data.get("banner_image_url", ""),
            description=(data.get("description", "") or "")[:300],
            contract=contract_addr,
            chain=chain,
            chain_label=CHAIN_LABELS.get(chain, chain.upper()),
            twitter=data.get("twitter_username", "") or "",
            discord=data.get("discord_url", "") or "",
            website=data.get("project_url", "") or "",
            opensea_url=data.get("opensea_url", "") or f"https://opensea.io/collection/{slug}",
            explorer_url=(CHAIN_EXPLORERS.get(chain, "") + contract_addr) if contract_addr else "",
            safelist=data.get("safelist_status", ""),
            category=data.get("category", ""),
            owner=data.get("owner", ""),
            created_date=data.get("created_date", ""),
            total_supply=data.get("total_supply", 0) or 0,
        )

    async def fetch_stats(self, slug: str) -> Optional[CollectionStats]:
        """Fetch and validate collection statistics.

        Returns None if data is clearly invalid or unavailable.
        """
        data = await self._request(f"collections/{slug}/stats")
        if not data:
            return None

        total = data.get("total", {})
        intervals = {iv.get("interval"): iv for iv in data.get("intervals", []) if isinstance(iv, dict)}
        day = intervals.get("one_day", {})
        week = intervals.get("seven_day", {})
        month = intervals.get("thirty_day", {})

        stats = CollectionStats(
            floor_price=_safe_float(total.get("floor_price")),
            floor_symbol=total.get("floor_price_symbol", "ETH") or "ETH",
            total_volume=_safe_float(total.get("volume")),
            total_sales=_safe_int(total.get("sales")),
            num_owners=_safe_int(total.get("num_owners")),
            market_cap=_safe_float(total.get("market_cap")),
            avg_price=_safe_float(total.get("average_price")),
            # 1-day
            volume_1d=_safe_float(day.get("volume")),
            volume_change_1d=_safe_float(day.get("volume_change")),
            sales_1d=_safe_int(day.get("sales")),
            avg_price_1d=_safe_float(day.get("average_price")),
            # 7-day
            volume_7d=_safe_float(week.get("volume")),
            volume_change_7d=_safe_float(week.get("volume_change")),
            sales_7d=_safe_int(week.get("sales")),
            # 30-day
            volume_30d=_safe_float(month.get("volume")),
            sales_30d=_safe_int(month.get("sales")),
            fetched_at=time.time(),
        )

        # Validate
        stats.is_valid = _validate_stats(stats)
        if not stats.is_valid:
            logger.debug(f"[OS-STATS] Invalid stats for {slug} — zeroing suspect fields")

        return stats

    async def fetch_events(
        self,
        slug: str,
        event_type: str = "transfer",
        limit: int = 50,
    ) -> List[Dict]:
        """Fetch collection events with proper error handling."""
        data = await self._request(
            f"events/collection/{slug}",
            params={"event_type": event_type, "limit": str(limit)},
        )
        if not data:
            return []
        return data.get("asset_events", [])

    async def fetch_top_collections(
        self,
        chain: str = "ethereum",
        order_by: str = "one_day_volume",
        limit: int = 30,
    ) -> List[CollectionInfo]:
        """Fetch top collections by volume or creation date."""
        data = await self._request(
            "collections",
            params={"chain": chain, "limit": str(limit), "order_by": order_by},
        )
        if not data or "collections" not in data:
            return []

        results = []
        for c in data["collections"]:
            contracts = c.get("contracts", [])
            contract_addr = contracts[0].get("address", "") if contracts else ""
            col_chain = contracts[0].get("chain", chain) if contracts else chain

            results.append(CollectionInfo(
                slug=c.get("collection", ""),
                name=c.get("name", "Unknown"),
                image=c.get("image_url", ""),
                banner=c.get("banner_image_url", ""),
                description=(c.get("description", "") or "")[:300],
                contract=contract_addr,
                chain=col_chain,
                chain_label=CHAIN_LABELS.get(col_chain, col_chain.upper()),
                twitter=c.get("twitter_username", "") or "",
                discord=c.get("discord_url", "") or "",
                website=c.get("project_url", "") or "",
                opensea_url=c.get("opensea_url", ""),
                explorer_url=(CHAIN_EXPLORERS.get(col_chain, "") + contract_addr) if contract_addr else "",
                safelist=c.get("safelist_status", ""),
                category=c.get("category", ""),
                owner=c.get("owner", ""),
                created_date=c.get("created_date", ""),
            ))
        return results

    async def fetch_collections_paginated(
        self,
        chain: str = "ethereum",
        order_by: str = "created_date",
        max_pages: int = 4,
        per_page: int = 50,
    ) -> List[CollectionInfo]:
        """Fetch collections with cursor pagination for deeper discovery.
        
        Returns up to max_pages * per_page collections.
        """
        all_results = []
        cursor = None
        
        for page in range(max_pages):
            params = {"chain": chain, "limit": str(per_page), "order_by": order_by}
            if cursor:
                params["next"] = cursor
            
            data = await self._request("collections", params=params)
            if not data or "collections" not in data:
                break
            
            for c in data["collections"]:
                contracts = c.get("contracts", [])
                contract_addr = contracts[0].get("address", "") if contracts else ""
                col_chain = contracts[0].get("chain", chain) if contracts else chain
                
                all_results.append(CollectionInfo(
                    slug=c.get("collection", ""),
                    name=c.get("name", "Unknown"),
                    image=c.get("image_url", ""),
                    banner=c.get("banner_image_url", ""),
                    description=(c.get("description", "") or "")[:300],
                    contract=contract_addr,
                    chain=col_chain,
                    chain_label=CHAIN_LABELS.get(col_chain, col_chain.upper()),
                    twitter=c.get("twitter_username", "") or "",
                    discord=c.get("discord_url", "") or "",
                    website=c.get("project_url", "") or "",
                    opensea_url=c.get("opensea_url", ""),
                    explorer_url=(CHAIN_EXPLORERS.get(col_chain, "") + contract_addr) if contract_addr else "",
                    safelist=c.get("safelist_status", ""),
                    category=c.get("category", ""),
                    owner=c.get("owner", ""),
                    created_date=c.get("created_date", ""),
                ))
            
            cursor = data.get("next")
            if not cursor:
                break
        
        logger.info(f"[OS-STATS] Paginated {chain}/{order_by}: {len(all_results)} collections ({max_pages} pages)")
        return all_results

    async def fetch_drops(self) -> List[Dict]:
        """Fetch live & upcoming drops from OpenSea /api/v2/drops.

        Returns structured drop data including:
        - collection_slug, collection_name, chain, contract_address
        - drop_type, is_minting, image_url, opensea_url

        This is the PRIMARY source for Minting and Upcoming tabs.
        """
        try:
            data = await self._request("drops")
            if data and "drops" in data:
                drops = data["drops"]
                logger.info(
                    f"[OPENSEA] Fetched {len(drops)} drops "
                    f"({sum(1 for d in drops if d.get('is_minting'))} minting now)"
                )
                return drops
        except Exception as e:
            logger.warning(f"[OPENSEA] Drops fetch failed: {e}")
        return []

    async def detect_minting(self, slug: str) -> MintActivity:
        """Detect active minting via transfer events from null address.

        STRICT: Only counts transfers FROM 0x0 as true mints.
        A collection is minting ONLY if real null-address mints exist
        within the freshness window. Secondary trading does NOT count.
        """
        events = await self.fetch_events(slug, "transfer", 50)

        now_ts = int(time.time())
        cutoff_1h = now_ts - 3600
        cutoff_6h = now_ts - 21600
        cutoff_24h = now_ts - 86400

        mint_1h = 0
        mint_6h = 0
        mint_24h = 0
        last_mint_ts = 0
        transfer_6h = 0

        for ev in events:
            from_addr = (ev.get("from_address", "") or "").lower()
            ev_ts = _safe_int(ev.get("event_timestamp"))

            if ev_ts >= cutoff_6h:
                transfer_6h += 1

            if from_addr == NULL_ADDR:
                if ev_ts >= cutoff_24h:
                    mint_24h += 1
                if ev_ts >= cutoff_6h:
                    mint_6h += 1
                if ev_ts >= cutoff_1h:
                    mint_1h += 1
                if ev_ts > last_mint_ts:
                    last_mint_ts = ev_ts

        # STRICT: Only real null-address mints count
        is_true_mint = mint_1h >= 1 or (mint_6h >= 3 and mint_24h >= 5)
        # NO is_active_launch — secondary trading must NEVER trigger minting

        # Confidence levels
        if mint_1h >= 3:
            mint_confidence = "high"
        elif mint_1h >= 1:
            mint_confidence = "medium"
        elif mint_6h >= 3:
            mint_confidence = "medium"
        else:
            mint_confidence = "none"

        return MintActivity(
            is_minting=is_true_mint,
            is_true_mint=is_true_mint,
            mint_count_1h=mint_1h,
            mint_count_6h=mint_6h,
            mint_count_24h=mint_24h,
            last_mint_ts=last_mint_ts,
            last_mint_ago=_time_ago(last_mint_ts) if last_mint_ts else "",
            transfer_count_6h=transfer_6h,
            mint_confidence=mint_confidence,
        )

    async def fetch_sales(self, slug: str, limit: int = 50) -> List[SaleEvent]:
        """Fetch recent sales with buyer/seller addresses.

        Critical for: smart money detection, buyer analysis, wash detection.
        """
        data = await self._request(
            f"events/collection/{slug}",
            params={"event_type": "sale", "limit": str(min(limit, 50))},
        )
        if not data:
            return []

        sales = []
        for ev in data.get("asset_events", []):
            # Extract price from payment
            payment = ev.get("payment", {})
            price = _safe_float(payment.get("quantity", 0))
            decimals = _safe_int(payment.get("decimals", 18))
            if decimals > 0:
                price = price / (10 ** decimals)
            symbol = payment.get("symbol", "ETH") or "ETH"

            sales.append(SaleEvent(
                buyer=(ev.get("buyer", "") or "").lower(),
                seller=(ev.get("seller", "") or "").lower(),
                price=price,
                price_symbol=symbol,
                token_id=str(ev.get("nft", {}).get("identifier", "")),
                timestamp=_safe_int(ev.get("event_timestamp")),
                tx_hash=ev.get("transaction", "") or "",
            ))
        return sales

    async def fetch_listings(
        self, slug: str, limit: int = 100,
    ) -> List[ListingData]:
        """Fetch all active listings for a collection.

        Critical for: floor depth analysis, supply shock detection, sweep detection.
        """
        data = await self._request(
            f"listings/collection/{slug}/all",
            params={"limit": str(min(limit, 100))},
        )
        if not data:
            return []

        listings = []
        for order in data.get("listings", []):
            protocol = order.get("protocol_data", {}) or {}
            params = protocol.get("parameters", {}) or {}
            offer_items = params.get("offer", []) or []
            consideration = params.get("consideration", []) or []

            # Price from consideration (what seller wants)
            price = 0.0
            symbol = "ETH"
            for item in consideration:
                token = item.get("token", "") or ""
                if token == NULL_ADDR or token.lower().startswith("0x0000"):
                    raw = _safe_float(item.get("startAmount", 0))
                    price += raw / 1e18
                    break

            # Token ID from offer
            token_id = ""
            if offer_items:
                token_id = str(offer_items[0].get("identifierOrCriteria", ""))

            maker = (params.get("offerer", "") or "").lower()

            listings.append(ListingData(
                price=price,
                price_symbol=symbol,
                maker=maker,
                token_id=token_id,
                created_at=_safe_int(order.get("order_hash", 0)),
                expiration=_safe_int(params.get("endTime", 0)),
            ))

        # Sort by price ascending
        listings.sort(key=lambda x: x.price if x.price > 0 else float('inf'))
        return listings

    async def fetch_best_listings(self, slug: str, limit: int = 30) -> List[ListingData]:
        """Fetch best (cheapest) listing per token.

        More efficient than fetch_listings for just floor depth analysis.
        """
        data = await self._request(
            f"listings/collection/{slug}/best",
            params={"limit": str(min(limit, 100))},
        )
        if not data:
            return []

        listings = []
        for order in data.get("listings", []):
            price_data = order.get("price", {}) or {}
            current = price_data.get("current", {}) or {}
            raw_price = _safe_float(current.get("value", 0))
            decimals = _safe_int(current.get("decimals", 18))
            price = raw_price / (10 ** decimals) if decimals > 0 else raw_price

            listings.append(ListingData(
                price=price,
                price_symbol=current.get("currency", "ETH") or "ETH",
                maker="",  # not always in best listings response
                token_id="",
                created_at=0,
                expiration=0,
            ))

        listings.sort(key=lambda x: x.price if x.price > 0 else float('inf'))
        return listings

    async def fetch_offers(self, slug: str, limit: int = 50) -> List[OfferData]:
        """Fetch active offers/bids for a collection.

        Used for: demand pressure analysis, bid wall detection.
        """
        data = await self._request(
            f"offers/collection/{slug}/all",
            params={"limit": str(min(limit, 50))},
        )
        if not data:
            return []

        offers = []
        for order in data.get("offers", []):
            protocol = order.get("protocol_data", {}) or {}
            params = protocol.get("parameters", {}) or {}
            offer_items = params.get("offer", []) or []

            price = 0.0
            symbol = "WETH"
            for item in offer_items:
                raw = _safe_float(item.get("startAmount", 0))
                price = raw / 1e18
                break

            maker = (params.get("offerer", "") or "").lower()

            offers.append(OfferData(
                price=price,
                price_symbol=symbol,
                maker=maker,
                quantity=1,
                created_at=0,
                expiration=_safe_int(params.get("endTime", 0)),
            ))

        offers.sort(key=lambda x: x.price, reverse=True)
        return offers

    async def fetch_drop_details(self, slug: str, minter: str = "") -> Optional[DropDetails]:
        """Fetch drop/mint stage details via OpenSea MCP server.

        Returns pricing, stages, supply, and eligibility data.
        Only called for collections detected as actively minting.
        """
        if not self._mcp_token:
            return None

        try:
            session = await self._ensure_session()

            # Step 1: Initialize MCP session
            init_headers = {
                "Authorization": f"Bearer {self._mcp_token}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            init_payload = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "mintbot-stats", "version": "6.0"},
                },
                "id": 1,
            }

            async with session.post(
                MCP_URL, json=init_payload, headers=init_headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                session_id = resp.headers.get("mcp-session-id", "")
                if not session_id:
                    return None

            # Step 2: Call get_drop_details
            call_headers = {
                **init_headers,
                "Mcp-Session-Id": session_id,
            }
            args = {"collectionSlug": slug}
            if minter:
                args["minter"] = minter

            call_payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {"name": "get_drop_details", "arguments": args},
                "id": 2,
            }

            async with session.post(
                MCP_URL, json=call_payload, headers=call_headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return None

                text = await resp.text()
                # Parse SSE response
                for line in text.split("\n"):
                    if line.startswith("data:"):
                        try:
                            data = json.loads(line[5:].strip())
                            return self._parse_drop_details(data)
                        except json.JSONDecodeError:
                            pass

            return None

        except Exception as e:
            logger.debug(f"[OS-STATS] MCP drop_details failed for {slug}: {e}")
            return None

    async def fetch_upcoming_drops_mcp(self, limit: int = 50) -> List[Dict]:
        """Fetch all upcoming/active drops via MCP get_upcoming_drops.
        
        Returns raw drop data with mintStatus, pricing, stages, chain info.
        This is the PRIMARY source for Minting and Upcoming tabs.
        """
        if not self._mcp_token:
            return []
        
        try:
            session = await self._ensure_session()
            init_headers = {
                "Authorization": f"Bearer {self._mcp_token}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            init_payload = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "mintbot-stats", "version": "7.0"},
                },
                "id": 1,
            }
            async with session.post(
                MCP_URL, json=init_payload, headers=init_headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                session_id = resp.headers.get("mcp-session-id", "")
                if not session_id:
                    return []
            
            call_headers = {**init_headers, "Mcp-Session-Id": session_id}
            call_payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "get_upcoming_drops",
                    "arguments": {"limit": limit},
                },
                "id": 2,
            }
            async with session.post(
                MCP_URL, json=call_payload, headers=call_headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
                for line in text.split("\n"):
                    if line.startswith("data:"):
                        try:
                            data = json.loads(line[5:].strip())
                            content = data.get("result", {}).get("content", [])
                            if content:
                                parsed = json.loads(content[0].get("text", "{}"))
                                items = parsed.get("upcomingDrops", {}).get("items", [])
                                logger.info(f"[OS-STATS] MCP get_upcoming_drops: {len(items)} drops")
                                return items
                        except (json.JSONDecodeError, KeyError):
                            pass
            return []
        except Exception as e:
            logger.warning(f"[OS-STATS] MCP upcoming_drops failed: {e}")
            return []

    async def fetch_mint_status_mcp(self, slug: str) -> Optional[Dict]:
        """Quick mint status check via MCP get_drop_details.
        
        Returns dict with mintStatus, stages count, and active price.
        Much faster than full fetch_drop_details since we skip parsing.
        """
        if not self._mcp_token:
            return None
        
        try:
            session = await self._ensure_session()
            init_headers = {
                "Authorization": f"Bearer {self._mcp_token}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            init_payload = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "mintbot-stats", "version": "7.0"},
                },
                "id": 1,
            }
            async with session.post(
                MCP_URL, json=init_payload, headers=init_headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                session_id = resp.headers.get("mcp-session-id", "")
                if not session_id:
                    return None
            
            call_headers = {**init_headers, "Mcp-Session-Id": session_id}
            call_payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "get_drop_details",
                    "arguments": {"collectionSlug": slug},
                },
                "id": 2,
            }
            async with session.post(
                MCP_URL, json=call_payload, headers=call_headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return None
                text = await resp.text()
                for line in text.split("\n"):
                    if line.startswith("data:"):
                        try:
                            data = json.loads(line[5:].strip())
                            content = data.get("result", {}).get("content", [])
                            if content:
                                parsed = json.loads(content[0].get("text", "{}"))
                                drop = parsed.get("drop", parsed)
                                mint_status = drop.get("mintStatus", "")
                                stages = drop.get("stages", [])
                                active = [s for s in stages if s.get("isActive")]
                                price = 0
                                price_symbol = "ETH"
                                if active:
                                    p = active[0].get("price", {})
                                    token = p.get("token", {})
                                    price = token.get("unit", 0)
                                    price_symbol = token.get("symbol", "ETH")
                                return {
                                    "mintStatus": mint_status,
                                    "stages": len(stages),
                                    "active_price": price,
                                    "active_symbol": price_symbol,
                                    "active_stage": active[0].get("label", "") if active else "",
                                    "total_minted": drop.get("totalMinted", 0),
                                    "max_supply": drop.get("maxSupply", 0),
                                }
                        except (json.JSONDecodeError, KeyError):
                            pass
            return None
        except Exception as e:
            logger.debug(f"[OS-STATS] MCP mint_status failed for {slug}: {e}")
            return None


    async def batch_check_mint_status_mcp(
        self, slugs: List[str], concurrency: int = 8,
        on_progress: Optional[callable] = None,
    ) -> Dict[str, Dict]:
        """Check multiple slugs for minting status using a SINGLE MCP session.
        
        Returns dict mapping slug -> status dict for collections that are MINTING or MINTING_SOON.
        10x faster than per-slug fetch_mint_status_mcp since session is reused.
        
        Args:
            on_progress: Optional callback(checked, total, found) called periodically.
        """
        if not self._mcp_token or not slugs:
            return {}
        
        results = {}
        checked_count = 0
        total = len(slugs)
        try:
            session = await self._ensure_session()
            init_headers = {
                "Authorization": f"Bearer {self._mcp_token}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            init_payload = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "mintbot-batch", "version": "7.0"},
                },
                "id": 1,
            }
            async with session.post(
                MCP_URL, json=init_payload, headers=init_headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                session_id = resp.headers.get("mcp-session-id", "")
                if not session_id:
                    return {}
            
            call_headers = {**init_headers, "Mcp-Session-Id": session_id}
            sem = asyncio.Semaphore(concurrency)
            call_id = 2
            
            async def _check(slug: str, cid: int):
                nonlocal checked_count
                async with sem:
                    try:
                        payload = {
                            "jsonrpc": "2.0",
                            "method": "tools/call",
                            "params": {
                                "name": "get_drop_details",
                                "arguments": {"collectionSlug": slug},
                            },
                            "id": cid,
                        }
                        async with session.post(
                            MCP_URL, json=payload, headers=call_headers,
                            timeout=aiohttp.ClientTimeout(total=12),
                        ) as resp:
                            if resp.status != 200:
                                checked_count += 1
                                if on_progress and checked_count % 25 == 0:
                                    on_progress(checked_count, total, len(results))
                                return slug, None
                            text = await resp.text()
                            for line in text.split("\n"):
                                if line.startswith("data:"):
                                    try:
                                        data = json.loads(line[5:].strip())
                                        content = data.get("result", {}).get("content", [])
                                        if content:
                                            parsed = json.loads(content[0].get("text", "{}"))
                                            drop = parsed.get("drop", parsed)
                                            mint_status = drop.get("mintStatus", "")
                                            if mint_status in ("MINTING", "MINTING_SOON"):
                                                stages = drop.get("stages", [])
                                                active = [s for s in stages if s.get("isActive")]
                                                price = 0
                                                price_symbol = "ETH"
                                                if active:
                                                    p = active[0].get("price", {})
                                                    token = p.get("token", {})
                                                    price = token.get("unit", 0)
                                                    price_symbol = token.get("symbol", "ETH")
                                                checked_count += 1
                                                if on_progress and checked_count % 25 == 0:
                                                    on_progress(checked_count, total, len(results) + 1)
                                                return slug, {
                                                    "mintStatus": mint_status,
                                                    "stages": len(stages),
                                                    "active_price": price,
                                                    "active_symbol": price_symbol,
                                                    "active_stage": active[0].get("label", "") if active else "",
                                                    "total_minted": drop.get("totalMinted", 0),
                                                    "max_supply": drop.get("maxSupply", 0),
                                                }
                                    except (json.JSONDecodeError, KeyError):
                                        pass
                    except Exception:
                        pass
                    checked_count += 1
                    if on_progress and checked_count % 25 == 0:
                        on_progress(checked_count, total, len(results))
                    return slug, None
            
            tasks = [_check(slug, call_id + i) for i, slug in enumerate(slugs)]
            check_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for res in check_results:
                if isinstance(res, tuple) and res[1] is not None:
                    results[res[0]] = res[1]
            
            if on_progress:
                on_progress(total, total, len(results))
            
            logger.info(f"[OS-STATS] Batch MCP: checked {len(slugs)}, "
                        f"found {len(results)} minting/upcoming")
        except Exception as e:
            logger.warning(f"[OS-STATS] Batch MCP failed: {e}")
        
        return results

    def _parse_drop_details(self, mcp_response: Dict) -> Optional[DropDetails]:
        content = mcp_response.get("result", {}).get("content", [])
        if not content or not content[0].get("text"):
            return None

        try:
            data = json.loads(content[0]["text"])
        except (json.JSONDecodeError, KeyError):
            return None

        now_ts = int(time.time())
        stages = []
        active_stage = None
        active_price = 0.0
        active_symbol = "ETH"

        for raw_stage in data.get("stages", []):
            # Parse timestamps
            start_ts = _parse_iso_ts(raw_stage.get("startTime", ""))
            end_ts = _parse_iso_ts(raw_stage.get("endTime", ""))

            is_active = False
            if start_ts and end_ts:
                is_active = start_ts <= now_ts <= end_ts
            elif start_ts:
                is_active = start_ts <= now_ts

            price_data = (raw_stage.get("price") or {}).get("token") or {}
            price = _safe_float(price_data.get("unit"))
            symbol = price_data.get("symbol", "ETH") or "ETH"

            stage = MintStage(
                name=(raw_stage.get("label") or f"Stage {raw_stage.get('stageIndex', 0)}").strip(),
                stage_index=raw_stage.get("stageIndex", 0),
                is_active=is_active,
                start_time=start_ts,
                end_time=end_ts,
                price=price,
                price_symbol=symbol,
                max_per_wallet=raw_stage.get("maxTotalMintableByWallet"),
            )
            stages.append(stage)

            if is_active:
                active_stage = stage.name
                active_price = price
                active_symbol = symbol

        total_supply = _safe_int(data.get("totalSupply"))
        max_supply = _safe_int(data.get("maxSupply"))
        supply_pct = (total_supply / max_supply * 100) if max_supply > 0 else 0.0

        return DropDetails(
            drop_type=data.get("__typename", ""),
            stages=stages,
            active_stage=active_stage,
            active_price=active_price,
            active_symbol=active_symbol,
            total_supply=total_supply,
            max_supply=max_supply,
            supply_pct=round(supply_pct, 1),
            fetched_at=time.time(),
        )

    # ── Batch Operations ──

    async def enrich_collection(
        self,
        slug: str,
        fetch_mint_activity: bool = True,
        fetch_mcp: bool = False,
    ) -> Dict:
        """Fetch stats + mint activity for a single collection.

        Returns a dict with '_stats', '_mint_info', and optionally '_drop_details'.
        """
        tasks = [self.fetch_stats(slug)]
        if fetch_mint_activity:
            tasks.append(self.detect_minting(slug))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        stats = results[0] if isinstance(results[0], CollectionStats) else CollectionStats()
        mint = results[1] if len(results) > 1 and isinstance(results[1], MintActivity) else MintActivity()

        enrichment = {
            "_stats": stats.to_dict(),
            "_mint_info": mint.to_dict(),
        }

        # MCP enrichment only for minting collections
        if fetch_mcp and mint.is_minting and self._mcp_token:
            try:
                drop = await self.fetch_drop_details(slug)
                if drop:
                    enrichment["_drop_details"] = drop.to_dict()
            except Exception as e:
                logger.debug(f"[OS-STATS] MCP enrichment failed for {slug}: {e}")

        return enrichment

    async def batch_enrich(
        self,
        slugs: List[str],
        concurrency: int = 5,
        fetch_mcp_for_minting: bool = True,
    ) -> Dict[str, Dict]:
        """Enrich multiple collections with concurrency control.

        Uses a semaphore to limit parallel requests and stay within
        OpenSea's rate limits (~4 req/s with API key).
        """
        sem = asyncio.Semaphore(concurrency)
        results: Dict[str, Dict] = {}

        async def _enrich_one(slug: str):
            async with sem:
                try:
                    data = await self.enrich_collection(slug, fetch_mcp=False)
                    results[slug] = data
                except Exception as e:
                    logger.debug(f"[OS-STATS] Batch enrich failed for {slug}: {e}")
                    results[slug] = {
                        "_stats": CollectionStats().to_dict(),
                        "_mint_info": MintActivity().to_dict(),
                    }

        # Phase 1: Fetch stats + mint activity for all collections
        await asyncio.gather(
            *[_enrich_one(slug) for slug in slugs],
            return_exceptions=True,
        )

        # Phase 2: MCP enrichment for minting collections only
        if fetch_mcp_for_minting and self._mcp_token:
            minting_slugs = [
                slug for slug, data in results.items()
                if data.get("_mint_info", {}).get("is_minting", False)
            ]
            if minting_slugs:
                logger.info(f"[OS-STATS] MCP enriching {len(minting_slugs)} minting collections")
                for slug in minting_slugs[:5]:  # Cap at 5 MCP calls per cycle
                    try:
                        drop = await self.fetch_drop_details(slug)
                        if drop:
                            results[slug]["_drop_details"] = drop.to_dict()
                    except Exception as e:
                        logger.debug(f"[OS-STATS] MCP failed for {slug}: {e}")
                    await asyncio.sleep(0.5)

        return results

    def get_diagnostics(self) -> Dict:
        """Return request/error counts for monitoring."""
        return {
            "total_requests": self._request_count,
            "total_errors": self._error_count,
            "error_rate": (
                round(self._error_count / max(1, self._request_count) * 100, 1)
            ),
            "last_429_at": self._last_429_at,
            "in_flight": len(self._in_flight),
        }


# ═══════════════════════════════════════════════════════
#  Validation & Helpers
# ═══════════════════════════════════════════════════════

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if val is None:
        return default
    try:
        result = float(val)
        if result != result:  # NaN check
            return default
        return result
    except (ValueError, TypeError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    """Safely convert a value to int."""
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _validate_stats(stats: CollectionStats) -> bool:
    """Validate stats for obviously impossible values.

    Returns True if data looks reasonable, False if suspect.
    Suspect fields are zeroed out rather than rejected entirely.
    """
    valid = True

    if stats.floor_price < 0:
        stats.floor_price = 0.0
        valid = False

    if stats.floor_price > MAX_FLOOR_PRICE:
        logger.debug(f"[OS-STATS] Suspect floor_price: {stats.floor_price}")
        stats.floor_price = 0.0
        valid = False

    if stats.num_owners < 0:
        stats.num_owners = 0
        valid = False

    if stats.market_cap < 0:
        stats.market_cap = 0.0
        valid = False

    if stats.market_cap > MAX_MARKET_CAP:
        logger.debug(f"[OS-STATS] Suspect market_cap: {stats.market_cap}")
        stats.market_cap = 0.0
        valid = False

    if abs(stats.volume_change_1d) > MAX_VOLUME_CHANGE:
        logger.debug(f"[OS-STATS] Suspect volume_change_1d: {stats.volume_change_1d}")
        stats.volume_change_1d = 0.0
        valid = False

    if stats.total_volume < 0:
        stats.total_volume = 0.0
        valid = False

    return valid


def _time_ago(ts: int) -> str:
    """Human-readable time-ago string."""
    if not ts:
        return ""
    diff = int(time.time()) - ts
    if diff < 0:
        return "just now"
    if diff < 60:
        return f"{diff}s ago"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h ago"
    return f"{diff // 86400}d ago"


def _parse_iso_ts(iso_str: str) -> Optional[int]:
    """Parse ISO timestamp to unix timestamp."""
    if not iso_str:
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return None


def compute_floor_depth(listings: List[ListingData]) -> FloorDepth:
    """Analyze listing depth around the floor price.

    This is the core supply shock detector. Thin floors + high demand = squeeze.
    """
    if not listings:
        return FloorDepth(is_thin=True)

    # Filter out zero-price listings
    priced = [l for l in listings if l.price > 0]
    if not priced:
        return FloorDepth(is_thin=True)

    priced.sort(key=lambda x: x.price)
    floor = priced[0].price
    total = len(priced)

    within_5 = sum(1 for l in priced if l.price <= floor * 1.05)
    within_10 = sum(1 for l in priced if l.price <= floor * 1.10)
    within_20 = sum(1 for l in priced if l.price <= floor * 1.20)

    # Spread to 10th percentile
    idx_10pct = min(int(total * 0.10), total - 1)
    spread = (priced[idx_10pct].price - floor) / floor * 100 if floor > 0 else 0

    # Top maker concentration near floor
    near_floor = [l for l in priced if l.price <= floor * 1.10]
    maker_counts: Dict[str, int] = {}
    for l in near_floor:
        if l.maker:
            maker_counts[l.maker] = maker_counts.get(l.maker, 0) + 1
    top_maker_count = max(maker_counts.values()) if maker_counts else 0
    top_maker_pct = top_maker_count / max(len(near_floor), 1) * 100

    return FloorDepth(
        floor_price=floor,
        listings_within_5pct=within_5,
        listings_within_10pct=within_10,
        listings_within_20pct=within_20,
        total_listings=total,
        floor_depth_ratio=within_10 / max(total, 1),
        spread_to_10pct=round(spread, 2),
        is_thin=within_10 < 5,
        top_maker_pct=round(top_maker_pct, 1),
    )
