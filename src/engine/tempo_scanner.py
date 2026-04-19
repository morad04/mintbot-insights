"""
MintBot — Tempo Network NFT Scanner
════════════════════════════════════════════
Real-time on-chain NFT mint detection for Tempo Network.

Uses direct EVM RPC calls (eth_getLogs + eth_call) to:
  - Detect active mints by scanning Transfer events from 0x0 (null address)
  - Fetch contract metadata (name, symbol, totalSupply) via eth_call
  - Track mint velocity and categorize into Minting Now vs Recently Minted

No API keys required — uses public Tempo RPC endpoint.

Data Sources:
  - Tempo RPC (https://rpc.tempo.xyz) — on-chain logs + contract reads
  - Stablewhel Launchpad — known launchpad contracts
"""

import asyncio
import aiohttp
import time
import logging
import re
from typing import Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger("mintbot.tempo")

# ═══════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════

TEMPO_CHAIN_ID = 4217
TEMPO_EXPLORER = "https://explore.mainnet.tempo.xyz"
STABLEWHEL_LAUNCHPAD = "https://stablewhel.xyz/launchpad"

def _get_tempo_rpc() -> str:
    """Get the fastest Tempo RPC from config (QuickNode > DRPC > public)."""
    try:
        from src.config.settings import NETWORKS
        rpcs = NETWORKS.get("TEMPO", {}).get("rpc", [])
        return rpcs[0] if rpcs else "https://rpc.tempo.xyz"
    except Exception:
        return "https://rpc.tempo.xyz"

TEMPO_RPC = _get_tempo_rpc()

# ERC-721 Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NULL_ADDR_TOPIC = "0x0000000000000000000000000000000000000000000000000000000000000000"

# ERC-721/ERC-1155 function signatures for metadata
NAME_SIG = "0x06fdde03"       # name()
SYMBOL_SIG = "0x95d89b41"     # symbol()
TOTAL_SUPPLY_SIG = "0x18160ddd"  # totalSupply()

# Scan parameters
SCAN_BLOCKS_ACTIVE = 5000     # ~recent hours for "minting now"
SCAN_BLOCKS_RECENT = 30000    # ~wider window for "recently minted"
MIN_MINTS_ACTIVE = 2          # minimum mints to be considered "active"
MIN_MINTS_RECENT = 5          # minimum mints to be in "recently minted"

# Cache
CACHE_TTL = 180               # 3 minutes
_tempo_cache: Optional[Dict] = None
_tempo_cache_ts: float = 0

# Known non-NFT contracts to exclude (LP tokens, stablecoins, test contracts)
EXCLUDE_PATTERNS = {
    "uniswap v2", "uniswap v3", "uni-v2", "uni-v3",
    "bridged usdc", "usdc.e", "layerzeroendpointdollar",
    "wrapped", "weth", "wbtc", "lzd",
    "amm e2e", "amm test", "test token", "test nft",
    "mock", "debug", "placeholder",
}

# Additional name-based heuristic filters
def _is_valid_name(name: str) -> bool:
    """Filter out garbage/test contract names."""
    name_lower = name.lower().strip()
    if not name_lower or len(name_lower) < 2:
        return False
    if any(excl in name_lower for excl in EXCLUDE_PATTERNS):
        return False
    # Filter out names that look like test hashes (e.g. "mnkcwfkj-e6b069")
    if re.search(r'[a-z0-9]{6,}-[a-z0-9]{5,}', name_lower):
        return False
    # Filter out single-char, numeric-only, or too-short names
    if len(name_lower) <= 3 and name_lower.replace(' ', '').isalnum():
        return False
    if name_lower.isdigit():
        return False
    return True


# ═══════════════════════════════════════════════════════
#  RPC Helpers
# ═══════════════════════════════════════════════════════

async def _rpc_call(session: aiohttp.ClientSession, method: str, params: list, id: int = 1) -> Optional[dict]:
    """Execute a JSON-RPC call to Tempo network."""
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": id}
    try:
        async with session.post(
            TEMPO_RPC,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as resp:
            if resp.status != 200:
                logger.debug(f"[TEMPO] RPC {method} failed: status={resp.status}")
                return None
            data = await resp.json()
            if "error" in data:
                logger.debug(f"[TEMPO] RPC {method} error: {data['error']}")
                return None
            return data
    except Exception as e:
        logger.debug(f"[TEMPO] RPC {method} exception: {e}")
        return None


async def _eth_call(session: aiohttp.ClientSession, to: str, data: str) -> str:
    """Execute eth_call and return hex result."""
    result = await _rpc_call(session, "eth_call", [{"to": to, "data": data}, "latest"])
    return result.get("result", "0x") if result else "0x"


def _decode_string(hex_str: str) -> str:
    """Decode an ABI-encoded string from hex."""
    if not hex_str or hex_str == "0x" or len(hex_str) < 130:
        return ""
    try:
        raw = hex_str[2:]  # strip 0x
        offset = int(raw[0:64], 16) * 2
        length = int(raw[offset:offset + 64], 16)
        string_hex = raw[offset + 64:offset + 64 + length * 2]
        return bytes.fromhex(string_hex).decode('utf-8', errors='ignore').strip()
    except Exception:
        return ""


def _decode_uint(hex_str: str) -> int:
    """Decode an ABI-encoded uint256 from hex."""
    if not hex_str or hex_str == "0x":
        return 0
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return 0


# ═══════════════════════════════════════════════════════
#  Core Scanner
# ═══════════════════════════════════════════════════════

async def scan_tempo_mints(session: aiohttp.ClientSession) -> Dict:
    """Scan Tempo Network for active and recent NFT mints.
    
    Returns:
        {
            "minting_now": [list of actively minting collections],
            "recently_minted": [list of recently minted collections],
            "stats": {total_mints, unique_contracts, latest_block}
        }
    """
    global _tempo_cache, _tempo_cache_ts
    
    if _tempo_cache and (time.time() - _tempo_cache_ts) < CACHE_TTL:
        return _tempo_cache

    logger.info("[TEMPO] Starting on-chain mint scan...")
    t_start = time.time()

    # Get latest block number
    block_result = await _rpc_call(session, "eth_blockNumber", [])
    if not block_result:
        logger.warning("[TEMPO] Cannot get latest block — RPC unreachable")
        return {"minting_now": [], "recently_minted": [], "stats": {}}

    latest_block = int(block_result["result"], 16)
    
    # ── Scan 1: Active mints (recent blocks) ──
    active_from = hex(max(0, latest_block - SCAN_BLOCKS_ACTIVE))
    active_logs = await _fetch_mint_logs(session, active_from, hex(latest_block))
    
    # ── Scan 2: Extended window (for recently minted) ──
    extended_from = hex(max(0, latest_block - SCAN_BLOCKS_RECENT))
    extended_to = hex(max(0, latest_block - SCAN_BLOCKS_ACTIVE))
    extended_logs = await _fetch_mint_logs(session, extended_from, extended_to)
    
    # Group by contract
    active_contracts = _group_by_contract(active_logs, latest_block)
    extended_contracts = _group_by_contract(extended_logs, latest_block)
    
    # Merge extended into one "all recent" view
    all_contracts = {}
    for addr, info in extended_contracts.items():
        all_contracts[addr] = info
    for addr, info in active_contracts.items():
        if addr in all_contracts:
            all_contracts[addr]["count"] += info["count"]
            all_contracts[addr]["last_block"] = max(all_contracts[addr]["last_block"], info["last_block"])
            all_contracts[addr]["first_block"] = min(all_contracts[addr]["first_block"], info["first_block"])
        else:
            all_contracts[addr] = info

    # Fetch metadata for contracts with enough mints
    qualified = {
        addr: info for addr, info in all_contracts.items()
        if info["count"] >= MIN_MINTS_ACTIVE
    }
    
    logger.info(f"[TEMPO] Found {len(active_logs)} active + {len(extended_logs)} extended logs across {len(qualified)} qualified contracts")
    
    collections = []
    for addr, info in sorted(qualified.items(), key=lambda x: -x[1]["count"]):
        meta = await _fetch_contract_metadata(session, addr)
        if not meta["name"]:
            continue
        
        # Filter out known non-NFT contracts and garbage names
        if not _is_valid_name(meta["name"]):
            continue
        
        # Determine status
        blocks_since_last = latest_block - info["last_block"]
        is_active = addr in active_contracts and active_contracts[addr]["count"] >= MIN_MINTS_ACTIVE
        
        collections.append({
            "address": addr,
            "name": meta["name"],
            "symbol": meta["symbol"],
            "total_supply": meta["total_supply"],
            "mint_count": info["count"],
            "active_mints": active_contracts.get(addr, {}).get("count", 0),
            "first_block": info["first_block"],
            "last_block": info["last_block"],
            "blocks_since_last": blocks_since_last,
            "is_active": is_active,
        })
        
        if len(collections) >= 25:
            break
        await asyncio.sleep(0.05)

    # Deduplicate by name (keep the one with highest mint count)
    seen_names = {}
    for col in collections:
        name_key = col["name"].lower().strip()
        if name_key not in seen_names or col["mint_count"] > seen_names[name_key]["mint_count"]:
            seen_names[name_key] = col
    collections = list(seen_names.values())

    # Split into lanes
    minting_now = []
    recently_minted = []
    
    for col in collections:
        card = _build_tempo_card(col, latest_block)
        if col["is_active"]:
            minting_now.append(card)
        elif col["mint_count"] >= MIN_MINTS_RECENT:
            recently_minted.append(card)

    # Sort by activity
    minting_now.sort(key=lambda x: x.get("active_mints", 0), reverse=True)
    recently_minted.sort(key=lambda x: x.get("mint_count", 0), reverse=True)

    result = {
        "minting_now": minting_now,
        "recently_minted": recently_minted,
        "stats": {
            "total_mints": len(active_logs) + len(extended_logs),
            "unique_contracts": len(qualified),
            "latest_block": latest_block,
            "scan_time": round(time.time() - t_start, 1),
        }
    }

    _tempo_cache = result
    _tempo_cache_ts = time.time()
    
    logger.info(
        f"[TEMPO] Scan complete: {len(minting_now)} active, "
        f"{len(recently_minted)} recent in {result['stats']['scan_time']}s"
    )
    return result


async def _fetch_mint_logs(
    session: aiohttp.ClientSession, from_block: str, to_block: str
) -> List[Dict]:
    """Fetch ERC-721 Transfer events from null address (= mints)."""
    result = await _rpc_call(session, "eth_getLogs", [{
        "fromBlock": from_block,
        "toBlock": to_block,
        "topics": [TRANSFER_TOPIC, NULL_ADDR_TOPIC],
    }])
    return result.get("result", []) if result else []


def _group_by_contract(logs: List[Dict], latest_block: int) -> Dict:
    """Group mint logs by contract address."""
    contracts = {}
    for log in logs:
        addr = log.get("address", "").lower()
        block_num = int(log.get("blockNumber", "0x0"), 16)
        if addr not in contracts:
            contracts[addr] = {
                "count": 0,
                "first_block": block_num,
                "last_block": block_num,
            }
        contracts[addr]["count"] += 1
        contracts[addr]["first_block"] = min(contracts[addr]["first_block"], block_num)
        contracts[addr]["last_block"] = max(contracts[addr]["last_block"], block_num)
    return contracts


async def _fetch_contract_metadata(
    session: aiohttp.ClientSession, address: str
) -> Dict:
    """Fetch ERC-721 metadata via eth_call: name(), symbol(), totalSupply()."""
    name_hex, sym_hex, supply_hex = await asyncio.gather(
        _eth_call(session, address, NAME_SIG),
        _eth_call(session, address, SYMBOL_SIG),
        _eth_call(session, address, TOTAL_SUPPLY_SIG),
    )
    
    name = _decode_string(name_hex)
    symbol = _decode_string(sym_hex)
    supply = _decode_uint(supply_hex)
    
    # Filter out absurdly large supplies (LP tokens, ERC-20s mistaken as NFTs)
    if supply > 100_000_000:
        supply = 0  # Treat as unknown — likely not a real NFT
    
    return {
        "name": name,
        "symbol": symbol,
        "total_supply": supply,
    }


def _build_tempo_card(col: Dict, latest_block: int) -> Dict:
    """Build a standardized card for a Tempo NFT collection."""
    address = col["address"]
    
    # Link priority: Stablewhel launchpad > Explorer
    launchpad_link = f"{STABLEWHEL_LAUNCHPAD}/{address}"
    explorer_link = f"{TEMPO_EXPLORER}/address/{address}"
    
    # Estimate time from block difference (Tempo ~2s block time)
    blocks_ago = col.get("blocks_since_last", 0)
    seconds_ago = blocks_ago * 2  # approximate
    
    if seconds_ago < 60:
        last_activity = f"{seconds_ago}s ago"
    elif seconds_ago < 3600:
        last_activity = f"{seconds_ago // 60}m ago"
    elif seconds_ago < 86400:
        last_activity = f"{seconds_ago // 3600}h ago"
    else:
        last_activity = f"{seconds_ago // 86400}d ago"

    status = "🟢 MINTING" if col["is_active"] else "⚪ Recent"

    return {
        # Identity
        "name": col["name"],
        "symbol": col["symbol"],
        "slug": f"tempo-{col['symbol'].lower() or address[:8]}",
        "contract": address,
        "chain": "tempo",
        "chain_label": "TEMPO",
        "image": "",  # Tempo explorer doesn't provide images via RPC
        
        # Mint metrics
        "total_supply": col["total_supply"],
        "mint_count": col["mint_count"],
        "active_mints": col.get("active_mints", 0),
        "is_minting": col["is_active"],
        "last_activity": last_activity,
        "status": status,
        
        # Links
        "primary_link": launchpad_link,
        "primary_link_label": "→ Mint Page",
        "explorer_url": explorer_link,
        "opensea_url": "",  # Not on OpenSea
        "launchpad_url": launchpad_link,
        "website": "",
        "twitter": "",
        
        # Scoring (basic — Tempo doesn't have price/volume data via RPC)
        "score": min(90, 30 + col.get("active_mints", 0) * 3),
        "score_label": "Active" if col["is_active"] else "Watch",
        "score_label_color": "#22c55e" if col["is_active"] else "#6b7280",
        "risk": "Medium",
        "verdict": "Active" if col["is_active"] else "Watch",
        "verdict_color": "#22c55e" if col["is_active"] else "#6b7280",
        "trend": "up" if col.get("active_mints", 0) > 5 else "stable",
        "reasons": [
            f"🟢 {col.get('active_mints', 0)} active mints detected on-chain" if col["is_active"] else "",
            f"📊 {col['total_supply']} total supply" if col["total_supply"] else "",
            f"⛓️ Tempo Network (Chain {TEMPO_CHAIN_ID})",
        ],
        
        # Backward compat fields for the frontend
        "lane": "minting_now" if col["is_active"] else "fresh_discovery",
        "floor_price": 0,
        "floor_symbol": "USD",
        "volume_1d": 0,
        "volume_change_1d": 0,
        "sales_1d": col.get("active_mints", 0),
        "num_owners": 0,
        "market_cap": 0,
        "safelist": "",
        "category_os": "tempo-nft",
        "data_freshness": datetime.now(timezone.utc).isoformat(),
        "mint_count_24h": col["mint_count"],
        "last_mint_ago": last_activity,
        "is_true_mint": True,
        "analysis": f"{col.get('active_mints', 0)} mints in recent blocks • {col['total_supply']} total supply",
        "scores": {
            "total": min(90, 30 + col.get("active_mints", 0) * 3),
            "volume": 0,
            "momentum": min(100, col.get("active_mints", 0) * 5),
            "mint_activity": min(100, col.get("active_mints", 0) * 10),
            "community": 0,
        },
        "breakdown": {},
        "source_items": [],
        "links": {
            "launchpad": launchpad_link,
            "explorer": explorer_link,
        },
        "originality": "original",
        "originality_reasons": [],
        "risk_flags": [],
        "is_blue_chip": False,
        "is_tempo": True,
    }
