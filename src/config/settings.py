"""
Insights-only settings stub.
Contains ONLY the NETWORKS definition needed by the discovery pipeline. 
NO wallets, NO private keys, NO sensitive data.
"""
import os, json, logging
from pathlib import Path

logger = logging.getLogger("mintbot.config")

NETWORKS = {
    "ETH": {"rpc": ["https://eth.llamarpc.com","https://rpc.ankr.com/eth","https://ethereum-rpc.publicnode.com"], "id": 1, "symbol": "ETH"},
    "BASE": {"rpc": ["https://mainnet.base.org","https://base.llamarpc.com","https://base-rpc.publicnode.com"], "id": 8453, "symbol": "ETH"},
    "OP": {"rpc": ["https://mainnet.optimism.io","https://optimism.llamarpc.com"], "id": 10, "symbol": "ETH"},
    "ARB": {"rpc": ["https://arb1.arbitrum.io/rpc","https://arbitrum.llamarpc.com"], "id": 42161, "symbol": "ETH"},
    "POLY": {"rpc": ["https://polygon-rpc.com","https://polygon.llamarpc.com"], "id": 137, "symbol": "MATIC"},
    "APECHAIN": {"rpc": ["https://rpc.apechain.com"], "id": 33139, "symbol": "APE"},
    "ZORA": {"rpc": ["https://rpc.zora.energy"], "id": 7777777, "symbol": "ETH"},
    "BERA": {"rpc": ["https://rpc.berachain.com"], "id": 80094, "symbol": "BERA"},
    "AVAX": {"rpc": ["https://api.avax.network/ext/bc/C/rpc"], "id": 43114, "symbol": "AVAX"},
    "BSC": {"rpc": ["https://bsc-dataseed1.binance.org"], "id": 56, "symbol": "BNB"},
    "TEMPO": {"rpc": ["https://rpc.tempo.xyz"], "id": 4217, "symbol": "TEMPO"},
}

CONFIG_PATH = Path(__file__).parent.parent.parent / "config.json"

def load_config() -> dict:
    """Load config from env vars only (no config.json in deployment)."""
    return {
        "opensea_api_key": os.environ.get("OPENSEA_API_KEY", ""),
        "opensea_mcp_token": os.environ.get("OPENSEA_MCP_TOKEN", ""),
        "drpc_token": os.environ.get("DRPC_TOKEN", ""),
    }

def save_config(cfg): pass
def get_safe_config(cfg): return {}
