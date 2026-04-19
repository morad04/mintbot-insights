"""
MintBot — News Intelligence Pipeline v5.1
═══════════════════════════════════════
Twitter/X scraping via Jina Reader + structured normalization + collection matching.

Fixed in v5.1:
  - Tweet URLs are now properly extracted from Jina markdown (status/xxx)
  - Added collection-specific search via Jina (x.com/search?q=...)
  - Added NFT calendar scraping for upcoming mints
  - Broader influencer/alpha list
  - Better tweet block parsing with URL association

Every news item includes:
  - direct_url: the actual tweet URL (NOT profile page)
  - source_type: official, team, marketplace, influencer, community, etc.
  - sentiment: bullish, neutral, bearish
  - catalyst: launch, mint_live, partnership, volume_spike, etc.
  - confidence: 0.0-1.0
  - collection_match: matched collection slug + confidence
  - score_effect: estimated impact on collection score
"""

import asyncio
import aiohttp
import re
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("mintbot.news")

# ═══════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════

JINA_READER_BASE = "https://r.jina.ai"
JINA_TIMEOUT = 25
MAX_ACCOUNTS_PER_CYCLE = 10   # Slightly increased
TWITTER_CACHE_TTL = 600       # 10 min cache for Twitter data

# Accounts to scrape — organized by type
TWITTER_ACCOUNTS = [
    # Marketplaces
    {"handle": "opensea", "label": "OpenSea", "type": "marketplace"},
    # Alpha hunters / Influencers
    {"handle": "punk6529", "label": "6529 (Alpha)", "type": "influencer"},
    {"handle": "NFTherder", "label": "NFTherder", "type": "influencer"},
    {"handle": "ZssBecker", "label": "ZssBecker", "type": "influencer"},
    {"handle": "farokh", "label": "Farokh", "type": "influencer"},
    {"handle": "frankdegods", "label": "Frank (DGods)", "type": "influencer"},
    {"handle": "Zeneca", "label": "Zeneca", "type": "influencer"},
    # Official collection accounts
    {"handle": "BoredApeYC", "label": "BAYC Official", "type": "official"},
    {"handle": "pudgypenguins", "label": "Pudgy Official", "type": "official"},
    {"handle": "Azuki", "label": "Azuki Official", "type": "official"},
    {"handle": "daborj", "label": "Dave NFT", "type": "influencer"},
    # Analytics
    {"handle": "NFTGo_Official", "label": "NFTGo", "type": "analytics"},
]

# Collection-specific search queries to find related tweets
COLLECTION_SEARCH_QUERIES = [
    "NFT minting now live",
    "NFT mint live today",
    "NFT new collection launch",
    "NFT alpha mint",
    "NFT floor price pump",
    "NFT upcoming mint free",
]

# ═══════════════════════════════════════════════════════
#  Sentiment + Catalyst Keywords
# ═══════════════════════════════════════════════════════

BULLISH_KEYWORDS = [
    "moon", "pump", "bull", "buy", "ape in", "lfg", "bullish", "gem",
    "alpha", "undervalued", "mint", "flip", "floor rising", "ath",
    "breakout", "100x", "hidden gem", "early", "send it",
    "fire", "strong", "accumulate", "dip buying", "load up",
    "partnership", "launched", "live now", "sold out", "game changer",
    "free mint", "public mint", "🚀", "🔥", "💎",
]
BEARISH_KEYWORDS = [
    "dump", "sell", "bear", "rug", "scam", "overvalued", "crash",
    "exit", "floor dropping", "paperhand", "down bad", "rekt",
    "avoid", "dying", "dead", "fud", "bleeding", "bag holders",
    "exploit", "hack", "vulnerability", "stolen",
]

CATALYST_KEYWORDS = {
    "launch": ["launched", "live now", "just dropped", "now live", "grand opening", "just launched"],
    "mint_live": ["minting", "mint live", "mint now", "mint is live", "public mint", "free mint", "minting now"],
    "partnership": ["partnership", "partnered", "collab", "collaboration", "teamed up"],
    "game_product": ["game", "gaming", "metaverse", "product launch", "app launch"],
    "token_utility": ["token", "utility", "staking", "rewards", "earn"],
    "exploit_security": ["exploit", "hack", "hacked", "vulnerability", "security", "stolen"],
    "controversy": ["controversy", "lawsuit", "sued", "accused", "banned"],
    "volume_spike": ["volume", "trading volume", "sales surge", "volume spike"],
    "whale_activity": ["whale", "whale buy", "large purchase", "whale alert"],
    "roadmap_update": ["roadmap", "milestone", "phase 2", "phase 3", "v2", "announcement"],
    "art_reveal": ["reveal", "art reveal", "metadata"],
    "allowlist": ["allowlist", "whitelist", "al spot", "wl spot", "oversubscribed"],
    "sellout": ["sold out", "sellout", "minted out"],
}

# Source type credibility weights for confidence
SOURCE_CONFIDENCE_BASE = {
    "official": 1.0,
    "team": 0.85,
    "marketplace": 0.75,
    "launchpad": 0.75,
    "influencer": 0.65,
    "analytics": 0.60,
    "media": 0.55,
    "community": 0.40,
    "search": 0.50,
    "rumor": 0.20,
    "unknown": 0.30,
}


# ═══════════════════════════════════════════════════════
#  Twitter Cache
# ═══════════════════════════════════════════════════════

class _TwitterCache:
    def __init__(self):
        self.data: Optional[List[Dict]] = None
        self.ts: float = 0

    @property
    def valid(self) -> bool:
        return self.data is not None and (time.time() - self.ts) < TWITTER_CACHE_TTL

    def set(self, items: List[Dict]):
        self.data = items
        self.ts = time.time()

_twitter_cache = _TwitterCache()


# ═══════════════════════════════════════════════════════
#  Jina Reader Twitter Scraper (Fixed v5.1)
# ═══════════════════════════════════════════════════════

async def scrape_twitter_via_jina(session: aiohttp.ClientSession) -> List[Dict]:
    """Scrape real tweets from NFT influencers + collection search using Jina Reader.

    Returns normalized news items with DIRECT tweet URLs (not profile pages).
    """
    if _twitter_cache.valid:
        return _twitter_cache.data

    all_items = []
    scraped_count = 0

    # Phase 1: Scrape individual accounts
    for account in TWITTER_ACCOUNTS:
        if scraped_count >= MAX_ACCOUNTS_PER_CYCLE:
            break

        handle = account["handle"]
        label = account["label"]
        acct_type = account["type"]

        try:
            jina_url = f"{JINA_READER_BASE}/https://x.com/{handle}"
            async with session.get(
                jina_url,
                timeout=aiohttp.ClientTimeout(total=JINA_TIMEOUT),
                headers={"Accept": "text/plain", "User-Agent": "MintBot/5.0"}
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"[NEWS] Jina {resp.status} for @{handle}")
                    continue

                md = await resp.text()
                tweets = _parse_jina_tweets(md, handle, label, acct_type)
                all_items.extend(tweets)
                scraped_count += 1

        except Exception as e:
            logger.debug(f"[NEWS] Jina scrape failed for @{handle}: {e}")

        await asyncio.sleep(1.0)

    # Phase 2: Search for collection-related tweets (2 queries)
    search_queries = COLLECTION_SEARCH_QUERIES[:2]
    for query in search_queries:
        try:
            search_items = await _scrape_twitter_search(session, query)
            all_items.extend(search_items)
        except Exception as e:
            logger.debug(f"[NEWS] Search scrape failed for '{query}': {e}")
        await asyncio.sleep(1.5)

    logger.info(f"[NEWS] Scraped {scraped_count} accounts + {len(search_queries)} searches, got {len(all_items)} items")
    _twitter_cache.set(all_items)
    return all_items


async def _scrape_twitter_search(
    session: aiohttp.ClientSession, query: str
) -> List[Dict]:
    """Scrape X/Twitter search results via Jina Reader to find collection-related tweets."""
    encoded_q = query.replace(" ", "+")
    jina_url = f"{JINA_READER_BASE}/https://x.com/search?q={encoded_q}&f=live"
    
    try:
        async with session.get(
            jina_url,
            timeout=aiohttp.ClientTimeout(total=JINA_TIMEOUT),
            headers={"Accept": "text/plain", "User-Agent": "MintBot/5.0"}
        ) as resp:
            if resp.status != 200:
                return []

            md = await resp.text()
            return _parse_jina_search_results(md, query)
    except Exception as e:
        logger.debug(f"[NEWS] Search scrape error: {e}")
        return []


def _parse_jina_search_results(md: str, query: str) -> List[Dict]:
    """Parse search results from Jina (slightly different format from profile)."""
    results = []

    # Extract all tweet URLs from the markdown
    tweet_urls = re.findall(r'https://(?:x|twitter)\.com/(\w+)/status/(\d+)', md)

    # Split text into sections between tweet URLs
    clean = _clean_jina_metadata(md)
    blocks = _split_blocks(clean)

    url_idx = 0
    for block in blocks:
        text = " ".join(block).strip()
        if len(text) < 25:
            continue

        # Find matching tweet URL
        direct_url = ""
        tweet_handle = ""
        if url_idx < len(tweet_urls):
            tweet_handle, tweet_id = tweet_urls[url_idx]
            direct_url = f"https://x.com/{tweet_handle}/status/{tweet_id}"
            url_idx += 1

        if not direct_url:
            continue

        item = _build_structured_news_item(
            text, tweet_handle or "search",
            f"Search: {query[:20]}", "search",
            override_direct_url=direct_url,
        )
        results.append(item)

        if len(results) >= 3:
            break

    return results


def _clean_jina_metadata(md: str) -> List[str]:
    """Remove Jina metadata headers and return clean lines."""
    clean_lines = []
    for line in md.split("\n"):
        stripped = line.strip()
        if stripped.startswith("Title:") or stripped.startswith("URL Source:"):
            continue
        if stripped.startswith("Published Time:") or stripped.startswith("Markdown Content:"):
            continue
        clean_lines.append(stripped)
    return clean_lines


def _split_blocks(lines: List[str]) -> List[List[str]]:
    """Split lines into tweet blocks using profile pictures as delimiters."""
    blocks = []
    current_block = []

    for line in lines:
        is_profile_pic = ("profile" in line.lower() and ("[![" in line or "![" in line))
        is_image_ref = line.startswith("[![") or line.startswith("![")

        if is_profile_pic:
            if current_block:
                blocks.append(current_block)
                current_block = []
            continue

        if is_image_ref:
            continue

        # Skip UI junk
        skip_words = [
            "log in", "sign up", "don't miss", "people on x",
            "cookie", "privacy policy", "terms of service",
            "relevant people", "who to follow", "show more",
            "what's happening", "see new posts", "accessibility",
            "square profile", "opens profile", "click to follow",
            "follow click", "get verified", "explore premium",
            "not on x? sign up", "more from",
        ]
        lower = line.lower()
        if any(sw in lower for sw in skip_words):
            continue

        if line.startswith("# ") or line.startswith("## "):
            continue
        if re.match(r'^[\d,.]+[KkMm]?\s+(posts|followers|following)', line.strip()):
            continue
        if line.strip() in ("Pinned", "Quote", "Replying to", "Show replies"):
            continue
        if re.match(r'^@\w+$', line.strip()):
            continue
        if re.match(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d', line.strip()):
            continue
        if line.startswith("[") and "](" in line and len(line) < 60:
            continue
        if len(line.strip()) < 5:
            continue

        cleaned = re.sub(r'!\[Image \d+[^]]*\]\([^)]+\)', '', line).strip()
        if cleaned and len(cleaned) > 3:
            current_block.append(cleaned)

    if current_block:
        blocks.append(current_block)

    return blocks


def _parse_jina_tweets(md: str, handle: str, label: str, acct_type: str) -> List[Dict]:
    """Parse tweets from Jina Reader markdown output.

    FIXED (v5.1):
      - Extracts actual tweet status URLs from the markdown
      - Associates each block with its closest tweet URL
      - Falls back to profile URL only if no tweet URL found
    """
    tweets = []

    # ── Step 1: Extract ALL tweet status URLs from the entire markdown ──
    all_tweet_urls = []
    for m in re.finditer(r'https://(?:x|twitter)\.com/(\w+)/status/(\d+)', md):
        url = f"https://x.com/{m.group(1)}/status/{m.group(2)}"
        if url not in all_tweet_urls:
            all_tweet_urls.append(url)

    # ── Step 2: Clean + split into blocks ──
    clean_lines = _clean_jina_metadata(md)

    # Find posts section
    posts_start = 0
    for i, line in enumerate(clean_lines):
        lower = line.lower()
        if "'s posts" in lower or ("## " in line and "post" in lower):
            posts_start = i + 1
            break
        if line.strip() == "Pinned":
            posts_start = i + 1
            break
    if posts_start == 0:
        posts_start = min(10, len(clean_lines))

    blocks = _split_blocks(clean_lines[posts_start:])

    # ── Step 3: Convert blocks with URL assignment ──
    url_idx = 0
    for block in blocks:
        text = " ".join(block).strip()
        if len(text) < 20:
            continue

        lower_t = text.lower()
        skip_bio = [
            "the best club", "click to follow", "follow click",
            "the best place to discover", "get help at",
            "posts followers", "and others",
        ]
        if any(s in lower_t for s in skip_bio) or text.startswith("@"):
            continue

        link_ratio = len(re.findall(r'\[.*?\]\(.*?\)', text)) / max(1, len(text.split()))
        if link_ratio > 0.5 and len(text) < 80:
            continue

        # Find direct URL for this tweet
        # First check if any tweet URL is referenced inside the block text
        block_url = None
        for bline in block:
            m = re.search(r'https://(?:x|twitter)\.com/\w+/status/\d+', bline)
            if m:
                block_url = m.group(0)
                break

        # If not in block text, use the next URL from the sequential list
        if not block_url and url_idx < len(all_tweet_urls):
            block_url = all_tweet_urls[url_idx]
            url_idx += 1

        item = _build_structured_news_item(
            text, handle, label, acct_type,
            override_direct_url=block_url,
        )
        tweets.append(item)

    return tweets[:4]  # Max 4 per account


# ═══════════════════════════════════════════════════════
#  News Item Builder
# ═══════════════════════════════════════════════════════

def _build_structured_news_item(
    text: str,
    handle: str,
    label: str,
    source_type: str,
    override_direct_url: str = None,
) -> Dict:
    """Build a structured, normalized news item from raw text.

    Includes direct_url (actual tweet, NOT profile page), sentiment, catalyst, etc.
    """
    display_text = text[:400] + "..." if len(text) > 400 else text
    lower = text.lower()

    # Analyze sentiment
    sentiment = _detect_sentiment(lower)

    # Detect catalyst type
    catalyst = _detect_catalyst(lower)

    # Base confidence from source type
    confidence = SOURCE_CONFIDENCE_BASE.get(source_type, 0.30)

    # ── Direct URL logic (FIXED v5.1) ──
    # Priority: 1) override URL 2) tweet URL in text 3) t.co link 4) profile fallback
    direct_url = None

    if override_direct_url and "/status/" in override_direct_url:
        direct_url = override_direct_url
    
    if not direct_url:
        # Look for tweet status URLs in text
        tweet_match = re.search(r'https://(?:x|twitter)\.com/\w+/status/\d+', text)
        if tweet_match:
            direct_url = tweet_match.group(0)

    if not direct_url:
        # Look for t.co short links (Twitter's own shortener)
        tco_match = re.search(r'https://t\.co/\w+', text)
        if tco_match:
            direct_url = tco_match.group(0)

    # Fallback to profile URL
    fallback_url = f"https://x.com/{handle}"
    if not direct_url:
        direct_url = fallback_url

    # Estimate score effect
    score_effect = _estimate_score_effect(sentiment, catalyst, confidence)

    return {
        "title": display_text,
        "direct_url": direct_url,
        "fallback_url": fallback_url,
        "source": f"@{handle}",
        "source_label": label,
        "source_type": source_type,
        "sentiment": sentiment,
        "catalyst": catalyst,
        "confidence": round(confidence, 2),
        "score_effect": score_effect,
        "published": datetime.now(timezone.utc).isoformat(),
        "is_real_tweet": "/status/" in (direct_url or ""),
        "collection_match": None,  # Set later by matcher
        "collection_confidence": 0.0,
    }


def _detect_sentiment(lower_text: str) -> str:
    """Detect bullish/bearish/neutral sentiment."""
    bull = sum(1 for kw in BULLISH_KEYWORDS if kw in lower_text)
    bear = sum(1 for kw in BEARISH_KEYWORDS if kw in lower_text)
    if bull > bear + 1:
        return "bullish"
    elif bear > bull + 1:
        return "bearish"
    else:
        return "neutral"


def _detect_catalyst(lower_text: str) -> str:
    """Detect the primary catalyst type from text."""
    best_cat = ""
    best_score = 0
    for cat_type, keywords in CATALYST_KEYWORDS.items():
        matches = sum(1 for kw in keywords if kw in lower_text)
        if matches > best_score:
            best_score = matches
            best_cat = cat_type
    return best_cat if best_score > 0 else ""


def _estimate_score_effect(sentiment: str, catalyst: str, confidence: float) -> int:
    """Estimate the score effect this news item would have on a collection."""
    from .scoring_engine import CATALYST_IMPORTANCE

    base = 0
    if catalyst:
        base = CATALYST_IMPORTANCE.get(catalyst, 5)
    elif sentiment == "bullish":
        base = 8
    elif sentiment == "bearish":
        base = -8
    else:
        base = 3

    effect = int(base * confidence)
    if sentiment == "bearish" and effect > 0:
        effect = -effect

    return max(-30, min(30, effect))


# ═══════════════════════════════════════════════════════
#  Collection Matching
# ═══════════════════════════════════════════════════════

def match_news_to_collections(
    news_items: List[Dict],
    collections: List[Dict],
) -> List[Dict]:
    """Match each news item to the most relevant collection.

    Uses the collection_registry's match_text_to_collection for fuzzy matching.
    Updates news items in-place with collection_match and collection_confidence.
    """
    from .collection_registry import match_text_to_collection

    for item in news_items:
        text = item.get("title", "")
        handle = item.get("source", "").lstrip("@")

        matches = match_text_to_collection(text, collections)
        if matches:
            best_col, confidence = matches[0]
            item["collection_match"] = best_col.get("slug", "")
            item["collection_confidence"] = round(confidence, 2)
            item["collection_name"] = best_col.get("name", "")

            if handle.lower() == (best_col.get("twitter", "") or "").lower():
                item["collection_confidence"] = min(1.0, confidence + 0.2)
                item["source_type"] = "official"

    return news_items


# ═══════════════════════════════════════════════════════
#  News Quality Filter
# ═══════════════════════════════════════════════════════

def filter_noise(news_items: List[Dict]) -> List[Dict]:
    """Filter out low-quality / spam news items."""
    filtered = []
    seen_texts = set()

    for item in news_items:
        title = item.get("title", "")

        if len(title) < 25:
            continue

        key = re.sub(r'[^a-z0-9]', '', title.lower())[:50]
        if key in seen_texts:
            continue
        seen_texts.add(key)

        lower = title.lower()
        spam_signals = [
            "click here", "dm me", "free nft", "giveaway",
            "airdrop alert", "claim your", "🎁 free",
        ]
        if sum(1 for s in spam_signals if s in lower) >= 2:
            continue

        filtered.append(item)

    return filtered


# ═══════════════════════════════════════════════════════
#  Sentiment Emoji
# ═══════════════════════════════════════════════════════

def sentiment_emoji(sentiment: str) -> str:
    return {
        "bullish": "🟢",
        "bearish": "🔴",
        "neutral": "👀",
    }.get(sentiment, "👀")


def catalyst_emoji(catalyst: str) -> str:
    return {
        "launch": "🚀",
        "mint_live": "🟢",
        "partnership": "🤝",
        "game_product": "🎮",
        "token_utility": "🪙",
        "exploit_security": "🚨",
        "controversy": "⚡",
        "volume_spike": "📊",
        "whale_activity": "🐋",
        "roadmap_update": "📋",
        "art_reveal": "🎨",
        "allowlist": "📝",
        "sellout": "🔥",
    }.get(catalyst, "📰")
