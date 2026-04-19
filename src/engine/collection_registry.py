"""
MintBot — Collection Registry
═══════════════════════════════════════
Identity management, classification, and alias matching for NFT collections.

Responsibilities:
  - Blue-chip registry with aliases, handles, contracts
  - Collection classification (blue_chip, minting_now, new_launch, etc.)
  - News-to-collection matching via fuzzy name/handle/contract
  - Originality/derivative detection
"""

import re
import logging
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger("mintbot.registry")

# ═══════════════════════════════════════════════════════
#  Blue-Chip Registry
# ═══════════════════════════════════════════════════════
# Each entry defines the canonical identity of a blue-chip collection.
# Fields:
#   slug          - OpenSea collection slug (primary key)
#   name          - Display name
#   alt_names     - Alternate names/abbreviations for matching
#   chain         - Primary chain
#   x_handles     - Official X/Twitter handles
#   founder_handles - Known founder/team handles
#   brand_terms   - Words strongly associated with the project
#   contract      - Primary contract address (if known)
#   website       - Official website

BLUE_CHIP_REGISTRY: List[Dict] = [
    {
        "slug": "boredapeyachtclub",
        "name": "Bored Ape Yacht Club",
        "alt_names": ["BAYC", "Bored Apes", "Bored Ape"],
        "chain": "ethereum",
        "x_handles": ["BoredApeYC"],
        "founder_handles": ["GordonGoner", "No_Sass", "CryptoGarga"],
        "brand_terms": ["bayc", "bored ape", "yacht club", "ape fest", "apefest"],
        "contract": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
        "website": "https://boredapeyachtclub.com",
    },
    {
        "slug": "cryptopunks",
        "name": "CryptoPunks",
        "alt_names": ["Punks", "CryptoPunk"],
        "chain": "ethereum",
        "x_handles": ["cryptopaborj"],
        "founder_handles": ["laraborj", "matt_hall"],
        "brand_terms": ["cryptopunk", "punk", "larva labs"],
        "contract": "0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB",
        "website": "https://www.larvalabs.com/cryptopunks",
    },
    {
        "slug": "mutant-ape-yacht-club",
        "name": "Mutant Ape Yacht Club",
        "alt_names": ["MAYC", "Mutant Apes", "Mutant Ape"],
        "chain": "ethereum",
        "x_handles": ["BoredApeYC"],
        "founder_handles": [],
        "brand_terms": ["mayc", "mutant ape", "mutant"],
        "contract": "0x60E4d786628Fea6478F785A6d7e704777c86a7c6",
        "website": "https://boredapeyachtclub.com",
    },
    {
        "slug": "azuki",
        "name": "Azuki",
        "alt_names": ["Azuki NFT"],
        "chain": "ethereum",
        "x_handles": ["Azuki"],
        "founder_handles": ["ZAGABOND"],
        "brand_terms": ["azuki", "the garden", "beanz", "elementals"],
        "contract": "0xED5AF388653567Af2F388E6224dC7C4b3241C544",
        "website": "https://www.azuki.com",
    },
    {
        "slug": "pudgypenguins",
        "name": "Pudgy Penguins",
        "alt_names": ["Pudgy", "Pudgys"],
        "chain": "ethereum",
        "x_handles": ["pudgypenguins"],
        "founder_handles": ["LucaNetz"],
        "brand_terms": ["pudgy", "penguin", "lil pudgy", "pudgy world"],
        "contract": "0xBd3531dA5CF5857e7CfAA92426877b022e612cf8",
        "website": "https://www.pudgypenguins.com",
    },
    {
        "slug": "doodles-official",
        "name": "Doodles",
        "alt_names": ["Doodles NFT"],
        "chain": "ethereum",
        "x_handles": ["daborj"],
        "founder_handles": ["poopie", "evankeast", "tulip"],
        "brand_terms": ["doodles", "doodlebank"],
        "contract": "0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e",
        "website": "https://doodles.app",
    },
    {
        "slug": "clonex",
        "name": "CloneX",
        "alt_names": ["Clone X", "RTFKT CloneX"],
        "chain": "ethereum",
        "x_handles": ["ABORJFKT"],
        "founder_handles": ["ClegFx"],
        "brand_terms": ["clonex", "clone x", "rtfkt"],
        "contract": "0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B",
        "website": "https://rtfkt.com",
    },
    {
        "slug": "moonbirds",
        "name": "Moonbirds",
        "alt_names": ["Moonbird"],
        "chain": "ethereum",
        "x_handles": ["moonaborjs"],
        "founder_handles": ["kevinrose"],
        "brand_terms": ["moonbirds", "proof", "grails"],
        "contract": "0x23581767a106ae21c074b2276D25e5C3e136a68b",
        "website": "https://www.proof.xyz",
    },
    {
        "slug": "lilpudgys",
        "name": "Lil Pudgys",
        "alt_names": ["Lil Pudgy", "Lil Pudgys"],
        "chain": "ethereum",
        "x_handles": ["pudgypenguins"],
        "founder_handles": ["LucaNetz"],
        "brand_terms": ["lil pudgy", "lil pudgys"],
        "contract": "0x524cAB2ec69124574082676e6F654a18df49A048",
        "website": "https://www.pudgypenguins.com",
    },
    {
        "slug": "milady",
        "name": "Milady Maker",
        "alt_names": ["Milady", "Miladys"],
        "chain": "ethereum",
        "x_handles": ["miaborjmaker"],
        "founder_handles": [],
        "brand_terms": ["milady", "remilia"],
        "contract": "0x5Af0D9827E0c53E4799BB226655A1de152A425a5",
        "website": "https://miladymaker.net",
    },
    {
        "slug": "degods",
        "name": "DeGods",
        "alt_names": ["DeGod"],
        "chain": "ethereum",
        "x_handles": ["DeGaborjNFT"],
        "founder_handles": ["FrankDeGods"],
        "brand_terms": ["degods", "degod", "dust labs"],
        "contract": "0x8821BeE2ba0dF28761AffF119D66390D594CD280",
        "website": "https://degods.com",
    },
    {
        "slug": "meebits",
        "name": "Meebits",
        "alt_names": ["Meebit"],
        "chain": "ethereum",
        "x_handles": [],
        "founder_handles": [],
        "brand_terms": ["meebits", "meebit", "larva labs"],
        "contract": "0x7Bd29408f11D2bFC23c34f18275bBafE693350aA",
        "website": "https://meebits.app",
    },
    {
        "slug": "cool-cats-nft",
        "name": "Cool Cats",
        "alt_names": ["Cool Cat"],
        "chain": "ethereum",
        "x_handles": ["coolcats"],
        "founder_handles": [],
        "brand_terms": ["cool cats", "cool cat"],
        "contract": "0x1A92f7381B9F03921564a437210bB9396471050C",
        "website": "https://www.coolcatsnft.com",
    },
    {
        "slug": "world-of-women-nft",
        "name": "World of Women",
        "alt_names": ["WoW", "WoW NFT"],
        "chain": "ethereum",
        "x_handles": ["worldofwomennft"],
        "founder_handles": [],
        "brand_terms": ["world of women", "wow nft"],
        "contract": "0xe785E82358879F061BC3dcAC6f0444462D4b5330",
        "website": "https://worldofwomen.art",
    },
    {
        "slug": "beanz-official",
        "name": "BEANZ",
        "alt_names": ["Beanz", "Azuki Beanz"],
        "chain": "ethereum",
        "x_handles": ["Azuki"],
        "founder_handles": ["ZAGABOND"],
        "brand_terms": ["beanz", "bean"],
        "contract": "0x306b1ea3ecdf94aB739F1910bbda052Ed4A9f949",
        "website": "https://www.azuki.com/beanz",
    },
    # ── Expanded Blue Chips (added for coverage) ──
    {
        "slug": "otherdeed",
        "name": "Otherdeed for Otherside",
        "alt_names": ["Otherdeed", "Otherside"],
        "chain": "ethereum",
        "x_handles": ["OthsideMetaj"],
        "founder_handles": [],
        "brand_terms": ["otherdeed", "otherside", "otherdeeds"],
        "contract": "0x34d85c9CDeB23FA97cb08333b511ac86E1C4E258",
    },
    {
        "slug": "art-blocks-curated",
        "name": "Art Blocks Curated",
        "alt_names": ["Art Blocks", "ArtBlocks"],
        "chain": "ethereum",
        "x_handles": ["artaborjs_io"],
        "founder_handles": ["erickcalderon"],
        "brand_terms": ["art blocks", "artblocks", "curated"],
        "contract": "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270",
    },
    {
        "slug": "fidenza-by-tyler-hobbs",
        "name": "Fidenza",
        "alt_names": ["Fidenza by Tyler Hobbs"],
        "chain": "ethereum",
        "x_handles": ["tylerxhobbs"],
        "founder_handles": ["tylerxhobbs"],
        "brand_terms": ["fidenza"],
        "contract": "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270",
    },
    {
        "slug": "chromie-squiggle-by-snowfro",
        "name": "Chromie Squiggle",
        "alt_names": ["Squiggle", "Squiggles"],
        "chain": "ethereum",
        "x_handles": ["artaborjs_io"],
        "founder_handles": ["ArtOnBlockchn"],
        "brand_terms": ["chromie", "squiggle"],
        "contract": "0x059EDD72Cd353dF5106D2B9cC5ab83a52287aC3a",
    },
    # NOTE: "pengu" slug removed — maps to junk "Penguin - JFQEIe6vjY" (1 owner)
    # on OpenSea, NOT the real PENGU token. Do not re-add without verifying slug.

    {
        "slug": "checks-vv-edition",
        "name": "Checks - VV Edition",
        "alt_names": ["Checks", "VV Checks"],
        "chain": "ethereum",
        "x_handles": ["jackaborj"],
        "founder_handles": ["jalil_eth"],
        "brand_terms": ["checks", "vv edition"],
        "contract": "0x036721e5A769Cc48B3189EFbb9ccE4471E8A48B1",
    },
    {
        "slug": "wrapped-cryptopunks",
        "name": "Wrapped CryptoPunks",
        "alt_names": ["WPUNKS"],
        "chain": "ethereum",
        "x_handles": [],
        "founder_handles": [],
        "brand_terms": ["wrapped punk"],
        "contract": "0xb7F7F6C52F2e2fdb1963Eab30438024864c313F6",
    },
    {
        "slug": "nakamigos",
        "name": "Nakamigos",
        "alt_names": [],
        "chain": "ethereum",
        "x_handles": ["Nakamigos"],
        "founder_handles": [],
        "brand_terms": ["nakamigos"],
        "contract": "0xd774557b647330C91Bf44cfEAB205095f7E6c367",
    },
    {
        "slug": "captainz",
        "name": "Captainz",
        "alt_names": ["The Captainz"],
        "chain": "ethereum",
        "x_handles": ["Memaborj"],
        "founder_handles": ["0xkowl"],
        "brand_terms": ["captainz", "memeland"],
        "contract": "0x769272677faB02575E84945F03Eca517ACc544Cc",
    },
    {
        "slug": "the-potatoz",
        "name": "The Potatoz",
        "alt_names": ["Potatoz"],
        "chain": "ethereum",
        "x_handles": ["Memaborj"],
        "founder_handles": [],
        "brand_terms": ["potatoz"],
        "contract": "0x39ee2c7b3cb80254225884ca001F57118C8f21B6",
    },
    {
        "slug": "kanpai-pandas",
        "name": "Kanpai Pandas",
        "alt_names": [],
        "chain": "ethereum",
        "x_handles": ["kanpaipandas"],
        "founder_handles": [],
        "brand_terms": ["kanpai"],
        "contract": "0xacF63E56Fd08970b43401492a02F6F38B6635C91",
    },
    {
        "slug": "opepen-edition",
        "name": "Opepen Edition",
        "alt_names": ["Opepen"],
        "chain": "ethereum",
        "x_handles": ["jackaborj"],
        "founder_handles": ["jalil_eth"],
        "brand_terms": ["opepen"],
        "contract": "0x6339e5E072086621540D0362C4e3Cea0d643E114",
    },
    {
        "slug": "sewer-pass",
        "name": "Sewer Pass",
        "alt_names": [],
        "chain": "ethereum",
        "x_handles": ["BoredApeYC"],
        "founder_handles": [],
        "brand_terms": ["sewer pass", "dookey dash"],
        "contract": "0xBA5BDe662c17e2aDFF1075610382B9B691296350",
    },
    {
        "slug": "the-memes-by-6529",
        "name": "The Memes by 6529",
        "alt_names": ["6529 Memes"],
        "chain": "ethereum",
        "x_handles": ["punk6529"],
        "founder_handles": ["punk6529"],
        "brand_terms": ["6529", "the memes"],
        "contract": "0x33FD426905F149f8376e227d0C9D3340AaD17aF1",
    },
    {
        "slug": "parallel-alpha",
        "name": "Parallel",
        "alt_names": ["Parallel Alpha", "Parallel TCG"],
        "chain": "ethereum",
        "x_handles": ["ParallelTCG"],
        "founder_handles": [],
        "brand_terms": ["parallel", "parallelfi"],
        "contract": "0x76BE3b62873462d2142405439777e971754E8E77",
    },
    {
        "slug": "azuki-elementals",
        "name": "Azuki Elementals",
        "alt_names": ["Elementals"],
        "chain": "ethereum",
        "x_handles": ["Azuki"],
        "founder_handles": ["ZAGABOND"],
        "brand_terms": ["elementals"],
        "contract": "0xB6a37b5d14D502c3Ab17A01c0F0c8Ea8E4Ae7815",
    },
    {
        "slug": "bored-ape-kennel-club",
        "name": "Bored Ape Kennel Club",
        "alt_names": ["BAKC", "Kennel Club"],
        "chain": "ethereum",
        "x_handles": ["BoredApeYC"],
        "founder_handles": [],
        "brand_terms": ["bakc", "kennel club"],
        "contract": "0xba30E5F9Bb24caa003E9f2f0497Ad287FDF95623",
    },
    {
        "slug": "invisible-friends",
        "name": "Invisible Friends",
        "alt_names": [],
        "chain": "ethereum",
        "x_handles": ["InvsbleFriends"],
        "founder_handles": ["MarkRise"],
        "brand_terms": ["invisible friends"],
        "contract": "0x59468516a8259058baD1cA5F8f4BFF6b7CD9d6F6",
    },
    {
        "slug": "sproto-gremlins",
        "name": "Sproto Gremlins",
        "alt_names": ["Sproto"],
        "chain": "ethereum",
        "x_handles": [],
        "founder_handles": [],
        "brand_terms": ["sproto", "gremlins"],
        "contract": "",
    },
    {
        "slug": "ens-ethereum-name-service",
        "name": "ENS: Ethereum Name Service",
        "alt_names": ["ENS", "ENS Domains"],
        "chain": "ethereum",
        "x_handles": ["ensdomains"],
        "founder_handles": [],
        "brand_terms": ["ens", "ethereum name service", ".eth"],
        "contract": "0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85",
    },
]

# Build lookup indexes at import time
_slug_index: Dict[str, Dict] = {}
_handle_index: Dict[str, Dict] = {}
_name_index: Dict[str, Dict] = {}
_contract_index: Dict[str, Dict] = {}

def _build_indexes():
    """Build fast lookup indexes from the registry."""
    global _slug_index, _handle_index, _name_index, _contract_index
    for bc in BLUE_CHIP_REGISTRY:
        slug = bc["slug"]
        _slug_index[slug] = bc
        # Index all handles
        for h in bc.get("x_handles", []):
            _handle_index[h.lower()] = bc
        for h in bc.get("founder_handles", []):
            _handle_index[h.lower()] = bc
        # Index all names
        _name_index[bc["name"].lower()] = bc
        for alt in bc.get("alt_names", []):
            _name_index[alt.lower()] = bc
        # Index contract
        contract = bc.get("contract", "")
        if contract:
            _contract_index[contract.lower()] = bc

_build_indexes()


# ═══════════════════════════════════════════════════════
#  Classification
# ═══════════════════════════════════════════════════════

# Collection classification labels
CLASS_BLUE_CHIP = "blue_chip"
CLASS_MINTING_NOW = "minting_now"
CLASS_NEW_LAUNCH = "new_launch"
CLASS_UPCOMING = "upcoming"
CLASS_UNDERVALUED = "undervalued_candidate"
CLASS_DERIVATIVE = "remake_or_derivative"
CLASS_HIGH_RISK = "high_risk"
CLASS_WATCH = "watch_only"


def is_blue_chip(slug: str = "", name: str = "", contract: str = "") -> bool:
    """Check if a collection is a known blue chip."""
    if slug and slug in _slug_index:
        return True
    if contract and contract.lower() in _contract_index:
        return True
    if name and name.lower() in _name_index:
        return True
    return False


def get_blue_chip_info(slug: str) -> Optional[Dict]:
    """Get blue-chip registry entry by slug."""
    return _slug_index.get(slug)


def get_all_blue_chip_slugs() -> List[str]:
    """Return all blue-chip slugs."""
    return [bc["slug"] for bc in BLUE_CHIP_REGISTRY]


def classify_collection(col: Dict) -> List[str]:
    """Classify a collection into one or more lanes.

    Args:
        col: Collection dict with _stats, _mint_info, safelist, etc.

    Returns:
        List of classification labels in priority order.
    """
    classes = []
    slug = col.get("slug", "")
    stats = col.get("_stats", {})
    mint_info = col.get("_mint_info", {})

    # Blue chip check
    bc = is_blue_chip(slug=slug, name=col.get("name", ""),
                      contract=col.get("contract", ""))
    if bc:
        classes.append(CLASS_BLUE_CHIP)

    # Active minting
    if mint_info.get("is_minting"):
        classes.append(CLASS_MINTING_NOW)

    # New launch detection (from "new" source or low total volume)
    if col.get("_source") == "new":
        classes.append(CLASS_NEW_LAUNCH)
    elif stats.get("total_volume", 0) < 10 and stats.get("total_sales", 0) < 100:
        classes.append(CLASS_NEW_LAUNCH)

    # Undervalued candidate
    owners = stats.get("num_owners", 0)
    vol_change = stats.get("volume_change_1d", 0)
    if owners > 50 and vol_change > 20 and CLASS_BLUE_CHIP not in classes:
        classes.append(CLASS_UNDERVALUED)

    # High risk detection
    if (owners < 20 and stats.get("volume_1d", 0) > 5 and
            col.get("safelist", "") != "verified"):
        classes.append(CLASS_HIGH_RISK)

    # Default: watch
    if not classes:
        classes.append(CLASS_WATCH)

    return classes


# ═══════════════════════════════════════════════════════
#  News-to-Collection Matching
# ═══════════════════════════════════════════════════════

def match_text_to_collection(
    text: str,
    known_collections: List[Dict],
) -> List[Tuple[Dict, float]]:
    """Match a text (tweet, article) to known collections.

    Returns list of (collection, confidence) tuples, sorted by confidence.

    Confidence levels:
        1.0  — official account tweet about own collection
        0.85 — founder/team handle match
        0.75 — direct marketplace/launchpad reference
        0.65 — influencer clearly naming collection
        0.40 — weak community mention
        0.20 — ambiguous/noisy mention
    """
    if not text:
        return []

    lower_text = text.lower()
    matches = []

    for col in known_collections:
        best_confidence = 0.0
        slug = col.get("slug", "")
        name = col.get("name", "")

        # Check blue-chip registry for richer matching data
        bc_info = _slug_index.get(slug)

        # 1. Check X handle mention (@handle)
        handles_to_check = []
        if col.get("twitter"):
            handles_to_check.append(col["twitter"])
        if bc_info:
            handles_to_check.extend(bc_info.get("x_handles", []))
            handles_to_check.extend(bc_info.get("founder_handles", []))

        for handle in handles_to_check:
            if f"@{handle.lower()}" in lower_text:
                if handle.lower() in [h.lower() for h in (bc_info or {}).get("x_handles", [])]:
                    best_confidence = max(best_confidence, 0.90)
                elif handle.lower() in [h.lower() for h in (bc_info or {}).get("founder_handles", [])]:
                    best_confidence = max(best_confidence, 0.85)
                else:
                    best_confidence = max(best_confidence, 0.65)

        # 2. Check exact name match
        if name and name.lower() in lower_text:
            best_confidence = max(best_confidence, 0.75)

        # 3. Check alt names and brand terms
        if bc_info:
            for alt in bc_info.get("alt_names", []):
                if alt.lower() in lower_text:
                    best_confidence = max(best_confidence, 0.70)
            for term in bc_info.get("brand_terms", []):
                if term.lower() in lower_text:
                    best_confidence = max(best_confidence, 0.55)

        # 4. Check contract address
        contract = col.get("contract", "")
        if contract and len(contract) > 10 and contract.lower() in lower_text:
            best_confidence = max(best_confidence, 0.95)

        # 5. Check slug as last resort
        if slug and len(slug) > 4 and slug.lower() in lower_text:
            best_confidence = max(best_confidence, 0.40)

        if best_confidence > 0.2:
            matches.append((col, best_confidence))

    # Sort by confidence descending
    matches.sort(key=lambda x: x[1], reverse=True)
    return matches


# ═══════════════════════════════════════════════════════
#  Originality / Derivative Detection
# ═══════════════════════════════════════════════════════

# Common blue-chip archetypes for derivative detection
_ARCHETYPE_PATTERNS = [
    ("ape", ["ape", "monkey", "primate", "gorilla"]),
    ("penguin", ["penguin", "pudgy", "waddle"]),
    ("punk", ["punk", "pixel", "8-bit", "retro"]),
    ("cat", ["cat", "kitty", "kitten", "feline"]),
    ("bear", ["bear", "teddy", "grizzly"]),
    ("duck", ["duck", "quack"]),
    ("dog", ["dog", "doge", "shiba", "pup"]),
]

# Words that suggest low originality
_DERIVATIVE_SIGNALS = [
    "inspired by", "tribute to", "homage", "based on",
    "like bayc", "like punks", "like azuki", "like pudgy",
    "next bayc", "next punks", "blue chip potential",
    "pfp collection", "10k pfp", "10,000 pfp",
]

ORIGINALITY_ORIGINAL = "original"
ORIGINALITY_INSPIRED = "inspired"
ORIGINALITY_DERIVATIVE = "derivative"
ORIGINALITY_COPYCAT = "copycat"


def detect_originality(col: Dict) -> Dict:
    """Analyze a collection for originality vs derivative characteristics.

    Returns:
        {
            "label": "original" | "inspired" | "derivative" | "copycat",
            "confidence": float,
            "reasons": [str],
            "archetype_match": str or None
        }
    """
    name = (col.get("name", "") or "").lower()
    desc = (col.get("description", "") or "").lower()
    combined = f"{name} {desc}"

    signals = []
    archetype_match = None

    # Check against known archetypes
    for archetype_name, keywords in _ARCHETYPE_PATTERNS:
        for kw in keywords:
            if kw in name:
                archetype_match = archetype_name
                signals.append(f"Name contains '{kw}' (common {archetype_name} archetype)")
                break

    # Check for derivative language in description
    for phrase in _DERIVATIVE_SIGNALS:
        if phrase in combined:
            signals.append(f"Description contains '{phrase}'")

    # Naming similarity to blue chips
    for bc in BLUE_CHIP_REGISTRY:
        bc_name_lower = bc["name"].lower()
        bc_slug = bc["slug"]
        # Check if the new collection name is suspiciously close
        for alt in [bc_name_lower] + [a.lower() for a in bc.get("alt_names", [])]:
            if alt in name and alt != name and len(alt) > 3:
                signals.append(f"Name resembles blue-chip '{bc['name']}'")
                break

    # Score originality
    if len(signals) >= 3:
        label = ORIGINALITY_COPYCAT
        confidence = 0.8
    elif len(signals) == 2:
        label = ORIGINALITY_DERIVATIVE
        confidence = 0.65
    elif len(signals) == 1:
        label = ORIGINALITY_INSPIRED
        confidence = 0.5
    else:
        label = ORIGINALITY_ORIGINAL
        confidence = 0.7

    return {
        "label": label,
        "confidence": confidence,
        "reasons": signals[:3],
        "archetype_match": archetype_match,
    }


# ═══════════════════════════════════════════════════════
#  Noise / Anti-Spam Detection
# ═══════════════════════════════════════════════════════

def detect_risk_flags(col: Dict) -> List[str]:
    """Detect risk/spam signals on a collection.

    Returns list of human-readable risk flags.
    """
    flags = []
    stats = col.get("_stats", {})
    owners = stats.get("num_owners", 0)
    volume = stats.get("volume_1d", 0)
    sales = stats.get("sales_1d", 0)
    floor = stats.get("floor_price", 0)
    verified = col.get("safelist", "") == "verified"

    # Suspicious volume with few owners
    if volume > 10 and owners < 30:
        flags.append("High volume with very few owners — possible wash trading")

    # Volume spike with no sales
    if volume > 5 and sales < 2:
        flags.append("Volume without proportional sales — suspicious activity")

    # Not verified
    if not verified and volume > 1:
        flags.append("Not verified on OpenSea")

    # No official identity
    if not col.get("twitter") and not col.get("website") and not col.get("discord"):
        flags.append("No official social channels found")

    # Extremely low floor with high volume
    if floor > 0 and floor < 0.001 and volume > 5:
        flags.append("Extremely low floor price — possible spam/bot collection")

    return flags
