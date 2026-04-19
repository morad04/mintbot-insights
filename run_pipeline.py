#!/usr/bin/env python3
"""
Run the insights pipeline and save the output as a JSON file for GitHub Pages.
Used by GitHub Actions to generate static data.
"""
import asyncio, json, sys, os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

async def main():
    from src.engine.insights import run_analysis
    print("[RUNNER] Starting insights pipeline...")
    result = await run_analysis(force=True)
    
    # Save to docs/data.json for GitHub Pages
    docs = Path(__file__).parent / "docs"
    docs.mkdir(exist_ok=True)
    out = docs / "data.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    
    # Summary
    minting = len(result.get("minting_now", []))
    upcoming = len(result.get("upcoming", []))
    alpha = len(result.get("alpha", []))
    print(f"[RUNNER] Done — {minting} minting, {upcoming} upcoming, {alpha} alpha")
    print(f"[RUNNER] Saved to {out}")

if __name__ == "__main__":
    asyncio.run(main())
