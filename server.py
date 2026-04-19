#!/usr/bin/env python3
"""
Insights-Only Cloud Server
===========================
Locked-down server exposing ONLY the Alpha Engine insights dashboard.
NO wallet, NO settings, NO minting, NO private data.

Environment Variables:
  OPENSEA_API_KEY   - OpenSea API key
  OPENSEA_MCP_TOKEN - OpenSea MCP token  
  PORT              - Server port (default: 10000, Render default)
"""

import asyncio, logging, sys, os
from pathlib import Path
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
import uvicorn

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("insights_cloud")

# ── ONLY these endpoints are exposed ──

async def page_insights(request):
    html_path = Path(__file__).parent / "static" / "insights.html"
    return HTMLResponse(html_path.read_text())

async def api_insights(request):
    from src.engine.insights import run_analysis
    force = request.query_params.get("force", "0") == "1"
    result = await run_analysis(force=force)
    return JSONResponse(result)

async def api_insights_refresh(request):
    """Trigger background refresh and return immediately."""
    from src.engine.insights import run_analysis, get_progress
    progress = get_progress()
    if progress.get("running"):
        return JSONResponse({"status": "already_running", **progress})
    # Fire and forget
    asyncio.create_task(run_analysis(force=True))
    return JSONResponse({"status": "refresh_started"})

async def api_insights_progress(request):
    from src.engine.insights import get_progress
    return JSONResponse(get_progress())

async def api_collections_minting(request):
    from src.engine.insights import run_analysis
    data = await run_analysis()
    return JSONResponse(data.get("minting_now", []))

async def api_collections_upcoming(request):
    from src.engine.insights import run_analysis
    data = await run_analysis()
    return JSONResponse(data.get("upcoming", []))

# ── Background analysis on startup ──
async def startup_analysis():
    logger.info("[CLOUD] Starting initial analysis...")
    try:
        from src.engine.insights import run_analysis
        await run_analysis()
        logger.info("[CLOUD] Initial analysis complete")
    except Exception as e:
        logger.error(f"[CLOUD] Initial analysis failed: {e}")

async def on_startup():
    asyncio.create_task(startup_analysis())

# ── App: insights-only routes ──
routes = [
    Route("/insights", page_insights),
    Route("/", page_insights),
    Route("/api/insights", api_insights),
    Route("/api/insights/refresh", api_insights_refresh),
    Route("/api/insights/progress", api_insights_progress),
    Route("/api/collections/minting", api_collections_minting),
    Route("/api/collections/upcoming", api_collections_upcoming),
    Mount("/static", app=StaticFiles(directory=str(Path(__file__).parent / "static")), name="static"),
]

app = Starlette(routes=routes, on_startup=[on_startup])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"\n  🔒 Insights-Only Cloud Server — port {port}")
    print(f"  📊 Dashboard → http://0.0.0.0:{port}/insights")
    print(f"  ⛔ No wallet/settings/minting endpoints\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
