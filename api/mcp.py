"""
Vercel serverless entrypoint for the JamMuse MCP server.

Vercel's @vercel/python runtime auto-detects the module-level `app` variable
as an ASGI callable. We mount the FastMCP app at multiple paths so it
handles requests regardless of how Vercel rewrites the URL.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from starlette.applications import Starlette  # noqa: E402
from starlette.routing import Mount  # noqa: E402

from scripts.mcp_server import mcp  # noqa: E402

# FastMCP serves the MCP endpoint at "/" inside its own ASGI app.
# We mount it at every URL the client might use so the function works
# whether Vercel delivers the original or rewritten path.
_mcp_app = mcp.http_app(path="/", stateless_http=True, json_response=True)

app = Starlette(
    routes=[
        Mount("/api/mcp", app=_mcp_app),
        Mount("/mcp", app=_mcp_app),
    ],
    lifespan=_mcp_app.lifespan,
)
