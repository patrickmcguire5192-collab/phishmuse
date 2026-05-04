"""
Vercel serverless entrypoint for the JamMuse MCP server.

Vercel's @vercel/python runtime auto-detects the module-level `app` variable
as an ASGI callable. We use stateless_http=True since serverless functions
don't preserve session state between invocations, and json_response=True for
broadest compatibility (no chunked streaming required).
"""

import sys
from pathlib import Path

# Add project root to sys.path so `scripts.*` resolves
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.mcp_server import mcp  # noqa: E402

app = mcp.http_app(
    path="/",
    stateless_http=True,
    json_response=True,
)
