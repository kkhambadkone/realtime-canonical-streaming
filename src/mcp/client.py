"""
src/mcp/client.py  -  MCP client for streaming pipeline.
"""
from __future__ import annotations

import structlog
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_core.tools import BaseTool
from src.config import settings

log = structlog.get_logger()


def _build_mcp_config() -> dict:
    config: dict = {}
    config["postgres"] = {
        "url": settings.postgres_mcp_url, "transport": "sse",
    }
    if settings.teradata_mcp_url:
        config["teradata"] = {
            # Teradata's official MCP server (Teradata/teradata-mcp-server) serves
            # "streamable-http" at a /mcp path, not SSE at /sse. Using "sse" here
            # against a streamable-http endpoint causes the client to hang on
            # connect instead of failing fast — match whatever transport your
            # server was actually started with (see README MCP setup section).
            "url": settings.teradata_mcp_url, "transport": "streamable_http",
        }
    config["datahub"] = {
        "url": settings.datahub_mcp_url, "transport": "sse",
    }
    return config


async def get_mcp_tools() -> dict[str, list[BaseTool]]:
    mcp_config = _build_mcp_config()
    client     = MultiServerMCPClient(mcp_config)
    tools_by_server: dict[str, list[BaseTool]] = {}
    for server_name in mcp_config:
        try:
            tools = await client.get_tools(server_name=server_name)
            tools_by_server[server_name] = tools
            log.info("mcp.connected", server=server_name,
                     tool_count=len(tools))
        except Exception as exc:
            log.error("mcp.failed", server=server_name, error=str(exc))
            tools_by_server[server_name] = []
    return tools_by_server


def get_tool(tools: list[BaseTool], name: str) -> BaseTool | None:
    return next((t for t in tools if t.name == name), None)


def find_tool_fuzzy(tools: list[BaseTool], *candidates: str) -> BaseTool | None:
    for name in candidates:
        tool = get_tool(tools, name)
        if tool:
            return tool
    return None
