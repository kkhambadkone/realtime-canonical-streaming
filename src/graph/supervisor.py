"""
src/graph/supervisor.py  -  LangGraph startup pipeline.

LangGraph orchestrates the startup phase only:
  schema_discovery → entity_matcher → canonical_builder →
  stream_coordinator → streaming (Spark takes over)

After stream_coordinator, Spark Structured Streaming runs indefinitely.
"""
from __future__ import annotations

import functools
import structlog
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.graph import StateGraph, END

from src.graph.state import StreamingPipelineState

log = structlog.get_logger()

# Lazy imports to avoid PySpark errors at module load time
def _import_nodes():
    from src.graph.nodes.schema_discovery import schema_discovery_node
    from src.graph.nodes.entity_matcher import entity_matcher_node
    from src.graph.nodes.canonical_builder import canonical_builder_node
    from src.graph.nodes.stream_coordinator import stream_coordinator_node
    return (schema_discovery_node, entity_matcher_node,
            canonical_builder_node, stream_coordinator_node)


def _phase_router(state: StreamingPipelineState) -> str:
    phase = state.get("phase", "error")
    log.debug("supervisor.routing", phase=phase)
    return phase


def build_graph(
    llm: BaseChatModel,
    pg_tools: list[BaseTool] = [],
    sql_tools: list[BaseTool] = [],
    td_tools: list[BaseTool] = [],
    dh_tools: list[BaseTool] = [],
) -> Any:
    (schema_discovery_node, entity_matcher_node,
     canonical_builder_node, stream_coordinator_node) = _import_nodes()

    graph = StateGraph(StreamingPipelineState)

    graph.add_node("schema_discovery",
        functools.partial(schema_discovery_node,
                          pg_tools=pg_tools, sql_tools=sql_tools,
                          td_tools=td_tools))
    graph.add_node("entity_matcher",
        functools.partial(entity_matcher_node, llm=llm))
    graph.add_node("canonical_builder",
        functools.partial(canonical_builder_node, llm=llm))
    graph.add_node("stream_coordinator", stream_coordinator_node)

    graph.set_entry_point("schema_discovery")

    phase_map = {
        "schema_discovery":  "schema_discovery",
        "entity_matcher":    "entity_matcher",
        "canonical_builder": "canonical_builder",
        "stream_coordinator": "stream_coordinator",
        "streaming":         END,   # Spark takes over
        "done":              END,
        "error":             END,
    }

    for node in ["schema_discovery", "entity_matcher",
                 "canonical_builder", "stream_coordinator"]:
        graph.add_conditional_edges(node, _phase_router, phase_map)

    return graph.compile()
