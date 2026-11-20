"""Optional stateless MCP Streamable HTTP adapter for scoped Engine research.

This adapter never authenticates against the administrative Engine API. Tools
reuse the exact existing Research HTTP handlers, including their resource
checks, idempotency, and task ownership verification. JSON-only responses avoid
a second streaming/session subsystem in the trading Engine.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

MCP_PROTOCOL_VERSION = "2025-11-25"
_MAX_REQUEST_BYTES = 512 * 1024


@dataclass(frozen=True)
class ResearchTool:
    name: str
    operation: str
    description: str
    input_schema: dict[str, Any]
    execute: Callable[[dict[str, Any], dict], dict]

    def public(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }


def _reply(identity: str | int | None, result: dict) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": identity, "result": result})


def _error(identity: str | int | None, code: int, message: str) -> JSONResponse:
    return JSONResponse({
        "jsonrpc": "2.0", "id": identity,
        "error": {"code": code, "message": message},
    })


def _tool_error(message: str) -> dict:
    return {"isError": True, "content": [{"type": "text", "text": message}]}


def install_research_mcp(
    app: FastAPI,
    authenticate: Callable[..., dict],
    tools: Iterable[ResearchTool],
) -> None:
    """Install one opt-in MCP endpoint inside the already-isolated Research ASGI app.

    The standard MCP transport itself is not an authorization mechanism.
    Every POST must carry a valid, short-lived Research Grant; the Engine
    independently verifies tenant status and each tool's operation scope.
    """
    defined = tuple(tools)
    registry = {tool.name: tool for tool in defined}
    if not registry or len(registry) != len(defined):
        raise ValueError("Research MCP tools must have distinct names")

    auth_dependency = Depends(authenticate)

    @app.post("/mcp")
    async def mcp(request: Request, claims: dict = auth_dependency):
        # No browser clients or cross-origin access: refuse all Origin headers,
        # including "null". Service-to-service MCP callers do not send Origin.
        if request.headers.get("origin") is not None:
            raise HTTPException(403, "Browser origins not allowed on Research MCP")
        media = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media != "application/json":
            raise HTTPException(415, "MCP requires application/json")
        accept = request.headers.get("accept", "*/*").lower()
        if not any(value in accept for value in ("application/json", "*/*")):
            raise HTTPException(406, "MCP JSON response not accepted")
        version = request.headers.get("mcp-protocol-version")
        if version is not None and version != MCP_PROTOCOL_VERSION:
            raise HTTPException(400, "Unsupported MCP protocol version")

        content = bytearray()
        async for chunk in request.stream():
            if len(content) + len(chunk) > _MAX_REQUEST_BYTES:
                raise HTTPException(413, "MCP request too large")
            content.extend(chunk)
        try:
            message = json.loads(content)
        except (UnicodeError, ValueError):
            return _error(None, -32700, "Parse error")
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return _error(None, -32600, "Invalid Request")
        method = message.get("method")
        identity = message.get("id")
        if not isinstance(method, str) or not method:
            return _error(None, -32600, "Invalid Request")
        if "id" not in message:
            # Notifications never run tools or modify Engine state.
            if method in {"notifications/initialized", "notifications/cancelled"}:
                return Response(status_code=202)
            return _error(None, -32600, "Invalid Request")
        if type(identity) not in {str, int}:
            return _error(None, -32600, "Invalid Request")
        params = message.get("params", {})
        if not isinstance(params, dict):
            return _error(identity, -32602, "Invalid params")

        if method == "initialize":
            if not isinstance(params.get("protocolVersion"), str):
                return _error(identity, -32602, "Invalid initialize params")
            return _reply(identity, {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "seealpha-engine-research", "version": "1.0.0"},
                "instructions": "Development-only, task-scoped research tools. "
                                "No trading or Engine administration.",
            })
        if method == "ping":
            return _reply(identity, {})
        if method == "tools/list":
            if params.get("cursor") is not None:
                return _error(identity, -32602, "Unsupported tools cursor")
            available = [
                tool.public() for tool in registry.values()
                if tool.operation in claims["ops"]
            ]
            return _reply(identity, {"tools": available})
        if method != "tools/call":
            return _error(identity, -32601, "Method not found")

        name = params.get("name")
        arguments = params.get("arguments", {})
        tool = registry.get(name) if isinstance(name, str) else None
        if tool is None or tool.operation not in claims["ops"]:
            return _error(identity, -32602, "Unknown or unavailable tool")
        if not isinstance(arguments, dict):
            return _error(identity, -32602, "Invalid tool arguments")
        try:
            # Research handlers are synchronous and can read disk or wait on a
            # journal. Do not block the main Engine's async event loop.
            output = await run_in_threadpool(tool.execute, arguments, claims)
        except (ValidationError, ValueError, TypeError, KeyError):
            return _reply(identity, _tool_error("Invalid research tool arguments"))
        except HTTPException as exc:
            # Do not echo internal HTTP error bodies or tenant details to AI.
            return _reply(identity, _tool_error(
                f"Research operation rejected (HTTP {exc.status_code})",
            ))
        except Exception:  # noqa: BLE001 - do not disclose paths, keys or raw errors
            return _reply(identity, _tool_error("Research operation unavailable"))
        if not isinstance(output, dict):
            return _reply(identity, _tool_error("Invalid Engine research response"))
        try:
            serialized = json.dumps(
                output, ensure_ascii=False, allow_nan=False, separators=(",", ":"),
            )
        except (TypeError, ValueError):
            return _reply(identity, _tool_error("Invalid Engine research response"))
        return _reply(identity, {
            "content": [{"type": "text", "text": serialized}],
            "structuredContent": output,
        })
