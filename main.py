import os
from typing import Optional

import httpx
from concierge import Concierge
from mcp.server.lowlevel.server import request_ctx

app = Concierge("s2-streamstore", host=os.environ.get("FASTMCP_HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8000")))

S2_BASE_URL = os.environ.get("S2_BASE_URL", "https://aws.s2.dev/v1")


def _account_url(path: str = "") -> str:
    return f"{S2_BASE_URL}{path}"


def _basin_url(basin: str, path: str = "") -> str:
    return f"https://{basin}.b.s2.dev/v1{path}"


def _headers() -> dict:
    ctx = request_ctx.get()
    token = ctx.request.headers.get("authorization", "").removeprefix("Bearer ")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _result(resp: httpx.Response) -> dict:
    if resp.status_code in (202, 204):
        return {"status": resp.status_code, "message": "ok"}
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code, "body": resp.text}


# ---------------------------------------------------------------------------
# Account-level tools
# ---------------------------------------------------------------------------

@app.tool()
def list_basins(
    prefix: str = "",
    start_after: str = "",
    limit: int = 100,
) -> dict:
    """List basins in the account. Optionally filter by prefix."""
    params = {"prefix": prefix, "start_after": start_after, "limit": limit}
    resp = httpx.get(_account_url("/basins"), headers=_headers(), params=params)
    return _result(resp)


@app.tool()
def create_basin(
    basin: str,
    scope: Optional[str] = None,
    create_stream_on_append: bool = False,
    create_stream_on_read: bool = False,
) -> dict:
    """Create a new basin. Name must be globally unique, 8-48 chars, lowercase alphanumeric and hyphens."""
    body: dict = {"basin": basin}
    config: dict = {}
    if create_stream_on_append:
        config["create_stream_on_append"] = True
    if create_stream_on_read:
        config["create_stream_on_read"] = True
    if config:
        body["config"] = config
    if scope:
        body["scope"] = scope
    resp = httpx.post(_account_url("/basins"), headers=_headers(), json=body)
    return _result(resp)


@app.tool()
def get_basin_config(basin: str) -> dict:
    """Get configuration for a basin."""
    resp = httpx.get(_account_url(f"/basins/{basin}"), headers=_headers())
    return _result(resp)


@app.tool()
def reconfigure_basin(
    basin: str,
    create_stream_on_append: Optional[bool] = None,
    create_stream_on_read: Optional[bool] = None,
) -> dict:
    """Reconfigure a basin."""
    body: dict = {}
    if create_stream_on_append is not None:
        body["create_stream_on_append"] = create_stream_on_append
    if create_stream_on_read is not None:
        body["create_stream_on_read"] = create_stream_on_read
    resp = httpx.patch(_account_url(f"/basins/{basin}"), headers=_headers(), json=body)
    return _result(resp)


@app.tool()
def delete_basin(basin: str) -> dict:
    """Delete a basin. This is irreversible."""
    resp = httpx.delete(_account_url(f"/basins/{basin}"), headers=_headers())
    return _result(resp)


@app.tool()
def list_access_tokens(
    prefix: str = "",
    start_after: str = "",
    limit: int = 100,
) -> dict:
    """List access tokens in the account."""
    params = {"prefix": prefix, "start_after": start_after, "limit": limit}
    resp = httpx.get(_account_url("/access-tokens"), headers=_headers(), params=params)
    return _result(resp)


@app.tool()
def issue_access_token(
    id: str,
    scope: Optional[dict] = None,
    expires_at: Optional[str] = None,
    auto_prefix_streams: bool = False,
) -> dict:
    """Issue a new access token. Provide an id and scope definition."""
    body: dict = {"id": id, "scope": scope or {}}
    if expires_at:
        body["expires_at"] = expires_at
    if auto_prefix_streams:
        body["auto_prefix_streams"] = True
    resp = httpx.post(_account_url("/access-tokens"), headers=_headers(), json=body)
    return _result(resp)


@app.tool()
def revoke_access_token(id: str) -> dict:
    """Revoke an access token by ID."""
    resp = httpx.delete(_account_url(f"/access-tokens/{id}"), headers=_headers())
    return _result(resp)


@app.tool()
def account_metrics(
    metric_set: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    interval: Optional[str] = None,
) -> dict:
    """Get account-level metrics. metric_set: 'active-basins' or 'account-ops'."""
    params: dict = {"set": metric_set}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    if interval:
        params["interval"] = interval
    resp = httpx.get(_account_url("/metrics"), headers=_headers(), params=params)
    return _result(resp)


# ---------------------------------------------------------------------------
# Basin-level tools (basin passed explicitly)
# ---------------------------------------------------------------------------

@app.tool()
def list_streams(
    basin: str,
    prefix: str = "",
    start_after: str = "",
    limit: int = 100,
) -> dict:
    """List streams in a basin."""
    params = {"prefix": prefix, "start_after": start_after, "limit": limit}
    resp = httpx.get(_basin_url(basin, "/streams"), headers=_headers(), params=params)
    return _result(resp)


@app.tool()
def create_stream(
    basin: str,
    stream: str,
    storage_class: Optional[str] = None,
    retention_policy: Optional[dict] = None,
) -> dict:
    """Create a stream in a basin. Name must be 1-512 chars."""
    body: dict = {"stream": stream}
    config: dict = {}
    if storage_class:
        config["storage_class"] = storage_class
    if retention_policy:
        config["retention_policy"] = retention_policy
    if config:
        body["config"] = config
    resp = httpx.post(_basin_url(basin, "/streams"), headers=_headers(), json=body)
    return _result(resp)


@app.tool()
def get_stream_config(basin: str, stream: str) -> dict:
    """Get configuration for a stream."""
    resp = httpx.get(_basin_url(basin, f"/streams/{stream}"), headers=_headers())
    return _result(resp)


@app.tool()
def reconfigure_stream(
    basin: str,
    stream: str,
    storage_class: Optional[str] = None,
    retention_policy: Optional[dict] = None,
    timestamping: Optional[dict] = None,
) -> dict:
    """Reconfigure a stream. storage_class: 'standard' or 'express'. retention_policy: {'age': seconds} or {'infinite': {}}."""
    body: dict = {}
    if storage_class:
        body["storage_class"] = storage_class
    if retention_policy:
        body["retention_policy"] = retention_policy
    if timestamping:
        body["timestamping"] = timestamping
    resp = httpx.patch(_basin_url(basin, f"/streams/{stream}"), headers=_headers(), json=body)
    return _result(resp)


@app.tool()
def delete_stream(basin: str, stream: str) -> dict:
    """Delete a stream. This is irreversible."""
    resp = httpx.delete(_basin_url(basin, f"/streams/{stream}"), headers=_headers())
    return _result(resp)


@app.tool()
def basin_metrics(
    basin: str,
    metric_set: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    interval: Optional[str] = None,
) -> dict:
    """Get basin-level metrics. metric_set: 'storage', 'append-ops', 'read-ops', 'read-throughput', 'append-throughput', or 'basin-ops'."""
    params: dict = {"set": metric_set}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    if interval:
        params["interval"] = interval
    resp = httpx.get(_account_url(f"/metrics/{basin}"), headers=_headers(), params=params)
    return _result(resp)


# ---------------------------------------------------------------------------
# Stream-level tools (basin + stream passed explicitly)
# ---------------------------------------------------------------------------

@app.tool()
def append_records(
    basin: str,
    stream: str,
    records: list[dict],
    match_seq_num: Optional[int] = None,
    fencing_token: Optional[str] = None,
) -> dict:
    """Append records to a stream.
    Each record can have optional 'headers' (list of [name, value] pairs), 'body' (string), and 'timestamp' (int).
    Example: [{"body": "hello world"}, {"body": "second record", "headers": [["key", "val"]]}]
    """
    body: dict = {"records": records}
    if match_seq_num is not None:
        body["match_seq_num"] = match_seq_num
    if fencing_token is not None:
        body["fencing_token"] = fencing_token
    resp = httpx.post(
        _basin_url(basin, f"/streams/{stream}/records"),
        headers=_headers(),
        json=body,
    )
    return _result(resp)


@app.tool()
def read_records(
    basin: str,
    stream: str,
    seq_num: Optional[int] = None,
    timestamp: Optional[int] = None,
    tail_offset: Optional[int] = None,
    count: Optional[int] = None,
    byte_limit: Optional[int] = None,
    until: Optional[int] = None,
    clamp: Optional[bool] = None,
    wait: Optional[int] = None,
) -> dict:
    """Read records from a stream. Specify a starting point with seq_num, timestamp, or tail_offset.
    Set clamp=true to start from the tail if the requested position is beyond it (avoids 416 errors).
    Set wait to a number of seconds (up to 60) for long-polling when no records are available yet.
    """
    params: dict = {}
    if seq_num is not None:
        params["seq_num"] = seq_num
    if timestamp is not None:
        params["timestamp"] = timestamp
    if tail_offset is not None:
        params["tail_offset"] = tail_offset
    if count is not None:
        params["count"] = count
    if byte_limit is not None:
        params["bytes"] = byte_limit
    if until is not None:
        params["until"] = until
    if clamp is not None:
        params["clamp"] = clamp
    if wait is not None:
        params["wait"] = wait
    resp = httpx.get(
        _basin_url(basin, f"/streams/{stream}/records"),
        headers=_headers(),
        params=params,
    )
    return _result(resp)


@app.tool()
def check_tail(basin: str, stream: str) -> dict:
    """Check the tail (next sequence number) of a stream."""
    resp = httpx.get(
        _basin_url(basin, f"/streams/{stream}/records/tail"),
        headers=_headers(),
    )
    return _result(resp)


@app.tool()
def stream_metrics(
    basin: str,
    stream: str,
    metric_set: str = "storage",
    start: Optional[int] = None,
    end: Optional[int] = None,
    interval: Optional[str] = None,
) -> dict:
    """Get stream-level metrics. metric_set: 'storage'."""
    params: dict = {"set": metric_set}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    if interval:
        params["interval"] = interval
    resp = httpx.get(_account_url(f"/metrics/{basin}/{stream}"), headers=_headers(), params=params)
    return _result(resp)


# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------

from starlette.middleware.cors import CORSMiddleware

http_app = app.streamable_http_app()
http_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["mcp-session-id"],
)


def run():
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(http_app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    run()
