import os
from typing import Optional

import httpx
from concierge import Concierge

app = Concierge("s2-streamstore")

S2_ACCESS_TOKEN = os.environ.get("S2_ACCESS_TOKEN", "")
S2_BASE_URL = os.environ.get("S2_BASE_URL", "https://aws.s2.dev/v1")


def _account_url(path: str = "") -> str:
    return f"{S2_BASE_URL}{path}"


def _basin_url(basin: str, path: str = "") -> str:
    return f"https://{basin}.b.s2.dev/v1{path}"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {S2_ACCESS_TOKEN}",
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
def select_basin(basin: str) -> dict:
    """Select a basin to work with. Moves to the basin stage where you can manage streams."""
    app.set_state("current_basin", basin)
    return {"selected_basin": basin, "hint": "You are now in the basin stage. Use list_streams, create_stream, etc."}


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
# Basin-level tools
# ---------------------------------------------------------------------------

@app.tool()
def get_basin_config() -> dict:
    """Get configuration for the currently selected basin."""
    basin = app.get_state("current_basin")
    if not basin:
        return {"error": "No basin selected. Use select_basin first."}
    resp = httpx.get(_account_url(f"/basins/{basin}"), headers=_headers())
    return _result(resp)


@app.tool()
def reconfigure_basin(
    create_stream_on_append: Optional[bool] = None,
    create_stream_on_read: Optional[bool] = None,
) -> dict:
    """Reconfigure the currently selected basin."""
    basin = app.get_state("current_basin")
    if not basin:
        return {"error": "No basin selected. Use select_basin first."}
    body: dict = {}
    if create_stream_on_append is not None:
        body["create_stream_on_append"] = create_stream_on_append
    if create_stream_on_read is not None:
        body["create_stream_on_read"] = create_stream_on_read
    resp = httpx.patch(_account_url(f"/basins/{basin}"), headers=_headers(), json=body)
    return _result(resp)


@app.tool()
def delete_basin() -> dict:
    """Delete the currently selected basin. This is irreversible."""
    basin = app.get_state("current_basin")
    if not basin:
        return {"error": "No basin selected. Use select_basin first."}
    resp = httpx.delete(_account_url(f"/basins/{basin}"), headers=_headers())
    result = _result(resp)
    if resp.status_code in (200, 202):
        app.set_state("current_basin", None)
        app.set_state("current_stream", None)
    return result


@app.tool()
def list_streams(
    prefix: str = "",
    start_after: str = "",
    limit: int = 100,
) -> dict:
    """List streams in the currently selected basin."""
    basin = app.get_state("current_basin")
    if not basin:
        return {"error": "No basin selected. Use select_basin first."}
    params = {"prefix": prefix, "start_after": start_after, "limit": limit}
    resp = httpx.get(_basin_url(basin, "/streams"), headers=_headers(), params=params)
    return _result(resp)


@app.tool()
def create_stream(
    stream: str,
    storage_class: Optional[str] = None,
    retention_policy: Optional[dict] = None,
) -> dict:
    """Create a stream in the currently selected basin. Name must be 1-512 chars."""
    basin = app.get_state("current_basin")
    if not basin:
        return {"error": "No basin selected. Use select_basin first."}
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
def select_stream(stream: str) -> dict:
    """Select a stream to work with. Moves to the stream stage for appending/reading records."""
    basin = app.get_state("current_basin")
    if not basin:
        return {"error": "No basin selected. Use select_basin first."}
    app.set_state("current_stream", stream)
    return {
        "selected_basin": basin,
        "selected_stream": stream,
        "hint": "You are now in the stream stage. Use append_records, read_records, check_tail, etc.",
    }


@app.tool()
def basin_metrics(
    metric_set: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    interval: Optional[str] = None,
) -> dict:
    """Get basin-level metrics. metric_set: 'storage', 'append-ops', 'read-ops', 'read-throughput', 'append-throughput', or 'basin-ops'."""
    basin = app.get_state("current_basin")
    if not basin:
        return {"error": "No basin selected. Use select_basin first."}
    params: dict = {"set": metric_set}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    if interval:
        params["interval"] = interval
    resp = httpx.get(_account_url(f"/metrics/{basin}"), headers=_headers(), params=params)
    return _result(resp)


@app.tool()
def go_back_to_account() -> dict:
    """Go back to the account stage to manage basins and access tokens."""
    app.set_state("current_basin", None)
    app.set_state("current_stream", None)
    return {"hint": "You are now in the account stage."}


# ---------------------------------------------------------------------------
# Stream-level tools
# ---------------------------------------------------------------------------

@app.tool()
def get_stream_config() -> dict:
    """Get configuration for the currently selected stream."""
    basin = app.get_state("current_basin")
    stream = app.get_state("current_stream")
    if not basin or not stream:
        return {"error": "No basin/stream selected."}
    resp = httpx.get(_basin_url(basin, f"/streams/{stream}"), headers=_headers())
    return _result(resp)


@app.tool()
def reconfigure_stream(
    storage_class: Optional[str] = None,
    retention_policy: Optional[dict] = None,
    timestamping: Optional[dict] = None,
) -> dict:
    """Reconfigure the currently selected stream. storage_class: 'standard' or 'express'. retention_policy: {'age': seconds} or {'infinite': {}}."""
    basin = app.get_state("current_basin")
    stream = app.get_state("current_stream")
    if not basin or not stream:
        return {"error": "No basin/stream selected."}
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
def delete_stream() -> dict:
    """Delete the currently selected stream. This is irreversible."""
    basin = app.get_state("current_basin")
    stream = app.get_state("current_stream")
    if not basin or not stream:
        return {"error": "No basin/stream selected."}
    resp = httpx.delete(_basin_url(basin, f"/streams/{stream}"), headers=_headers())
    result = _result(resp)
    if resp.status_code in (200, 202):
        app.set_state("current_stream", None)
    return result


@app.tool()
def append_records(
    records: list[dict],
    match_seq_num: Optional[int] = None,
    fencing_token: Optional[str] = None,
) -> dict:
    """Append records to the currently selected stream.
    Each record in the list can have optional 'headers' (list of [name, value] pairs), 'body' (string), and 'timestamp' (int).
    Example: [{"body": "hello world"}, {"body": "second record", "headers": [["key", "val"]]}]
    """
    basin = app.get_state("current_basin")
    stream = app.get_state("current_stream")
    if not basin or not stream:
        return {"error": "No basin/stream selected."}
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
    seq_num: Optional[int] = None,
    timestamp: Optional[int] = None,
    tail_offset: Optional[int] = None,
    count: Optional[int] = None,
    byte_limit: Optional[int] = None,
    until: Optional[int] = None,
    clamp: Optional[bool] = None,
    wait: Optional[int] = None,
) -> dict:
    """Read records from the currently selected stream. Specify a starting point with seq_num, timestamp, or tail_offset.
    Set clamp=true to start from the tail if the requested position is beyond it (avoids 416 errors).
    Set wait to a number of seconds (up to 60) for long-polling when no records are available yet.
    """
    basin = app.get_state("current_basin")
    stream = app.get_state("current_stream")
    if not basin or not stream:
        return {"error": "No basin/stream selected."}
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
def check_tail() -> dict:
    """Check the tail (next sequence number) of the currently selected stream."""
    basin = app.get_state("current_basin")
    stream = app.get_state("current_stream")
    if not basin or not stream:
        return {"error": "No basin/stream selected."}
    resp = httpx.get(
        _basin_url(basin, f"/streams/{stream}/records/tail"),
        headers=_headers(),
    )
    return _result(resp)


@app.tool()
def stream_metrics(
    metric_set: str = "storage",
    start: Optional[int] = None,
    end: Optional[int] = None,
    interval: Optional[str] = None,
) -> dict:
    """Get stream-level metrics for the currently selected stream. metric_set: 'storage'."""
    basin = app.get_state("current_basin")
    stream = app.get_state("current_stream")
    if not basin or not stream:
        return {"error": "No basin/stream selected."}
    params: dict = {"set": metric_set}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    if interval:
        params["interval"] = interval
    resp = httpx.get(_account_url(f"/metrics/{basin}/{stream}"), headers=_headers(), params=params)
    return _result(resp)


@app.tool()
def go_back_to_basin() -> dict:
    """Go back to the basin stage to manage streams."""
    app.set_state("current_stream", None)
    basin = app.get_state("current_basin")
    return {"current_basin": basin, "hint": "You are now in the basin stage."}


# ---------------------------------------------------------------------------
# Stages & transitions
# ---------------------------------------------------------------------------

app.stages = {
    "account": [
        "list_basins",
        "create_basin",
        "select_basin",
        "list_access_tokens",
        "issue_access_token",
        "revoke_access_token",
        "account_metrics",
    ],
    "basin": [
        "get_basin_config",
        "reconfigure_basin",
        "delete_basin",
        "list_streams",
        "create_stream",
        "select_stream",
        "basin_metrics",
        "go_back_to_account",
    ],
    "stream": [
        "get_stream_config",
        "reconfigure_stream",
        "delete_stream",
        "append_records",
        "read_records",
        "check_tail",
        "stream_metrics",
        "go_back_to_basin",
    ],
}

app.transitions = {
    "account": ["basin"],
    "basin": ["account", "stream"],
    "stream": ["basin"],
}


def run():
    app.run()


if __name__ == "__main__":
    app.run()
