# S2 StreamStore MCP Server

An MCP server for [S2 StreamStore](https://s2.dev) built with [Concierge](https://github.com/concierge-hq/concierge) progressive tool disclosure.

Instead of exposing all S2 API tools at once, the server guides AI agents through a natural workflow:

```
account → basin → stream
```

At each stage, only the relevant tools are visible.

## Stages

| Stage | Tools | Description |
|---|---|---|
| **account** | `list_basins`, `create_basin`, `select_basin`, `list_access_tokens`, `issue_access_token`, `revoke_access_token`, `account_metrics` | Manage basins and access tokens |
| **basin** | `get_basin_config`, `reconfigure_basin`, `delete_basin`, `list_streams`, `create_stream`, `select_stream`, `basin_metrics`, `go_back_to_account` | Manage streams within a basin |
| **stream** | `get_stream_config`, `reconfigure_stream`, `delete_stream`, `append_records`, `read_records`, `check_tail`, `stream_metrics`, `go_back_to_basin` | Read/write records and manage stream config |

## Setup

```bash
pip install -e .
```

## Configuration

Set your S2 access token as an environment variable:

```bash
export S2_ACCESS_TOKEN="your-token-here"
```

## Run

**stdio** (for Cursor, Claude Desktop, etc.):

```bash
python main.py
```

## MCP Client Configuration

Add to your MCP client config (e.g. `~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "s2": {
      "command": "python",
      "args": ["/path/to/mcp/main.py"],
      "env": {
        "S2_ACCESS_TOKEN": "your-token-here"
      }
    }
  }
}
```
