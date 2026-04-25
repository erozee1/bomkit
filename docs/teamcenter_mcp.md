# Teamcenter MCP Server

This project includes a minimal MCP server for communicating with Siemens Teamcenter
over its REST endpoints.

## Install

```bash
pip install -e .
```

## Run (stdio)

```bash
TEAMCENTER_BASE_URL="https://teamcenter.example.com/tc" \
TEAMCENTER_USERNAME="your-user" \
TEAMCENTER_PASSWORD="your-pass" \
teamcenter-mcp
```

## Environment variables

- `TEAMCENTER_BASE_URL` (required): Base Teamcenter URL, e.g. `https://host:port/tc`
- `TEAMCENTER_AUTH_MODE`: `basic` (default), `bearer`, or `none`
- `TEAMCENTER_USERNAME` / `TEAMCENTER_PASSWORD`: Basic auth credentials
- `TEAMCENTER_BEARER_TOKEN`: Bearer token for `bearer` mode
- `TEAMCENTER_VERIFY_TLS`: `true`/`false` (default `true`)
- `TEAMCENTER_CA_BUNDLE`: Path to a CA bundle (overrides `TEAMCENTER_VERIFY_TLS`)
- `TEAMCENTER_TIMEOUT`: Request timeout in seconds (default `30`)
- `TEAMCENTER_DEFAULT_HEADERS`: JSON object of headers to include in every request
- `TEAMCENTER_SEARCH_PATH`: Search endpoint path (default `/tc/search`)
- `TEAMCENTER_SEARCH_QUERY_PARAM`: Query parameter name (default `query`)
- `TEAMCENTER_ITEM_PATH_TEMPLATE`: Item path template (default `/tc/item/{item_id}`)
- `TEAMCENTER_CREATE_ITEM_PATH`: Item create path (default `/tc/item`)
- `TEAMCENTER_UPDATE_ITEM_PATH_TEMPLATE`: Update path template (default `/tc/item/{item_id}`)

## Tools

- `health_check(path="/tc/controller/test")`
- `request(method, path, params=None, json_body=None, headers=None)`
- `search_items(query, params=None, path=None)`
- `get_item(item_id, path_template=None)`
- `create_item(payload, path=None)`
- `update_item(item_id, payload, method="PATCH", path_template=None)`
