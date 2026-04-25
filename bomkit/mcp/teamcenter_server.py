from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from mcp.server.fastmcp import FastMCP
from requests.auth import HTTPBasicAuth


@dataclass(frozen=True)
class TeamcenterConfig:
    base_url: str
    auth_mode: str
    username: Optional[str]
    password: Optional[str]
    bearer_token: Optional[str]
    verify_tls: object
    timeout: int
    default_headers: Dict[str, str]
    search_path: str
    search_query_param: str
    item_path_template: str
    create_item_path: str
    update_item_path_template: str


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_config() -> TeamcenterConfig:
    base_url = os.getenv("TEAMCENTER_BASE_URL")
    if not base_url:
        raise ValueError(
            "TEAMCENTER_BASE_URL is required, e.g. https://teamcenter.example.com/tc"
        )

    auth_mode = os.getenv("TEAMCENTER_AUTH_MODE", "basic").strip().lower()
    username = os.getenv("TEAMCENTER_USERNAME")
    password = os.getenv("TEAMCENTER_PASSWORD")
    bearer_token = os.getenv("TEAMCENTER_BEARER_TOKEN")

    verify_tls_env = os.getenv("TEAMCENTER_VERIFY_TLS")
    ca_bundle = os.getenv("TEAMCENTER_CA_BUNDLE")
    verify_tls: object = _parse_bool(verify_tls_env, True)
    if ca_bundle:
        verify_tls = ca_bundle

    timeout = int(os.getenv("TEAMCENTER_TIMEOUT", "30"))

    default_headers: Dict[str, str] = {}
    default_headers_env = os.getenv("TEAMCENTER_DEFAULT_HEADERS")
    if default_headers_env:
        default_headers = json.loads(default_headers_env)

    return TeamcenterConfig(
        base_url=base_url,
        auth_mode=auth_mode,
        username=username,
        password=password,
        bearer_token=bearer_token,
        verify_tls=verify_tls,
        timeout=timeout,
        default_headers=default_headers,
        search_path=os.getenv("TEAMCENTER_SEARCH_PATH", "/tc/search"),
        search_query_param=os.getenv("TEAMCENTER_SEARCH_QUERY_PARAM", "query"),
        item_path_template=os.getenv("TEAMCENTER_ITEM_PATH_TEMPLATE", "/tc/item/{item_id}"),
        create_item_path=os.getenv("TEAMCENTER_CREATE_ITEM_PATH", "/tc/item"),
        update_item_path_template=os.getenv(
            "TEAMCENTER_UPDATE_ITEM_PATH_TEMPLATE", "/tc/item/{item_id}"
        ),
    )


def _join_url(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _build_auth(config: TeamcenterConfig) -> Optional[HTTPBasicAuth]:
    if config.auth_mode == "basic":
        if not config.username or not config.password:
            raise ValueError("Basic auth requires TEAMCENTER_USERNAME and TEAMCENTER_PASSWORD")
        return HTTPBasicAuth(config.username, config.password)
    return None


def _build_headers(config: TeamcenterConfig, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    merged = dict(config.default_headers)
    if headers:
        merged.update(headers)
    if config.auth_mode == "bearer":
        if not config.bearer_token:
            raise ValueError("Bearer auth requires TEAMCENTER_BEARER_TOKEN")
        merged.setdefault("Authorization", f"Bearer {config.bearer_token}")
    return merged


def _request(
    config: TeamcenterConfig,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    url = _join_url(config.base_url, path)
    auth = _build_auth(config)
    req_headers = _build_headers(config, headers)

    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json_body,
            headers=req_headers,
            auth=auth,
            timeout=config.timeout,
            verify=config.verify_tls,
        )
    except requests.RequestException as exc:
        return {
            "ok": False,
            "status": None,
            "error": str(exc),
            "url": url,
        }

    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            body: Any = response.json()
        except ValueError:
            body = response.text
    else:
        body = response.text

    return {
        "ok": response.ok,
        "status": response.status_code,
        "url": url,
        "headers": dict(response.headers),
        "body": body,
    }


mcp = FastMCP("teamcenter")


@mcp.tool()
def health_check(path: str = "/tc/controller/test") -> Dict[str, Any]:
    """Check connectivity to Teamcenter with a simple GET request."""
    config = _load_config()
    return _request(config, "GET", path)


@mcp.tool()
def request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Send an arbitrary Teamcenter REST request."""
    config = _load_config()
    return _request(config, method, path, params=params, json_body=json_body, headers=headers)


@mcp.tool()
def search_items(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    path: Optional[str] = None,
) -> Dict[str, Any]:
    """Search for items via a configurable Teamcenter search endpoint."""
    config = _load_config()
    search_params = dict(params or {})
    search_params.setdefault(config.search_query_param, query)
    return _request(config, "GET", path or config.search_path, params=search_params)


@mcp.tool()
def get_item(item_id: str, path_template: Optional[str] = None) -> Dict[str, Any]:
    """Fetch a Teamcenter item by ID using a configurable path template."""
    config = _load_config()
    template = path_template or config.item_path_template
    path = template.format(item_id=item_id)
    return _request(config, "GET", path)


@mcp.tool()
def create_item(
    payload: Dict[str, Any],
    path: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a Teamcenter item using a configurable endpoint."""
    config = _load_config()
    return _request(config, "POST", path or config.create_item_path, json_body=payload)


@mcp.tool()
def update_item(
    item_id: str,
    payload: Dict[str, Any],
    method: str = "PATCH",
    path_template: Optional[str] = None,
) -> Dict[str, Any]:
    """Update a Teamcenter item using a configurable endpoint."""
    config = _load_config()
    template = path_template or config.update_item_path_template
    path = template.format(item_id=item_id)
    return _request(config, method, path, json_body=payload)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
