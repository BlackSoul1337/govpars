from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, SecretStr

HOP_BY_HOP_HEADERS = {
    "accept-encoding",
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
HEADER_NAME_PATTERN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
BROWSER_FORBIDDEN_HEADERS = {
    "accept-charset",
    "accept-encoding",
    "access-control-request-headers",
    "access-control-request-method",
    "connection",
    "content-length",
    "cookie",
    "cookie2",
    "date",
    "dnt",
    "expect",
    "host",
    "keep-alive",
    "origin",
    "permissions-policy",
    "referer",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "via",
}


def is_valid_header_name(name: str) -> bool:
    return bool(HEADER_NAME_PATTERN.fullmatch(name))


class RequestAuthMaterial(BaseModel):
    headers: dict[str, SecretStr] = Field(default_factory=dict)
    expires_at: datetime | None = None

    def revealed_headers(self) -> dict[str, str]:
        return {key: value.get_secret_value() for key, value in self.headers.items()}


class RequestProfile(BaseModel):
    method: str
    url_pattern: str
    static_headers: dict[str, str] = Field(default_factory=dict)
    session_headers: dict[str, SecretStr] = Field(default_factory=dict)
    request_scoped_header_names: set[str] = Field(default_factory=set)
    captured_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    body_template: dict[str, Any] | list[Any] | str | None = None
    browser_only: bool = False

    def transferable_headers(self) -> dict[str, str]:
        result = {
            key: value
            for key, value in self.static_headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS
            and key.lower() not in self.request_scoped_header_names
        }
        result.update(
            {
                key: value.get_secret_value()
                for key, value in self.session_headers.items()
                if key.lower() not in HOP_BY_HOP_HEADERS
                and key.lower() not in self.request_scoped_header_names
            }
        )
        return result

    def browser_transferable_headers(self) -> dict[str, str]:
        return {
            key: value
            for key, value in self.transferable_headers().items()
            if is_valid_header_name(key)
            and key.lower() not in BROWSER_FORBIDDEN_HEADERS
            and not key.lower().startswith(("proxy-", "sec-"))
        }


class SessionIdentity(BaseModel):
    lane_id: str
    source: str
    proxy_id: str | None = None
    proxy_url: SecretStr | None = None
    user_agent: str
    tls_impersonation: str = "chrome"
    cookies: dict[str, SecretStr] = Field(default_factory=dict)
    storage_state_path: str | None = None
    request_profiles: dict[str, RequestProfile] = Field(default_factory=dict)
    auth_material: RequestAuthMaterial = Field(default_factory=RequestAuthMaterial)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None

    def cookie_dict(self) -> dict[str, str]:
        return {key: value.get_secret_value() for key, value in self.cookies.items()}
