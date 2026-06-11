from __future__ import annotations

import re
from urllib.parse import urlsplit

DETAIL_PATH_PATTERNS = (
    re.compile(r"(?P<prefix>/4dv3rts/lots)/[^/]+$"),
    re.compile(r"(?P<prefix>/4dv3rts)/[^/]+$"),
    re.compile(r"(?P<prefix>/lots)/[^/]+$"),
    re.compile(r"(?P<prefix>/plan-items)/[^/]+$"),
)


def request_profile_key(method: str, url: str) -> str:
    path = urlsplit(url).path.rstrip("/")
    for pattern in DETAIL_PATH_PATTERNS:
        match = pattern.search(path)
        if match and path.rsplit("/", 1)[-1] != "filter":
            path = f"{path[: match.start()]}{match.group('prefix')}/{{id}}"
            break
    return f"{method.upper()} {path}"
