from __future__ import annotations

import logging
import sys
from collections.abc import Mapping
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import structlog

REDACTED_KEYS = {
    "authorization",
    "cookie",
    "cookies",
    "captcha_token",
    "proxy_password",
    "twocaptcha_api_key",
    "x-api-key",
}


def _redact_value(key: str, value: Any) -> Any:
    if key.lower() in REDACTED_KEYS:
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {
            nested_key: _redact_value(str(nested_key), nested_value)
            for nested_key, nested_value in value.items()
        }
    if isinstance(value, list):
        return [_redact_value("", item) for item in value]
    return value


def _redact(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    for key in list(event_dict):
        event_dict[key] = _redact_value(key, event_dict[key])
    return event_dict


def configure_logging(
    level: str = "INFO",
    *,
    log_to_file: bool = True,
    log_dir: str = "logs",
    log_filename: str = "procurement-parser.jsonl",
    log_max_bytes: int = 10_485_760,
    log_backup_count: int = 5,
) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    formatter = logging.Formatter("%(message)s")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_to_file:
        directory = Path(log_dir)
        directory.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                directory / log_filename,
                maxBytes=log_max_bytes,
                backupCount=log_backup_count,
                encoding="utf-8",
            )
        )
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(numeric_level)
    for handler in handlers:
        handler.setFormatter(formatter)
        handler.setLevel(numeric_level)
        root.addHandler(handler)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            _redact,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            numeric_level
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger():
    return structlog.get_logger()
