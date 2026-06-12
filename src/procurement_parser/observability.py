from __future__ import annotations

import json
import logging
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import orjson
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


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        try:
            payload = json.loads(message)
        except (TypeError, ValueError):
            payload = {
                "event": message,
                "level": record.levelname.lower(),
                "logger": record.name,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            if record.exc_info:
                payload["exception"] = self.formatException(record.exc_info)
            payload = _redact(None, record.levelname.lower(), payload)
            return orjson.dumps(payload).decode("utf-8")
        if isinstance(payload, dict):
            payload = _redact(None, record.levelname.lower(), payload)
            return orjson.dumps(payload).decode("utf-8")
        return orjson.dumps(
            {
                "event": payload,
                "level": record.levelname.lower(),
                "logger": record.name,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ).decode("utf-8")


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
    formatter = JsonLineFormatter()
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
