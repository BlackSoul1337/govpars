import json
import logging

import structlog

from procurement_parser.observability import configure_logging


def test_stdlib_exception_is_written_as_one_valid_json_line(tmp_path) -> None:
    configure_logging(
        log_to_file=True,
        log_dir=str(tmp_path),
        log_filename="test.jsonl",
    )

    try:
        raise RuntimeError("connection failed")
    except RuntimeError:
        logging.getLogger("test.stdlib").exception("worker failed")

    lines = (tmp_path / "test.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "worker failed"
    assert payload["level"] == "error"
    assert payload["logger"] == "test.stdlib"
    assert "RuntimeError: connection failed" in payload["exception"]


def test_structlog_event_is_not_double_encoded(tmp_path) -> None:
    configure_logging(
        log_to_file=True,
        log_dir=str(tmp_path),
        log_filename="test.jsonl",
    )

    structlog.get_logger().info("task_complete", source="eep-mitwork")

    lines = (tmp_path / "test.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "task_complete"
    assert payload["source"] == "eep-mitwork"


def test_json_message_is_redacted(tmp_path) -> None:
    configure_logging(
        log_to_file=True,
        log_dir=str(tmp_path),
        log_filename="test.jsonl",
    )

    logging.getLogger("test.json").info(
        '{"event":"session_created","captcha_token":"secret"}'
    )

    payload = json.loads(
        (tmp_path / "test.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    assert payload["event"] == "session_created"
    assert payload["captcha_token"] == "[REDACTED]"
