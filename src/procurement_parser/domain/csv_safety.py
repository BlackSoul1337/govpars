from __future__ import annotations

import re

_NEGATIVE_NUMBER = re.compile(r"^-\d+(?:[.,]\d+)?$")


def is_spreadsheet_formula(value: str) -> bool:
    candidate = value.lstrip(" \t\r\n")
    if not candidate:
        return False
    if candidate[0] in {"=", "+", "@"}:
        return True
    return candidate[0] == "-" and not _NEGATIVE_NUMBER.fullmatch(candidate)


def escape_spreadsheet_formula(value: str) -> str:
    return f"'{value}" if is_spreadsheet_formula(value) else value
