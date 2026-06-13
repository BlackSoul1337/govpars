from pathlib import Path

import pytest

from procurement_parser.application.csv_validator import (
    validate_csv,
    validate_export_directory,
)
from procurement_parser.domain.csv_safety import (
    escape_spreadsheet_formula,
    is_spreadsheet_formula,
)


def _write(path: Path, content: str) -> None:
    path.write_bytes(b"\xef\xbb\xbf" + content.encode("utf-8"))


def test_csv_validator_accepts_unique_utf8_bom_rows(tmp_path) -> None:
    path = tmp_path / "lots.csv"
    _write(path, "source,source_entity_id,title_ru\r\neep-mitwork,1,Тест\r\n")

    result = validate_csv(path)

    assert result["valid"] is True
    assert result["rows"] == 1


def test_csv_validator_rejects_duplicate_identity_and_replacement_character(
    tmp_path,
) -> None:
    path = tmp_path / "lots.csv"
    _write(
        path,
        "source,source_entity_id,title_ru\r\n"
        "eep-mitwork,1,Тест\ufffd\r\n"
        "eep-mitwork,1,Дубль\r\n",
    )

    result = validate_csv(path)

    assert result["valid"] is False
    assert result["duplicate_identity_rows"] == 1
    assert result["replacement_characters"] == 1


def test_directory_validator_compares_combined_and_split_counts(tmp_path) -> None:
    header = "source,source_entity_id\r\n"
    _write(tmp_path / "lots.csv", header + "eep-mitwork,1\r\n")
    _write(tmp_path / "lots_eep_mitwork.csv", header + "eep-mitwork,1\r\n")
    _write(tmp_path / "lots_zakup_sk.csv", header + "zakup-sk,2\r\n")

    result = validate_export_directory(tmp_path)

    assert result["valid"] is False
    assert result["split_mismatches"][0]["dataset"] == "lots"


def test_csv_validator_accepts_excel_semicolon_delimiter(tmp_path) -> None:
    path = tmp_path / "lots.csv"
    _write(
        path,
        "source;source_entity_id;title_ru\r\n"
        'zakup-sk;1;"Строка 1\nСтрока 2"\r\n',
    )

    result = validate_csv(path)

    assert result["valid"] is True
    assert result["rows"] == 1
    assert result["columns"] == 3
    assert result["delimiter"] == ";"


def test_csv_validator_rejects_malformed_row_width(tmp_path) -> None:
    path = tmp_path / "lots.csv"
    _write(
        path,
        "source,source_entity_id,title_ru\r\n"
        "eep-mitwork,1,title,unexpected\r\n",
    )

    result = validate_csv(path)

    assert result["valid"] is False
    assert result["malformed_rows"] == 1


def test_csv_validator_rejects_spreadsheet_formula_like_cells(tmp_path) -> None:
    path = tmp_path / "lots.csv"
    _write(
        path,
        "source,source_entity_id,title_ru\r\n"
        'eep-mitwork,1,"=HYPERLINK(""https://example.test"")"\r\n',
    )

    result = validate_csv(path)

    assert result["valid"] is False
    assert result["formula_like_cells"] == 1


@pytest.mark.parametrize(
    ("value", "formula_like"),
    [
        ("=1+1", True),
        (" +7 (717) 000-00-00", True),
        ("@SUM(A1:A2)", True),
        ("- command", True),
        ("-123.45", False),
        ("ordinary text", False),
    ],
)
def test_spreadsheet_formula_detection(value, formula_like) -> None:
    assert is_spreadsheet_formula(value) is formula_like
    escaped = escape_spreadsheet_formula(value)
    assert escaped.startswith("'") is formula_like
