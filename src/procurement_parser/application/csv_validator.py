from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Any

DATASET_IDENTITY_COLUMNS = {
    "lots": ("source", "source_entity_id"),
    "notices": ("source", "source_entity_id"),
    "plan_items": ("source", "source_entity_id"),
    "organizations": ("source", "source_entity_id"),
    "relations": (
        "parent_source",
        "parent_type",
        "parent_id",
        "relation_type",
        "child_source",
        "child_type",
        "child_id",
    ),
}


def validate_csv(path: Path, *, dataset: str | None = None) -> dict[str, Any]:
    raw_prefix = path.read_bytes()[:3]
    has_bom = raw_prefix == b"\xef\xbb\xbf"
    replacement_characters = 0
    row_count = 0
    empty_identity_rows = 0
    identities: Counter[tuple[str, ...]] = Counter()
    inferred_dataset = dataset or _dataset_from_filename(path)
    identity_columns = DATASET_IDENTITY_COLUMNS.get(inferred_dataset, ())

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        first_line = handle.readline()
        delimiter = ";" if first_line.count(";") > first_line.count(",") else ","
        handle.seek(0)
        reader = csv.DictReader(handle, delimiter=delimiter)
        headers = reader.fieldnames or []
        for row in reader:
            row_count += 1
            replacement_characters += sum(
                value.count("\ufffd")
                for value in row.values()
                if value
            )
            if identity_columns:
                identity = tuple((row.get(column) or "").strip() for column in identity_columns)
                if any(not value for value in identity):
                    empty_identity_rows += 1
                else:
                    identities[identity] += 1

    duplicate_identity_rows = sum(
        count - 1
        for count in identities.values()
        if count > 1
    )
    errors = []
    if not has_bom:
        errors.append("missing UTF-8 BOM")
    if not headers:
        errors.append("missing CSV header")
    if replacement_characters:
        errors.append("contains Unicode replacement characters")
    if empty_identity_rows:
        errors.append("contains rows with incomplete identity")
    if duplicate_identity_rows:
        errors.append("contains duplicate identity rows")
    return {
        "path": str(path),
        "dataset": inferred_dataset,
        "rows": row_count,
        "columns": len(headers),
        "delimiter": delimiter,
        "utf8_bom": has_bom,
        "replacement_characters": replacement_characters,
        "empty_identity_rows": empty_identity_rows,
        "duplicate_identity_rows": duplicate_identity_rows,
        "valid": not errors,
        "errors": errors,
    }


def validate_export_directory(path: Path) -> dict[str, Any]:
    files = sorted(path.glob("*.csv"))
    results = [validate_csv(file) for file in files]
    split_mismatches = _split_mismatches(results)
    return {
        "directory": str(path),
        "files": results,
        "split_mismatches": split_mismatches,
        "valid": bool(files)
        and all(result["valid"] for result in results)
        and not split_mismatches,
    }


def _dataset_from_filename(path: Path) -> str:
    stem = path.stem
    for suffix in ("_eep_mitwork", "_zakup_sk"):
        if stem.endswith(suffix):
            return stem.removesuffix(suffix)
    return stem


def _split_mismatches(results: list[dict[str, Any]]) -> list[dict[str, int | str]]:
    by_name = {Path(result["path"]).stem: result for result in results}
    mismatches = []
    for dataset in DATASET_IDENTITY_COLUMNS:
        combined = by_name.get(dataset)
        eep = by_name.get(f"{dataset}_eep_mitwork")
        zakup = by_name.get(f"{dataset}_zakup_sk")
        if not combined or not eep or not zakup:
            continue
        split_total = int(eep["rows"]) + int(zakup["rows"])
        if int(combined["rows"]) != split_total:
            mismatches.append(
                {
                    "dataset": dataset,
                    "combined_rows": int(combined["rows"]),
                    "split_rows": split_total,
                }
            )
    return mismatches
