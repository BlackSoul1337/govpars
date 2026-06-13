from __future__ import annotations

import asyncio
import csv
import shutil
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from procurement_parser.domain.csv_safety import escape_spreadsheet_formula
from procurement_parser.infrastructure.persistence.postgres.database import Database

ALLOWED_EXPORTS = {
    "lots": "export_lots",
    "notices": "export_procurement_notices",
    "plan_items": "export_plan_items",
    "organizations": "export_organizations",
    "relations": "export_entity_relations",
    "delivery_places": "export_delivery_places",
    "payment_terms": "export_payment_terms",
    "documents": "export_documents",
}
PROCUREMENT_EXPORT_COLUMNS = (
    "source",
    "source_entity_id",
    "business_number",
    "canonical_url",
    "title_ru",
    "title_kk",
    "description_ru",
    "description_kk",
    "additional_characteristics_ru",
    "additional_characteristics_kk",
    "status",
    "procurement_method",
    "tru_code",
    "oktru_code",
    "oktru_category_ru",
    "oktru_category_kk",
    "plan_row_number",
    "priority",
    "procurement_year",
    "procurement_month",
    "plan_item_type",
    "quantity",
    "unit",
    "unit_price",
    "total_amount",
    "currency",
    "published_at",
    "published_at_local",
    "application_start_at",
    "application_start_at_local",
    "application_end_at",
    "application_end_at_local",
    "source_timezone",
    "delivery_terms_ru",
    "delivery_terms_kk",
    "delivery_conditions_ru",
    "delivery_conditions_kk",
    "venue_ru",
    "venue_kk",
    "contact_email",
    "contact_phone",
    "contact_extension",
    "source_payload",
    "fetched_at",
    "updated_at",
)
EXPORT_COLUMNS = {
    "lots": PROCUREMENT_EXPORT_COLUMNS,
    "notices": PROCUREMENT_EXPORT_COLUMNS,
    "plan_items": PROCUREMENT_EXPORT_COLUMNS,
    "organizations": (
        "source",
        "source_entity_id",
        "business_number",
        "canonical_url",
        "name_ru",
        "name_kk",
        "bin",
        "address",
        "phone",
        "email",
        "source_payload",
        "fetched_at",
        "updated_at",
    ),
    "relations": (
        "parent_source",
        "parent_type",
        "parent_id",
        "relation_type",
        "child_source",
        "child_type",
        "child_id",
        "source_payload",
        "first_seen_at",
        "last_seen_at",
    ),
    "delivery_places": (
        "source",
        "entity_type",
        "source_entity_id",
        "source_row_id",
        "country",
        "address",
        "quantity",
        "incoterms",
        "source_payload",
    ),
    "payment_terms": (
        "source",
        "entity_type",
        "source_entity_id",
        "prepayment_percent",
        "interim_percent",
        "final_percent",
        "raw_text",
    ),
    "documents": (
        "source",
        "entity_type",
        "source_entity_id",
        "source_document_id",
        "category",
        "filename",
        "extension",
        "url",
        "size_bytes",
        "uploaded_at",
        "document_hash",
        "declared_content_type",
        "inferred_content_type",
        "response_content_type",
        "source_payload",
    ),
}
EXPORT_SOURCE_COLUMNS = {
    "lots": "source",
    "notices": "source",
    "plan_items": "source",
    "organizations": "source",
    "relations": "parent_source",
    "delivery_places": "source",
    "payment_terms": "source",
    "documents": "source",
}
EXPORT_SOURCES = ("eep-mitwork", "zakup-sk")
type ExportRequest = tuple[str, str, Path, str | None]


class PostgresCsvExporter:
    def __init__(
        self,
        database: Database,
        *,
        delimiter: str = ",",
        excel_safe: bool = True,
    ) -> None:
        if delimiter not in {",", ";"}:
            raise ValueError("CSV delimiter must be comma or semicolon")
        self.database = database
        self.delimiter = delimiter
        self.excel_safe = excel_safe

    async def export(
        self,
        view_name: str,
        destination: Path,
        *,
        source: str | None = None,
    ) -> int:
        results = await self._export_requests(
            [("result", view_name, destination, source)]
        )
        return results["result"]

    async def _export_requests(
        self,
        requests: list[ExportRequest],
    ) -> dict[str, int]:
        for _, view_name, destination, source in requests:
            self._validate_request(view_name, source)
            await asyncio.to_thread(
                destination.parent.mkdir,
                parents=True,
                exist_ok=True,
            )

        copied: list[tuple[Path, Path]] = []
        results: dict[str, int] = {}
        try:
            async with self.database.engine.connect() as connection:
                async with connection.begin():
                    await connection.execute(
                        text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    )
                    for key, view_name, destination, source in requests:
                        count, temporary = await self._copy_export(
                            connection,
                            view_name,
                            destination,
                            source=source,
                        )
                        copied.append((temporary, destination))
                        results[key] = count
            for temporary, destination in copied:
                await asyncio.to_thread(
                    _write_csv,
                    temporary,
                    destination,
                    delimiter=self.delimiter,
                    excel_safe=self.excel_safe,
                )
            return results
        finally:
            for temporary, _ in copied:
                if temporary.exists():
                    await asyncio.to_thread(temporary.unlink)

    @staticmethod
    def _validate_request(view_name: str, source: str | None) -> None:
        relation = ALLOWED_EXPORTS.get(view_name)
        if relation is None:
            raise ValueError(f"Unsupported export: {view_name}")
        if source is not None and source not in EXPORT_SOURCES:
            raise ValueError(f"Unsupported source: {source}")

    async def _copy_export(
        self,
        connection: AsyncConnection,
        view_name: str,
        destination: Path,
        *,
        source: str | None,
    ) -> tuple[int, Path]:
        relation = ALLOWED_EXPORTS[view_name]
        where_clause = ""
        if source:
            source_column = EXPORT_SOURCE_COLUMNS[view_name]
            where_clause = f' WHERE "{source_column}" = \'{source}\''
        columns = ", ".join(f'"{column}"' for column in EXPORT_COLUMNS[view_name])
        query = f'SELECT {columns} FROM "{relation}"{where_clause}'
        temporary = destination.with_name(
            f".{destination.name}.{uuid4().hex}.copy.tmp"
        )
        count = int(
            (
                await connection.execute(
                    text(
                        f'SELECT count(*) FROM "{relation}"'
                        f"{where_clause}"
                    )
                )
            ).scalar_one()
        )
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        await driver.copy_from_query(
            query,
            output=str(temporary),
            format="csv",
            header=True,
            delimiter=self.delimiter,
        )
        return count, temporary

    async def export_all(self, destination: Path) -> dict[str, int]:
        return await self._export_requests(
            [
                (dataset, dataset, destination / f"{dataset}.csv", None)
                for dataset in ALLOWED_EXPORTS
            ]
        )

    async def export_split(
        self,
        view_name: str,
        destination: Path,
    ) -> dict[str, int]:
        return await self._export_requests(
            [
                (
                    source,
                    view_name,
                    destination / f"{view_name}_{source.replace('-', '_')}.csv",
                    source,
                )
                for source in EXPORT_SOURCES
            ]
        )

    async def export_all_split(self, destination: Path) -> dict[str, int]:
        return await self._export_requests(
            [
                (
                    f"{dataset}:{source}",
                    dataset,
                    destination
                    / f"{dataset}_{source.replace('-', '_')}.csv",
                    source,
                )
                for dataset in ALLOWED_EXPORTS
                for source in EXPORT_SOURCES
            ]
        )

    async def export_all_both(self, destination: Path) -> dict[str, int]:
        return await self._export_requests(
            [
                (
                    f"combined:{dataset}",
                    dataset,
                    destination / f"{dataset}.csv",
                    None,
                )
                for dataset in ALLOWED_EXPORTS
            ]
            + [
                (
                    f"split:{dataset}:{source}",
                    dataset,
                    destination
                    / f"{dataset}_{source.replace('-', '_')}.csv",
                    source,
                )
                for dataset in ALLOWED_EXPORTS
                for source in EXPORT_SOURCES
            ]
        )

    async def export_both(
        self,
        view_name: str,
        destination: Path,
        split_destination: Path,
    ) -> dict[str, int]:
        return await self._export_requests(
            [
                ("combined", view_name, destination, None),
                *[
                    (
                        f"split:{source}",
                        view_name,
                        split_destination
                        / f"{view_name}_{source.replace('-', '_')}.csv",
                        source,
                    )
                    for source in EXPORT_SOURCES
                ],
            ]
        )


def _write_csv(
    source: Path,
    destination: Path,
    *,
    delimiter: str,
    excel_safe: bool,
) -> None:
    output = destination.with_name(
        f".{destination.name}.{uuid4().hex}.write.tmp"
    )
    try:
        if excel_safe:
            with (
                source.open("r", encoding="utf-8", newline="") as source_file,
                output.open("w", encoding="utf-8-sig", newline="") as output_file,
            ):
                reader = csv.reader(source_file, delimiter=delimiter)
                writer = csv.writer(output_file, delimiter=delimiter)
                for row in reader:
                    writer.writerow(
                        [escape_spreadsheet_formula(value) for value in row]
                    )
        else:
            with source.open("rb") as source_file, output.open("wb") as output_file:
                output_file.write(b"\xef\xbb\xbf")
                shutil.copyfileobj(source_file, output_file)
        output.replace(destination)
    finally:
        source.unlink(missing_ok=True)
        output.unlink(missing_ok=True)
