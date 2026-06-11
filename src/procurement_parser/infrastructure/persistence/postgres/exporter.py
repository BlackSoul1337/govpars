from __future__ import annotations

import asyncio
import shutil
from pathlib import Path

from sqlalchemy import text

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
    "application_start_at",
    "application_end_at",
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


class PostgresCsvExporter:
    def __init__(
        self,
        database: Database,
        *,
        delimiter: str = ",",
    ) -> None:
        if delimiter not in {",", ";"}:
            raise ValueError("CSV delimiter must be comma or semicolon")
        self.database = database
        self.delimiter = delimiter

    async def export(
        self,
        view_name: str,
        destination: Path,
        *,
        source: str | None = None,
    ) -> int:
        relation = ALLOWED_EXPORTS.get(view_name)
        if relation is None:
            raise ValueError(f"Unsupported export: {view_name}")
        if source is not None and source not in EXPORT_SOURCES:
            raise ValueError(f"Unsupported source: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)

        where_clause = ""
        if source:
            source_column = EXPORT_SOURCE_COLUMNS[view_name]
            where_clause = f' WHERE "{source_column}" = \'{source}\''
        columns = ", ".join(f'"{column}"' for column in EXPORT_COLUMNS[view_name])
        query = f'SELECT {columns} FROM "{relation}"{where_clause}'
        temporary = destination.with_suffix(f"{destination.suffix}.tmp")
        async with self.database.engine.connect() as connection:
            count = int(
                (
                    await connection.execute(
                        text(f'SELECT count(*) FROM "{relation}"{where_clause}')
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
        await asyncio.to_thread(_write_utf8_bom, temporary, destination)
        return count

    async def export_all(self, destination: Path) -> dict[str, int]:
        await asyncio.to_thread(destination.mkdir, parents=True, exist_ok=True)
        results: dict[str, int] = {}
        for dataset in ALLOWED_EXPORTS:
            results[dataset] = await self.export(
                dataset,
                destination / f"{dataset}.csv",
            )
        return results

    async def export_split(
        self,
        view_name: str,
        destination: Path,
    ) -> dict[str, int]:
        await asyncio.to_thread(destination.mkdir, parents=True, exist_ok=True)
        results: dict[str, int] = {}
        for source in EXPORT_SOURCES:
            source_slug = source.replace("-", "_")
            filename = f"{view_name}_{source_slug}.csv"
            results[source] = await self.export(
                view_name,
                destination / filename,
                source=source,
            )
        return results

    async def export_all_split(self, destination: Path) -> dict[str, int]:
        await asyncio.to_thread(destination.mkdir, parents=True, exist_ok=True)
        results: dict[str, int] = {}
        for dataset in ALLOWED_EXPORTS:
            source_results = await self.export_split(dataset, destination)
            for source, count in source_results.items():
                results[f"{dataset}:{source}"] = count
        return results


def _write_utf8_bom(source: Path, destination: Path) -> None:
    with source.open("rb") as source_file, destination.open("wb") as destination_file:
        destination_file.write(b"\xef\xbb\xbf")
        shutil.copyfileobj(source_file, destination_file)
    source.unlink()
