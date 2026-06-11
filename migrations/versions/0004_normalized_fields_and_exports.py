"""Add high-value normalized fields and complete export views.

Revision ID: 0004
Revises: 0003
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


PROCUREMENT_COLUMNS = {
    "additional_characteristics_ru": "TEXT",
    "additional_characteristics_kk": "TEXT",
    "oktru_code": "VARCHAR(128)",
    "oktru_category_ru": "TEXT",
    "oktru_category_kk": "TEXT",
    "plan_row_number": "VARCHAR(256)",
    "priority": "TEXT",
    "procurement_year": "INTEGER",
    "procurement_month": "VARCHAR(64)",
    "plan_item_type": "TEXT",
    "delivery_conditions_ru": "TEXT",
    "delivery_conditions_kk": "TEXT",
    "venue_ru": "TEXT",
    "venue_kk": "TEXT",
    "contact_email": "VARCHAR(320)",
    "contact_phone": "VARCHAR(128)",
    "contact_extension": "VARCHAR(64)",
}

PROCUREMENT_SELECT = """
    se.source, se.source_entity_id, se.business_number, se.canonical_url,
    e.title_ru, e.title_kk, e.description_ru, e.description_kk,
    e.additional_characteristics_ru, e.additional_characteristics_kk,
    e.status, e.procurement_method, e.tru_code,
    e.oktru_code, e.oktru_category_ru, e.oktru_category_kk,
    e.plan_row_number, e.priority, e.procurement_year,
    e.procurement_month, e.plan_item_type,
    e.quantity, e.unit, e.unit_price, e.total_amount, e.currency,
    e.published_at, e.application_start_at, e.application_end_at,
    e.delivery_terms_ru, e.delivery_terms_kk,
    e.delivery_conditions_ru, e.delivery_conditions_kk,
    e.venue_ru, e.venue_kk,
    e.contact_email, e.contact_phone, e.contact_extension,
    e.source_payload, e.fetched_at, e.updated_at
"""


def _recreate_views() -> None:
    for name in (
        "export_lots",
        "export_procurement_notices",
        "export_plan_items",
        "export_organizations",
        "export_entity_relations",
        "export_delivery_places",
        "export_payment_terms",
        "export_documents",
    ):
        op.execute(f'DROP VIEW IF EXISTS "{name}"')

    op.execute(
        f"""
        CREATE VIEW export_lots AS
        SELECT {PROCUREMENT_SELECT}
        FROM lots e JOIN source_entities se ON se.id = e.source_entity_fk
        """
    )
    op.execute(
        f"""
        CREATE VIEW export_procurement_notices AS
        SELECT {PROCUREMENT_SELECT}
        FROM procurement_notices e
        JOIN source_entities se ON se.id = e.source_entity_fk
        """
    )
    op.execute(
        f"""
        CREATE VIEW export_plan_items AS
        SELECT {PROCUREMENT_SELECT}
        FROM plan_items e JOIN source_entities se ON se.id = e.source_entity_fk
        """
    )
    op.execute(
        """
        CREATE VIEW export_organizations AS
        SELECT se.source, se.source_entity_id, se.business_number, se.canonical_url,
               o.name_ru, o.name_kk, o.bin, o.address, o.phone, o.email,
               o.source_payload, o.fetched_at, o.updated_at
        FROM organizations o JOIN source_entities se ON se.id = o.source_entity_fk
        """
    )
    op.execute(
        """
        CREATE VIEW export_entity_relations AS
        SELECT p.source AS parent_source, p.entity_type AS parent_type,
               p.source_entity_id AS parent_id, r.relation_type,
               c.source AS child_source, c.entity_type AS child_type,
               c.source_entity_id AS child_id, r.source_payload,
               r.first_seen_at, r.last_seen_at
        FROM entity_relations r
        JOIN source_entities p ON p.id = r.parent_source_entity_fk
        JOIN source_entities c ON c.id = r.child_source_entity_fk
        """
    )
    op.execute(
        """
        CREATE VIEW export_delivery_places AS
        SELECT se.source, se.entity_type, se.source_entity_id,
               dp.source_row_id, dp.country, dp.address, dp.quantity,
               dp.incoterms, dp.source_payload
        FROM delivery_places dp
        JOIN source_entities se ON se.id = dp.owner_source_entity_fk
        """
    )
    op.execute(
        """
        CREATE VIEW export_payment_terms AS
        SELECT se.source, se.entity_type, se.source_entity_id,
               pt.prepayment_percent, pt.interim_percent,
               pt.final_percent, pt.raw_text
        FROM payment_terms pt
        JOIN source_entities se ON se.id = pt.owner_source_entity_fk
        """
    )
    op.execute(
        """
        CREATE VIEW export_documents AS
        SELECT se.source, se.entity_type, se.source_entity_id,
               dm.source_document_id, dm.category, dm.filename, dm.extension,
               dm.url, dm.size_bytes, dm.uploaded_at, dm.document_hash,
               dm.declared_content_type, dm.inferred_content_type,
               dm.response_content_type, dm.source_payload
        FROM document_metadata dm
        JOIN source_entities se ON se.id = dm.owner_source_entity_fk
        """
    )


def upgrade() -> None:
    for table in ("lots", "procurement_notices", "plan_items"):
        for column, sql_type in PROCUREMENT_COLUMNS.items():
            op.execute(
                f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{column}" {sql_type}'
            )

    for table in ("lots", "procurement_notices", "plan_items"):
        op.execute(
            f"""
            UPDATE {table} e
            SET additional_characteristics_ru =
                    COALESCE(e.source_payload->>'addAttributeRu',
                             e.source_payload->'fields'
                               ->>'Дополнительная характеристика на русском языке'),
                additional_characteristics_kk =
                    COALESCE(e.source_payload->>'addAttributeKk',
                             e.source_payload->'fields'
                               ->>'Дополнительная характеристика на государственном языке'),
                oktru_code = e.source_payload->>'oktruFullCode',
                oktru_category_ru = e.source_payload->>'oktruCategoryNameRu',
                oktru_category_kk = e.source_payload->>'oktruCategoryNameKk',
                plan_row_number = e.source_payload->>'lotRowNumber',
                priority = e.source_payload->>'tenderPriority',
                procurement_year = CASE
                    WHEN e.source_payload->'fields'->>'Год' ~ '^\\d{{4}}$'
                    THEN (e.source_payload->'fields'->>'Год')::integer
                    ELSE NULL
                END,
                procurement_month = e.source_payload->'fields'->>'Месяц',
                plan_item_type = e.source_payload->'fields'->>'Тип пункта плана',
                delivery_conditions_ru = e.source_payload->>'incoterms',
                delivery_conditions_kk = e.source_payload->>'incoterms',
                venue_ru = COALESCE(e.source_payload->>'tenderLocationRu',
                                    e.source_payload->>'tenderLocation'),
                venue_kk = e.source_payload->>'tenderLocationKk',
                contact_email = e.source_payload->>'email',
                contact_phone = e.source_payload->>'phone',
                contact_extension = e.source_payload->>'extensionNumber'
            """
        )

    for table in ("lots", "plan_items"):
        op.execute(
            f"""
            UPDATE {table} e
            SET unit = substring(
                e.source_payload->'fields'->>'Расчет полной стоимости'
                FROM 'x[[:space:]]*[0-9[:space:],.]+[[:space:]]+(.+?)[[:space:]]*='
            )
            FROM source_entities se
            WHERE se.id = e.source_entity_fk
              AND se.source = 'eep-mitwork'
              AND e.unit IS NULL
            """
        )

    op.execute(
        """
        UPDATE lots e
        SET delivery_terms_ru = COALESCE(
                e.delivery_terms_ru,
                CASE
                    WHEN e.source_payload->'schedule'->>'count' IS NOT NULL
                    THEN concat(
                        'В течение ',
                        e.source_payload->'schedule'->>'count',
                        CASE
                            WHEN e.source_payload->'schedule'->'dayType'->>'ru'
                                 IS NOT NULL
                            THEN concat(
                                ' ',
                                lower(e.source_payload->'schedule'->'dayType'->>'ru')
                            )
                            ELSE ''
                        END,
                        ' дней'
                    )
                    WHEN e.source_payload->'schedule'->>'monthTo' IS NOT NULL
                    THEN concat(
                        'До ',
                        e.source_payload->'schedule'->>'monthTo'
                    )
                END
            ),
            delivery_terms_kk = COALESCE(
                e.delivery_terms_kk,
                CASE
                    WHEN e.source_payload->'schedule'->>'count' IS NOT NULL
                    THEN concat(
                        e.source_payload->'schedule'->>'count',
                        CASE
                            WHEN e.source_payload->'schedule'->'dayType'->>'kk'
                                 IS NOT NULL
                            THEN concat(
                                ' ',
                                lower(e.source_payload->'schedule'->'dayType'->>'kk')
                            )
                            ELSE ''
                        END,
                        ' күн ішінде'
                    )
                    WHEN e.source_payload->'schedule'->>'monthTo' IS NOT NULL
                    THEN concat(
                        e.source_payload->'schedule'->>'monthTo',
                        ' дейін'
                    )
                END
            )
        FROM source_entities se
        WHERE se.id = e.source_entity_fk
          AND se.source = 'zakup-sk'
        """
    )

    op.execute(
        """
        DELETE FROM entity_relations r
        USING source_entities p
        WHERE p.id = r.parent_source_entity_fk
          AND p.source = 'eep-mitwork'
          AND p.entity_type = 'organization'
          AND r.relation_type = 'customer'
        """
    )
    _recreate_views()


def downgrade() -> None:
    for name in (
        "export_lots",
        "export_procurement_notices",
        "export_plan_items",
        "export_organizations",
        "export_entity_relations",
        "export_delivery_places",
        "export_payment_terms",
        "export_documents",
    ):
        op.execute(f'DROP VIEW IF EXISTS "{name}"')
    for table in ("lots", "procurement_notices", "plan_items"):
        for column in reversed(PROCUREMENT_COLUMNS):
            op.execute(f'ALTER TABLE "{table}" DROP COLUMN IF EXISTS "{column}"')
