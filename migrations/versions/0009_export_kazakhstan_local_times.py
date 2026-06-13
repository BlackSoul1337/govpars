"""Add Kazakhstan-local timestamps to procurement export views.

Revision ID: 0009
Revises: 0008
"""

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


PROCUREMENT_SELECT_UTC = """
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

PROCUREMENT_SELECT_LOCAL = """
    se.source, se.source_entity_id, se.business_number, se.canonical_url,
    e.title_ru, e.title_kk, e.description_ru, e.description_kk,
    e.additional_characteristics_ru, e.additional_characteristics_kk,
    e.status, e.procurement_method, e.tru_code,
    e.oktru_code, e.oktru_category_ru, e.oktru_category_kk,
    e.plan_row_number, e.priority, e.procurement_year,
    e.procurement_month, e.plan_item_type,
    e.quantity, e.unit, e.unit_price, e.total_amount, e.currency,
    e.published_at,
    e.published_at AT TIME ZONE 'Asia/Almaty' AS published_at_local,
    e.application_start_at,
    e.application_start_at AT TIME ZONE 'Asia/Almaty'
        AS application_start_at_local,
    e.application_end_at,
    e.application_end_at AT TIME ZONE 'Asia/Almaty'
        AS application_end_at_local,
    'Asia/Almaty'::text AS source_timezone,
    e.delivery_terms_ru, e.delivery_terms_kk,
    e.delivery_conditions_ru, e.delivery_conditions_kk,
    e.venue_ru, e.venue_kk,
    e.contact_email, e.contact_phone, e.contact_extension,
    e.source_payload, e.fetched_at, e.updated_at
"""

PROCUREMENT_VIEWS = {
    "export_lots": "lots",
    "export_procurement_notices": "procurement_notices",
    "export_plan_items": "plan_items",
}


def _recreate_views(select_columns: str) -> None:
    for view_name in PROCUREMENT_VIEWS:
        op.execute(f'DROP VIEW IF EXISTS "{view_name}"')
    for view_name, table_name in PROCUREMENT_VIEWS.items():
        op.execute(
            f"""
            CREATE VIEW "{view_name}" AS
            SELECT {select_columns}
            FROM "{table_name}" e
            JOIN source_entities se ON se.id = e.source_entity_fk
            """
        )


def upgrade() -> None:
    _recreate_views(PROCUREMENT_SELECT_LOCAL)


def downgrade() -> None:
    _recreate_views(PROCUREMENT_SELECT_UTC)
