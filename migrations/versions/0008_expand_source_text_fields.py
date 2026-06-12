"""Expand source-controlled free-text fields.

Revision ID: 0008
Revises: 0007
"""

import sqlalchemy as sa
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


PROCUREMENT_TABLES = ("lots", "procurement_notices", "plan_items")
DEPENDENT_VIEWS = (
    "export_lots",
    "export_procurement_notices",
    "export_plan_items",
    "export_organizations",
    "export_delivery_places",
)


def _replace_column_types(*, expand: bool) -> None:
    connection = op.get_bind()
    view_definitions = {
        view_name: connection.execute(
            sa.text("SELECT pg_get_viewdef(:view_name, true)"),
            {"view_name": view_name},
        ).scalar_one()
        for view_name in DEPENDENT_VIEWS
    }
    for view_name in DEPENDENT_VIEWS:
        op.execute(sa.text(f'DROP VIEW "{view_name}"'))

    text_type = sa.Text()
    phone_type = sa.String(length=128)
    extension_type = sa.String(length=64)
    if expand:
        contact_phone_target = text_type
        contact_extension_target = text_type
        organization_phone_target = text_type
        incoterms_target = text_type
    else:
        contact_phone_target = phone_type
        contact_extension_target = extension_type
        organization_phone_target = phone_type
        incoterms_target = extension_type

    for table_name in PROCUREMENT_TABLES:
        op.alter_column(
            table_name,
            "contact_phone",
            existing_type=phone_type if expand else text_type,
            type_=contact_phone_target,
            existing_nullable=True,
        )
        op.alter_column(
            table_name,
            "contact_extension",
            existing_type=extension_type if expand else text_type,
            type_=contact_extension_target,
            existing_nullable=True,
        )
    op.alter_column(
        "organizations",
        "phone",
        existing_type=phone_type if expand else text_type,
        type_=organization_phone_target,
        existing_nullable=True,
    )
    op.alter_column(
        "delivery_places",
        "incoterms",
        existing_type=extension_type if expand else text_type,
        type_=incoterms_target,
        existing_nullable=True,
    )

    for view_name, definition in view_definitions.items():
        op.execute(sa.text(f'CREATE VIEW "{view_name}" AS {definition}'))


def upgrade() -> None:
    _replace_column_types(expand=True)


def downgrade() -> None:
    _replace_column_types(expand=False)
