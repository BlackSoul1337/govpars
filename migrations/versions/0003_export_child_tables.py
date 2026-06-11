"""Add normalized child-table export views.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


EXPORT_VIEWS = {
    "export_delivery_places": """
        SELECT se.source, se.entity_type, se.source_entity_id,
               dp.source_row_id, dp.country, dp.address, dp.quantity,
               dp.incoterms
        FROM delivery_places dp
        JOIN source_entities se ON se.id = dp.owner_source_entity_fk
    """,
    "export_payment_terms": """
        SELECT se.source, se.entity_type, se.source_entity_id,
               pt.prepayment_percent, pt.interim_percent,
               pt.final_percent, pt.raw_text
        FROM payment_terms pt
        JOIN source_entities se ON se.id = pt.owner_source_entity_fk
    """,
    "export_documents": """
        SELECT se.source, se.entity_type, se.source_entity_id,
               dm.source_document_id, dm.category, dm.filename, dm.extension,
               dm.url, dm.size_bytes, dm.uploaded_at, dm.document_hash,
               dm.declared_content_type, dm.inferred_content_type,
               dm.response_content_type
        FROM document_metadata dm
        JOIN source_entities se ON se.id = dm.owner_source_entity_fk
    """,
}


def upgrade() -> None:
    for name, query in EXPORT_VIEWS.items():
        op.execute(f'CREATE VIEW "{name}" AS {query}')


def downgrade() -> None:
    for name in reversed(EXPORT_VIEWS):
        op.execute(f'DROP VIEW IF EXISTS "{name}"')
