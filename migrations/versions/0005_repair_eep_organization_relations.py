"""Repair EEP customer and organizer relation semantics.

Revision ID: 0005
Revises: 0004
"""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


PARENTS_SQL = """
    SELECT se.id AS parent_fk,
           e.source_payload->'fields'->>'Заказчик' AS customer_name,
           e.source_payload->'fields'->>'Организатор' AS organizer_name
    FROM lots e
    JOIN source_entities se ON se.id = e.source_entity_fk
    WHERE se.source = 'eep-mitwork'
    UNION ALL
    SELECT se.id,
           e.source_payload->'fields'->>'Заказчик',
           e.source_payload->'fields'->>'Организатор'
    FROM procurement_notices e
    JOIN source_entities se ON se.id = e.source_entity_fk
    WHERE se.source = 'eep-mitwork'
    UNION ALL
    SELECT se.id,
           e.source_payload->'fields'->>'Заказчик',
           e.source_payload->'fields'->>'Организатор'
    FROM plan_items e
    JOIN source_entities se ON se.id = e.source_entity_fk
    WHERE se.source = 'eep-mitwork'
"""


def upgrade() -> None:
    op.execute(
        f"""
        WITH parents AS ({PARENTS_SQL}),
        candidates AS (
            SELECT DISTINCT r.parent_source_entity_fk,
                            r.child_source_entity_fk
            FROM entity_relations r
            JOIN parents p ON p.parent_fk = r.parent_source_entity_fk
            JOIN organizations o ON o.source_entity_fk = r.child_source_entity_fk
            WHERE r.relation_type = 'customer'
              AND p.organizer_name IS NOT NULL
              AND o.name_ru = p.organizer_name
        )
        INSERT INTO entity_relations (
            relation_type,
            parent_source_entity_fk,
            child_source_entity_fk,
            source_payload
        )
        SELECT 'organizer', parent_source_entity_fk, child_source_entity_fk, '{{}}'::jsonb
        FROM candidates
        ON CONFLICT ON CONSTRAINT uq_entity_relation DO NOTHING
        """
    )
    op.execute(
        f"""
        WITH parents AS ({PARENTS_SQL})
        DELETE FROM entity_relations r
        USING parents p, organizations o
        WHERE r.parent_source_entity_fk = p.parent_fk
          AND r.child_source_entity_fk = o.source_entity_fk
          AND r.relation_type = 'customer'
          AND (
              p.customer_name IS NULL
              OR o.name_ru IS DISTINCT FROM p.customer_name
          )
        """
    )


def downgrade() -> None:
    # The old relation labels were incorrect and cannot be restored safely.
    pass
