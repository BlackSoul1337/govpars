"""Initial durable ETL schema.

Revision ID: 0001
Revises:
Create Date: 2026-06-09
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


EXPORT_VIEWS = {
    "export_lots": """
        SELECT se.source, se.source_entity_id, se.business_number, se.canonical_url,
               l.title_ru, l.title_kk, l.description_ru, l.description_kk,
               l.status, l.procurement_method, l.tru_code, l.quantity, l.unit,
               l.unit_price, l.total_amount, l.currency, l.application_start_at,
               l.application_end_at, l.delivery_terms_ru, l.delivery_terms_kk,
               l.fetched_at, l.updated_at
        FROM lots l JOIN source_entities se ON se.id = l.source_entity_fk
    """,
    "export_procurement_notices": """
        SELECT se.source, se.source_entity_id, se.business_number, se.canonical_url,
               n.title_ru, n.title_kk, n.status, n.procurement_method,
               n.published_at, n.application_start_at, n.application_end_at,
               n.fetched_at, n.updated_at
        FROM procurement_notices n
        JOIN source_entities se ON se.id = n.source_entity_fk
    """,
    "export_plan_items": """
        SELECT se.source, se.source_entity_id, se.business_number, se.canonical_url,
               p.title_ru, p.title_kk, p.description_ru, p.description_kk,
               p.status, p.procurement_method, p.tru_code, p.quantity, p.unit,
               p.unit_price, p.total_amount, p.currency, p.delivery_terms_ru,
               p.delivery_terms_kk, p.fetched_at, p.updated_at
        FROM plan_items p JOIN source_entities se ON se.id = p.source_entity_fk
    """,
    "export_organizations": """
        SELECT se.source, se.source_entity_id, se.business_number, se.canonical_url,
               o.name_ru, o.name_kk, o.bin, o.address, o.phone, o.email,
               o.fetched_at, o.updated_at
        FROM organizations o JOIN source_entities se ON se.id = o.source_entity_fk
    """,
    "export_entity_relations": """
        SELECT p.source AS parent_source, p.entity_type AS parent_type,
               p.source_entity_id AS parent_id, r.relation_type,
               c.source AS child_source, c.entity_type AS child_type,
               c.source_entity_id AS child_id, r.first_seen_at, r.last_seen_at
        FROM entity_relations r
        JOIN source_entities p ON p.id = r.parent_source_entity_fk
        JOIN source_entities c ON c.id = r.child_source_entity_fk
    """,
}


def _procurement_columns() -> list[sa.Column]:
    return [
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("title_ru", sa.Text()),
        sa.Column("title_kk", sa.Text()),
        sa.Column("description_ru", sa.Text()),
        sa.Column("description_kk", sa.Text()),
        sa.Column("status", sa.String(length=256)),
        sa.Column("procurement_method", sa.Text()),
        sa.Column("tru_code", sa.String(length=128)),
        sa.Column("quantity", sa.Numeric(24, 6)),
        sa.Column("unit", sa.String(length=128)),
        sa.Column("unit_price", sa.Numeric(24, 6)),
        sa.Column("total_amount", sa.Numeric(24, 6)),
        sa.Column("currency", sa.String(length=16)),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("application_start_at", sa.DateTime(timezone=True)),
        sa.Column("application_end_at", sa.DateTime(timezone=True)),
        sa.Column("delivery_terms_ru", sa.Text()),
        sa.Column("delivery_terms_kk", sa.Text()),
        sa.Column(
            "source_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_entity_fk"),
    ]


def upgrade() -> None:
    op.create_table(
        "source_entities",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("source_entity_id", sa.String(length=128), nullable=False),
        sa.Column("business_number", sa.String(length=256)),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column(
            "summary_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("current_content_hash", sa.String(length=64)),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source",
            "entity_type",
            "source_entity_id",
            name="uq_source_entity_identity",
        ),
    )
    op.create_index(
        "ix_source_entities_last_seen",
        "source_entities",
        ["source", "entity_type", "last_seen_at"],
    )

    op.create_table(
        "crawl_frontier",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("task_type", sa.String(length=32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("lease_owner", sa.String(length=128)),
        sa.Column("leased_until", sa.DateTime(timezone=True)),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_entity_fk",
            "task_type",
            name="uq_frontier_entity_task",
        ),
    )
    op.create_index(
        "ix_frontier_claim",
        "crawl_frontier",
        ["priority", "available_at", "id"],
        postgresql_include=["leased_until", "source_entity_fk"],
    )

    op.create_table(
        "crawl_task_history",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("task_type", sa.String(length=32), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("content_hash", sa.String(length=64)),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_task_history_completed",
        "crawl_task_history",
        ["completed_at"],
    )
    op.create_index(
        "ix_task_history_entity",
        "crawl_task_history",
        ["source_entity_fk"],
    )

    op.create_table(
        "discovery_checkpoints",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("scope", sa.String(length=128), nullable=False),
        sa.Column("next_page", sa.Integer(), nullable=False),
        sa.Column("completed", sa.Boolean(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source",
            "entity_type",
            "scope",
            name="uq_discovery_checkpoint",
        ),
    )

    for table_name in ("lots", "procurement_notices", "plan_items"):
        op.create_table(table_name, *_procurement_columns())

    op.create_table(
        "organizations",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("name_ru", sa.Text()),
        sa.Column("name_kk", sa.Text()),
        sa.Column("bin", sa.String(length=32)),
        sa.Column("address", sa.Text()),
        sa.Column("phone", sa.String(length=128)),
        sa.Column("email", sa.String(length=320)),
        sa.Column(
            "source_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_entity_fk"),
    )
    op.create_index("ix_organizations_bin", "organizations", ["bin"])

    op.create_table(
        "delivery_places",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("owner_source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("source_row_id", sa.String(length=128)),
        sa.Column("country", sa.String(length=128)),
        sa.Column("address", sa.Text()),
        sa.Column("quantity", sa.Numeric(24, 6)),
        sa.Column("incoterms", sa.String(length=64)),
        sa.Column(
            "source_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["owner_source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "owner_source_entity_fk",
            "source_row_id",
            name="uq_delivery_owner_row",
        ),
    )

    op.create_table(
        "payment_terms",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("owner_source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("prepayment_percent", sa.Numeric(8, 3)),
        sa.Column("interim_percent", sa.Numeric(8, 3)),
        sa.Column("final_percent", sa.Numeric(8, 3)),
        sa.Column("raw_text", sa.Text()),
        sa.ForeignKeyConstraint(
            ["owner_source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("owner_source_entity_fk"),
    )

    op.create_table(
        "document_metadata",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("owner_source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("identity_key", sa.String(length=512), nullable=False),
        sa.Column("source_document_id", sa.String(length=128)),
        sa.Column("category", sa.Text()),
        sa.Column("filename", sa.Text()),
        sa.Column("extension", sa.String(length=32)),
        sa.Column("url", sa.Text()),
        sa.Column("size_bytes", sa.BigInteger()),
        sa.Column("uploaded_at", sa.DateTime(timezone=True)),
        sa.Column("document_hash", sa.String(length=256)),
        sa.Column("declared_content_type", sa.String(length=256)),
        sa.Column("inferred_content_type", sa.String(length=256)),
        sa.Column("response_content_type", sa.String(length=256)),
        sa.Column(
            "source_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["owner_source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "owner_source_entity_fk",
            "identity_key",
            name="uq_document_owner_identity",
        ),
    )

    op.create_table(
        "entity_relations",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("relation_type", sa.String(length=64), nullable=False),
        sa.Column("parent_source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("child_source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column(
            "source_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["parent_source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["child_source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "relation_type",
            "parent_source_entity_fk",
            "child_source_entity_fk",
            name="uq_entity_relation",
        ),
    )

    op.create_table(
        "entity_revisions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_entity_fk", sa.BigInteger(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_entity_revisions_entity",
        "entity_revisions",
        ["source_entity_fk", "created_at"],
    )

    op.create_table(
        "session_lanes",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("proxy_id", sa.String(length=128)),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("consecutive_blocks", sa.Integer(), nullable=False),
        sa.Column("cooldown_until", sa.DateTime(timezone=True)),
        sa.Column(
            "profile_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "captcha_challenges",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("session_lane_id", sa.String(length=128)),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("challenge_type", sa.String(length=64), nullable=False),
        sa.Column("provider_task_id", sa.String(length=128)),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("cost", sa.Numeric(12, 6)),
        sa.Column("latency_ms", sa.Integer()),
        sa.Column("error_code", sa.String(length=256)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(
            ["session_lane_id"],
            ["session_lanes.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "fetch_failures",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_entity_fk", sa.BigInteger()),
        sa.Column("strategy", sa.String(length=64), nullable=False),
        sa.Column("http_status", sa.Integer()),
        sa.Column("error", sa.Text(), nullable=False),
        sa.Column("response_excerpt", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_entity_fk"],
            ["source_entities.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.execute(
        """
        ALTER TABLE crawl_frontier SET (
            fillfactor = 75,
            autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_vacuum_threshold = 1000,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_analyze_threshold = 1000
        )
        """
    )
    for name, query in EXPORT_VIEWS.items():
        op.execute(f'CREATE VIEW "{name}" AS {query}')


def downgrade() -> None:
    for name in reversed(EXPORT_VIEWS):
        op.execute(f'DROP VIEW IF EXISTS "{name}"')
    for table_name in (
        "fetch_failures",
        "captcha_challenges",
        "session_lanes",
        "entity_revisions",
        "entity_relations",
        "document_metadata",
        "payment_terms",
        "delivery_places",
        "organizations",
        "plan_items",
        "procurement_notices",
        "lots",
        "discovery_checkpoints",
        "crawl_task_history",
        "crawl_frontier",
        "source_entities",
    ):
        op.drop_table(table_name)
