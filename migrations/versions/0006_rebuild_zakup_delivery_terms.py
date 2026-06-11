"""Rebuild Zakup delivery terms from schedule payload.

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE lots e
        SET delivery_terms_ru = CASE
                WHEN e.source_payload->'schedule'->>'count' IS NOT NULL
                THEN concat(
                    'С даты подписания договора в течение ',
                    e.source_payload->'schedule'->>'count',
                    CASE e.source_payload->'schedule'->'dayType'->>'code'
                        WHEN 'CALENDAR' THEN ' календарных'
                        WHEN 'WORKING' THEN ' рабочих'
                        ELSE CASE
                            WHEN e.source_payload->'schedule'->'dayType'->>'ru'
                                 IS NOT NULL
                            THEN concat(
                                ' ',
                                lower(e.source_payload->'schedule'->'dayType'->>'ru')
                            )
                            ELSE ''
                        END
                    END,
                    ' дней'
                )
                WHEN e.source_payload->'schedule'->>'monthTo' IS NOT NULL
                THEN concat(
                    'С даты подписания договора по (включительно) ',
                    e.source_payload->'schedule'->>'monthTo'
                )
                ELSE e.delivery_terms_ru
            END,
            delivery_terms_kk = CASE
                WHEN e.source_payload->'schedule'->>'count' IS NOT NULL
                THEN concat(
                    'Шартқа қол қойылған күннен бастап ',
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
                    'Шартқа қол қойылған күннен бастап ',
                    e.source_payload->'schedule'->>'monthTo',
                    ' дейін (қоса алғанда)'
                )
                ELSE e.delivery_terms_kk
            END
        FROM source_entities se
        WHERE se.id = e.source_entity_fk
          AND se.source = 'zakup-sk'
        """
    )


def downgrade() -> None:
    pass
