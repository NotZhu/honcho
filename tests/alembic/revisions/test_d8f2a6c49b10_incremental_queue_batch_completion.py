"""Upgrade a nonempty queue without rebuilding its existing aggregates."""

from sqlalchemy import text

from tests.alembic.registry import register_after_upgrade, register_before_upgrade
from tests.alembic.verifier import MigrationVerifier

KEY = "summary:f37-migration"


@register_before_upgrade("d8f2a6c49b10")
def prepare(verifier: MigrationVerifier) -> None:
    verifier.conn.execute(
        text(f"""
        INSERT INTO "{verifier.schema}".queue
            (work_unit_key, task_type, workspace_name, payload, processed, created_at)
        VALUES ('{KEY}', 'summary', 'f37', '{{}}', false, now() - interval '1 minute'),
               ('{KEY}', 'summary', 'f37', '{{}}', false, now())
    """)
    )


@register_after_upgrade("d8f2a6c49b10")
def verify(verifier: MigrationVerifier) -> None:
    verifier.assert_indexes_exist([("queue", "ix_queue_pending_work_unit_created_at")])
    schema = verifier.schema
    assert (
        verifier.conn.scalar(
            text(
                f"SELECT pending_count FROM \"{schema}\".queue_item_batches WHERE work_unit_key = '{KEY}'"
            )
        )
        == 2
    )
    verifier.conn.execute(
        text(f"""
        UPDATE "{schema}".queue SET processed = true
        WHERE id = (SELECT min(id) FROM "{schema}".queue WHERE work_unit_key = '{KEY}')
    """)
    )
    row = verifier.conn.execute(
        text(f"""
        SELECT pending_count, total_tokens, oldest_created_at = (
            SELECT min(created_at) FROM "{schema}".queue WHERE work_unit_key = '{KEY}' AND NOT processed
        ) FROM "{schema}".queue_item_batches WHERE work_unit_key = '{KEY}'
    """)
    ).one()
    assert tuple(row) == (1, 0, True)
