"""Exercise online build recovery and reversible trigger installation."""

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, text

from src.config import settings

REVISION = "d8f2a6c49b10"
BASE = "c075274557cd"
INDEX = "ix_queue_pending_work_unit_created_at"


@pytest.mark.parametrize("valid", [True, False])
def test_prebuilt_or_interrupted_index(
    alembic_cfg: Config,
    alembic_engine: Engine,
    valid: bool,
) -> None:
    command.upgrade(alembic_cfg, BASE)
    schema = settings.DB.SCHEMA
    with alembic_engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE INDEX {INDEX} ON "{schema}".queue (work_unit_key, created_at) WHERE NOT processed'
            )
        )
        oid = conn.scalar(text(f"SELECT '\"{schema}\".{INDEX}'::regclass::oid"))
        if not valid:
            conn.execute(
                text("UPDATE pg_index SET indisvalid = false WHERE indexrelid = :oid"),
                {"oid": oid},
            )
    command.upgrade(alembic_cfg, REVISION)
    with alembic_engine.connect() as conn:
        result = conn.execute(
            text(
                f"SELECT indexrelid, indisvalid FROM pg_index WHERE indexrelid = '\"{schema}\".{INDEX}'::regclass"
            )
        ).one()
        assert result.indisvalid
        assert (result.indexrelid == oid) is valid


def test_downgrade_restores_recompute_with_pending_work(
    alembic_cfg: Config,
    alembic_engine: Engine,
) -> None:
    command.upgrade(alembic_cfg, REVISION)
    schema = settings.DB.SCHEMA
    with alembic_engine.begin() as conn:
        conn.execute(
            text(f"""
            INSERT INTO "{schema}".queue (work_unit_key, task_type, workspace_name, payload, processed)
            VALUES ('summary:rollback', 'summary', 'w', '{{}}', false),
                   ('summary:rollback', 'summary', 'w', '{{}}', false)
        """)
        )
    command.downgrade(alembic_cfg, BASE)
    with alembic_engine.begin() as conn:
        assert (
            conn.scalar(
                text("SELECT to_regclass(:name)"), {"name": f'"{schema}".{INDEX}'}
            )
            is None
        )
        conn.execute(
            text(
                f'UPDATE "{schema}".queue SET processed = true WHERE id = (SELECT min(id) FROM "{schema}".queue)'
            )
        )
        assert (
            conn.scalar(
                text(f'SELECT pending_count FROM "{schema}".queue_item_batches')
            )
            == 1
        )
    command.upgrade(alembic_cfg, REVISION)
    with alembic_engine.begin() as conn:
        conn.execute(text(f'UPDATE "{schema}".queue SET processed = true'))
        assert (
            conn.scalar(text(f'SELECT count(*) FROM "{schema}".queue_item_batches'))
            == 0
        )
