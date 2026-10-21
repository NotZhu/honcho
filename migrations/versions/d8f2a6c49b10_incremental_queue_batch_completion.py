"""subtract completed queue items instead of repricing the remaining backlog

Revision ID: d8f2a6c49b10
Revises: c075274557cd

Normal completion prices only the changed rows. Message token counts are
immutable through the API; queue cancellation happens before message deletion.
If a completed representation's message is missing (or has zero tokens), use
the original recompute instead of risking an incorrect subtraction. Reopens
and other exceptional transitions also retain that repair path. DELETE keeps
the original recompute, including its tolerance of already-deleted messages.

The pending-only index makes finding the next oldest row an ordered LIMIT 1,
not another scan of the backlog. Queue is unpartitioned in both deployment
modes. Build the index concurrently before replacing the function: no queue
backfill or trigger replacement is needed, and existing batch rows stay valid.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from migrations.utils import get_schema

revision: str = "d8f2a6c49b10"
down_revision: str | None = "c075274557cd"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

schema = get_schema()
INDEX = "ix_queue_pending_work_unit_created_at"


def _create_index(*, concurrently: bool) -> None:
    connection = op.get_bind()
    valid = connection.scalar(
        sa.text(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)"
        ),
        {"name": f'"{schema}"."{INDEX}"'},
    )
    if valid is False:
        op.drop_index(
            INDEX,
            table_name="queue",
            schema=schema,
            postgresql_concurrently=concurrently,
        )
    op.create_index(
        INDEX,
        "queue",
        ["work_unit_key", "created_at"],
        schema=schema,
        postgresql_where=sa.text("NOT processed"),
        postgresql_concurrently=concurrently,
        if_not_exists=True,
    )


def upgrade() -> None:
    config = op.get_context().config
    if config is not None and not config.attributes.get("owns_transaction", True):
        # The migration test harness can supply a caller-owned transaction.
        _create_index(concurrently=False)
    else:
        # Keep the migration's timeouts: a busy/large build can be retried, and
        # an INVALID index left by cancellation is repaired on the next run.
        with op.get_context().autocommit_block():
            _create_index(concurrently=True)

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {schema}.queue_item_batches_apply_update()
        RETURNS trigger AS $$
        DECLARE
            affected_key TEXT;
            previous_count INTEGER;
            previous_tokens BIGINT;
            removed_count INTEGER;
            removed_tokens BIGINT;
            can_subtract BOOLEAN;
            next_oldest TIMESTAMPTZ;
        BEGIN
            FOR affected_key IN
                SELECT DISTINCT n.work_unit_key
                FROM new_rows n JOIN old_rows o ON o.id = n.id
                WHERE o.processed IS DISTINCT FROM n.processed
                ORDER BY 1
            LOOP
                -- Serialize with INSERT and DELETE before reading anything
                -- used to replace the aggregate. Later statements get fresh
                -- READ COMMITTED snapshots after any lock wait.
                SELECT pending_count, total_tokens
                INTO previous_count, previous_tokens
                FROM {schema}.queue_item_batches
                WHERE work_unit_key = affected_key FOR UPDATE;

                IF NOT FOUND THEN
                    PERFORM {schema}._queue_item_batches_recompute(affected_key);
                    CONTINUE;
                END IF;

                WITH changed AS MATERIALIZED (
                    SELECT o.*,
                           NOT o.processed AND n.processed
                           AND o.work_unit_key = n.work_unit_key
                           AND o.tenant_id IS NOT DISTINCT FROM n.tenant_id
                           AND o.task_type = n.task_type
                           AND o.message_id IS NOT DISTINCT FROM n.message_id
                           AS completion,
                           {schema}._queue_item_token_count(
                               o.tenant_id, o.message_id, o.task_type
                           ) AS tokens
                    FROM old_rows o JOIN new_rows n ON n.id = o.id
                    WHERE n.work_unit_key = affected_key
                      AND o.processed IS DISTINCT FROM n.processed
                )
                SELECT count(*), coalesce(sum(tokens), 0),
                       bool_and(completion AND (
                           task_type <> 'representation'
                           OR message_id IS NULL OR tokens > 0
                       ))
                INTO removed_count, removed_tokens, can_subtract
                FROM changed;

                IF NOT can_subtract
                   OR previous_count < removed_count
                   OR previous_tokens < removed_tokens THEN
                    PERFORM {schema}._queue_item_batches_recompute(affected_key);
                    CONTINUE;
                END IF;

                SELECT created_at INTO next_oldest
                FROM {schema}.queue
                WHERE work_unit_key = affected_key AND NOT processed
                ORDER BY created_at LIMIT 1;

                IF NOT FOUND THEN
                    -- Membership, rather than a possibly stale counter,
                    -- determines whether the work unit has drained.
                    DELETE FROM {schema}.queue_item_batches
                    WHERE work_unit_key = affected_key;
                ELSIF previous_count = removed_count THEN
                    PERFORM {schema}._queue_item_batches_recompute(affected_key);
                ELSE
                    UPDATE {schema}.queue_item_batches
                    SET pending_count = previous_count - removed_count,
                        total_tokens = previous_tokens - removed_tokens,
                        oldest_created_at = next_oldest
                    WHERE work_unit_key = affected_key;
                END IF;
            END LOOP;
            RETURN NULL;
        END $$ LANGUAGE plpgsql;
        """
    )


def downgrade() -> None:
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {schema}.queue_item_batches_apply_update()
        RETURNS trigger AS $$
        DECLARE
            affected_key TEXT;
        BEGIN
            FOR affected_key IN
                SELECT DISTINCT n.work_unit_key
                FROM new_rows n JOIN old_rows o ON o.id = n.id
                WHERE o.processed IS DISTINCT FROM n.processed
                ORDER BY 1
            LOOP
                PERFORM {schema}._queue_item_batches_recompute(affected_key);
            END LOOP;
            RETURN NULL;
        END $$ LANGUAGE plpgsql;
        """
    )
    # Queue is unpartitioned. Dropping an index does no table scan and is
    # guarded by the migration's lock timeout.
    op.drop_index(INDEX, table_name="queue", schema=schema, if_exists=True)
