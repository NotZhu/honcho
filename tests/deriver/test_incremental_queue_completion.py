"""Real PostgreSQL regressions for F37's completion trigger."""

import asyncio
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import delete, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from src import models
from tests.deriver.conftest import SeedWorkUnit, independent_sessions, read_batch_row


@pytest.fixture
async def token_probes(db_session: AsyncSession) -> AsyncIterator[None]:
    """Count real trigger-side message pricing calls, without timing assertions."""
    original = str(
        await db_session.scalar(
            text(
                "SELECT pg_get_functiondef('public._queue_item_token_count(text,bigint,text)'::regprocedure)"
            )
        )
    )
    await db_session.execute(
        text("CREATE TABLE public.test_token_probes (message_id bigint)")
    )
    await db_session.execute(
        text(
            original.replace(
                "_queue_item_token_count(", "_test_original_token_count(", 1
            )
        )
    )
    await db_session.execute(
        text("""
        CREATE OR REPLACE FUNCTION public._queue_item_token_count(
            tenant text, msg_id bigint, item_task_type text
        ) RETURNS bigint LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO public.test_token_probes VALUES (msg_id);
            RETURN public._test_original_token_count(tenant, msg_id, item_task_type);
        END $$
    """)
    )
    await db_session.commit()
    try:
        yield
    finally:
        await db_session.rollback()
        await db_session.execute(text(original))
        await db_session.execute(
            text("DROP FUNCTION public._test_original_token_count(text,bigint,text)")
        )
        await db_session.execute(text("DROP TABLE public.test_token_probes"))
        await db_session.commit()


@pytest.mark.parametrize("backlog", [10, 1000])
async def test_completion_prices_only_the_completed_rows(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
    token_probes: None,
    backlog: int,
) -> None:
    _ = token_probes
    key = "representation:bounded-completion"
    items = await seed_work_unit(
        key,
        token_counts=[7] * backlog,
        ages_seconds=list(range(backlog, 0, -1)),
    )
    await db_session.execute(text("TRUNCATE public.test_token_probes"))
    await db_session.execute(
        update(models.QueueItem)
        .where(models.QueueItem.id.in_([i.id for i in items[:2]]))
        .values(processed=True)
    )
    await db_session.commit()

    probes = list(
        (
            await db_session.scalars(
                text("SELECT message_id FROM public.test_token_probes")
            )
        ).all()
    )
    assert set(probes) == {i.message_id for i in items[:2]}
    assert len(probes) == 2
    row = await read_batch_row(db_session, key)
    assert row is not None
    assert row.pending_count == backlog - 2
    assert row.total_tokens == 7 * (backlog - 2)
    assert row.oldest_created_at == items[2].created_at


async def test_one_statement_completes_multiple_units_and_ignores_unchanged_rows(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
) -> None:
    a = await seed_work_unit("representation:a", token_counts=(3, 7))
    b = await seed_work_unit("representation:b", token_counts=(11,))
    done = await seed_work_unit("representation:a", token_counts=(99,), processed=True)
    await db_session.execute(
        update(models.QueueItem)
        .where(models.QueueItem.id.in_([a[0].id, b[0].id, done[0].id]))
        .values(processed=True)
    )
    await db_session.commit()
    row = await read_batch_row(db_session, "representation:a")
    assert row is not None and (row.pending_count, row.total_tokens) == (1, 7)
    assert await read_batch_row(db_session, "representation:b") is None


@pytest.mark.parametrize("tenant_id", ["default", None])
async def test_completion_then_reopen_remains_exact(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
    tenant_id: str | None,
) -> None:
    key = "representation:retry"
    items = await seed_work_unit(key, token_counts=(5, 13), tenant_id=tenant_id)
    items[0].processed = True
    await db_session.commit()
    row = await read_batch_row(db_session, key)
    assert row is not None and (row.pending_count, row.total_tokens) == (1, 13)
    items[0].processed = False
    await db_session.commit()
    row = await read_batch_row(db_session, key)
    assert row is not None and (row.pending_count, row.total_tokens) == (2, 18)
    assert row.tenant_id == tenant_id


async def test_mixed_completion_and_reopen_in_one_statement(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
) -> None:
    key = "representation:mixed"
    await seed_work_unit(key, token_counts=(5,))
    await seed_work_unit(key, token_counts=(13,), processed=True)
    await db_session.execute(
        update(models.QueueItem)
        .where(models.QueueItem.work_unit_key == key)
        .values(processed=~models.QueueItem.processed)
    )
    await db_session.commit()
    row = await read_batch_row(db_session, key)
    assert row is not None and (row.pending_count, row.total_tokens) == (1, 13)


async def test_deleted_message_uses_recompute_instead_of_subtracting_zero(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
) -> None:
    key = "representation:missing-message"
    items = await seed_work_unit(key, token_counts=(5, 13))
    await db_session.execute(
        delete(models.Message).where(models.Message.id == items[0].message_id)
    )
    await db_session.commit()
    items[0].processed = True
    await db_session.commit()
    row = await read_batch_row(db_session, key)
    assert row is not None and (row.pending_count, row.total_tokens) == (1, 13)


async def test_concurrent_completions_subtract_from_the_locked_current_total(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
) -> None:
    key = "representation:concurrent-completion"
    items = await seed_work_unit(
        key, token_counts=(5, 13, 17), ages_seconds=(30, 20, 10)
    )
    sessions = independent_sessions(db_engine)
    async with sessions() as first, sessions() as second:
        await first.execute(
            update(models.QueueItem)
            .where(models.QueueItem.id == items[0].id)
            .values(processed=True)
        )
        completing = asyncio.create_task(
            second.execute(
                update(models.QueueItem)
                .where(models.QueueItem.id == items[1].id)
                .values(processed=True)
            )
        )
        try:
            await asyncio.sleep(0.1)
            assert not completing.done()
            await first.commit()
            await asyncio.wait_for(completing, timeout=10)
            await second.commit()
        finally:
            completing.cancel()
    row = await read_batch_row(db_session, key)
    assert row is not None and (row.pending_count, row.total_tokens) == (1, 17)
    assert row.oldest_created_at == items[2].created_at


async def test_rolled_back_completion_preserves_the_aggregate(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
) -> None:
    key = "representation:completion-rollback"
    items = await seed_work_unit(key, token_counts=(5, 13))
    await db_session.execute(
        update(models.QueueItem)
        .where(models.QueueItem.id == items[0].id)
        .values(processed=True)
    )
    row = await read_batch_row(db_session, key)
    assert row is not None and (row.pending_count, row.total_tokens) == (1, 13)
    await db_session.rollback()
    row = await read_batch_row(db_session, key)
    assert row is not None and (row.pending_count, row.total_tokens) == (2, 18)


@pytest.mark.parametrize("task_type", ["summary", "webhook", "reconciler"])
async def test_non_representation_completion_keeps_zero_tokens(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
    task_type: str,
) -> None:
    key = f"{task_type}:zero-tokens"
    items = await seed_work_unit(
        key,
        task_type=task_type,
        token_counts=(0,) if task_type == "reconciler" else (0, 0),
        with_messages=False,
    )
    items[0].processed = True
    await db_session.commit()
    row = await read_batch_row(db_session, key)
    if task_type == "reconciler":
        assert row is None
    else:
        assert row is not None and (row.pending_count, row.total_tokens) == (1, 0)


async def test_oldest_lookup_uses_pending_order_index(
    db_session: AsyncSession,
    seed_work_unit: SeedWorkUnit,
) -> None:
    await seed_work_unit(
        "summary:other-unit",
        task_type="summary",
        token_counts=[0] * 2000,
        ages_seconds=[3600] * 2000,
        with_messages=False,
    )
    await seed_work_unit(
        "summary:ordered",
        task_type="summary",
        token_counts=[0] * 10,
        with_messages=False,
    )
    await db_session.execute(text("ANALYZE public.queue"))
    plan = await db_session.scalar(
        text("""
        EXPLAIN (FORMAT JSON) SELECT created_at FROM public.queue
        WHERE work_unit_key = 'summary:ordered' AND NOT processed
        ORDER BY created_at LIMIT 1
    """)
    )
    assert "ix_queue_pending_work_unit_created_at" in str(plan)
    assert "Sort" not in str(plan)
