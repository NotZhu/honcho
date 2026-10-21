"""index the reconciler's pending claim

The reconciler claims work to re-embed with (src/reconciler/sync_vectors.py:121,
:161)::

    WHERE sync_state = 'pending'
      AND (last_sync_at IS NULL OR last_sync_at < now() - backoff)
    ORDER BY last_sync_at NULLS FIRST
    LIMIT n

``ix_documents_sync_state_last_sync_at`` and its message_embeddings twin cannot
serve that order. They are ASC NULLS LAST; the claim reads NULLS FIRST. The
backoff disjunction is never an index condition either, so it stays a filter, and
the mixed scan types that result across partitions force a Sort over a full
Append. The LIMIT then cannot stop early: every pending row of every tenant is
read to return n of them. Cost grows with the whole fleet's backlog, and the
backlog is largest during the embedding outage that produced it.

A partial index whose order matches the claim restores Merge Append, so the LIMIT
stops early and the cost is O(limit x partitions) -- flat in the backlog. Measured
on a 32-partition replica, documents:

    backlog     before          after
    2,000       822 buffers      88
    100,000     3,117 buffers   105   (before also escalates to a Parallel
                                       Append and drains all 100k rows across
                                       3 workers, taking workers from the
                                       request path)

NULLS FIRST is load-bearing, not cosmetic: built without it the index is ASC
NULLS LAST, cannot satisfy the order, and the plan falls back to Sort -- 3,115
buffers against 105, while still costing write amplification on two of the
hottest tables.

The claim runs on the service role, which bypasses RLS, so unlike the message
search path these indexes are genuinely reachable (a non-leakproof predicate
cannot be an index condition while RLS is forced; `sync_state = 'pending'` is a
plain equality and is not affected either way).

The builds hold no long write lock, on plain or partitioned tables (see
migrations.utils.create_index_online). To build them ahead of a release, run the
same steps by hand; this revision then finds them done.

Revision ID: a3f9c2e81d47
Revises: 647b773f05ac
Create Date: 2026-10-21

"""

from collections.abc import Sequence

from migrations.utils import create_index_online, drop_index_online

# revision identifiers, used by Alembic.
revision: str = "a3f9c2e81d47"
down_revision: str | None = "647b773f05ac"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (name, table, columns, include, where): the same as in src/models.py.
INDEXES: list[tuple[str, str, str, str | None, str | None]] = [
    (
        "ix_documents_pending_claim",
        "documents",
        "last_sync_at NULLS FIRST",
        None,
        "sync_state = 'pending'",
    ),
    (
        "ix_message_embeddings_pending_claim",
        "message_embeddings",
        "last_sync_at NULLS FIRST",
        None,
        "sync_state = 'pending'",
    ),
]


def upgrade() -> None:
    for name, table, columns, include, where in INDEXES:
        create_index_online(name, table, columns, include=include, where=where)


def downgrade() -> None:
    for name, table, *_ in reversed(INDEXES):
        drop_index_online(name, table)
