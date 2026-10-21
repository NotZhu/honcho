"""Hooks for revision a3f9c2e81d47 (partial indexes for the reconciler's pending claim)."""

from __future__ import annotations

from tests.alembic.registry import register_after_upgrade, register_before_upgrade
from tests.alembic.verifier import MigrationVerifier

INDEXES = [
    ("documents", "ix_documents_pending_claim"),
    ("message_embeddings", "ix_message_embeddings_pending_claim"),
]


@register_before_upgrade("a3f9c2e81d47")
def prepare_pending_claim_indexes(verifier: MigrationVerifier) -> None:
    verifier.assert_indexes_not_exist(INDEXES)


@register_after_upgrade("a3f9c2e81d47")
def verify_pending_claim_indexes(verifier: MigrationVerifier) -> None:
    verifier.assert_indexes_exist(INDEXES)
