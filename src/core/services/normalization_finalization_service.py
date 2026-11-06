"""Durable inbox and one bounded finalization transaction; no transport imports."""

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import NAMESPACE_URL, UUID, uuid5

from core.entities.normalization import MaintenanceMessage, RevisionStatus, ScanCheckpoint
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol
from core.schemas.messaging.maintenance import CleanupCompleted, MergeCompleted
from core.services.normalization_results import (
    CLEANUP_CONSUMER,
    MERGE_CONSUMER,
    parse_result,
    validate_correlation,
)

FINALIZED_SUBJECT = "events.seo.pages.merge.finalized"


class NormalizationFinalizationService:
    def __init__(self, repository: NormalizationRepositoryProtocol, *, batch_size: int = 500) -> None:
        if not 1 <= batch_size <= 500:
            raise ValueError("invalid delete batch size")
        self._repository = repository
        self._batch_size = batch_size

    async def accept_result(self, *, message: MaintenanceMessage, now: datetime) -> bool:
        """Caller TX commits before ACK. False=duplicate, NOT already processed.

        ValueError is a permanent safe validation/correlation failure (term).
        Storage/integration failures propagate for rollback/retry. Never delete here.
        The durable result remains queryable after ACK/restart without redelivery.
        """
        parse_result(message)
        return await self._repository.record_result(message=message, now=now)

    async def run_batch(
        self, *, equestrian_id: UUID, now: datetime, duration: timedelta = timedelta(seconds=30)
    ) -> tuple[UUID, ScanCheckpoint] | None:
        """One caller-owned TX, commit success/rollback error; no network under lock.

        None means waiting/idle/leased, not completion. Only revision FINALIZED
        permits the scan runner to activate its successor in a later transaction.
        Both scan sealed AND correlated cleanup completed are required for cancel.
        """
        if now.utcoffset() is None or duration <= timedelta(0):
            raise ValueError("timezone and positive duration required")
        repo = self._repository
        await repo.lock_tenant(equestrian_id=equestrian_id)
        revision = await repo.get_active_revision(equestrian_id=equestrian_id)
        if revision is None or revision.status not in (
            RevisionStatus.AWAITING_RESULT,
            RevisionStatus.AWAITING_CLEANUP,
            RevisionStatus.DELETING,
        ):
            return None
        operation = revision.operation_id
        checkpoint = await repo.get_checkpoint(equestrian_id=equestrian_id, operation_id=operation)
        message = await repo.get_inbox_result(
            consumer=MERGE_CONSUMER, equestrian_id=equestrian_id, operation_id=operation
        )
        if message is None:
            return None
        result = parse_result(message)
        if not isinstance(result, MergeCompleted):
            raise ValueError("merge result required")
        validate_correlation(revision, checkpoint, result)
        if not checkpoint.sealed:
            return None
        if result.status == "superseded_by_delete":
            assert result.cleanup_operation_id is not None
            cleanup = await repo.get_inbox_result(
                consumer=CLEANUP_CONSUMER, equestrian_id=equestrian_id, operation_id=result.cleanup_operation_id
            )
            if cleanup is None:
                return None
            completed = parse_result(cleanup)
            if not isinstance(completed, CleanupCompleted) or completed.cleanup_seq != result.cleanup_seq:
                raise ValueError("cleanup correlation mismatch")
        lease = await repo.claim_scan(equestrian_id=equestrian_id, operation_id=operation, now=now, duration=duration)
        if lease is None:
            return None
        message_id = uuid5(
            NAMESPACE_URL,
            f"eqcms:maintenance:v1:{FINALIZED_SUBJECT}:{operation}:finalized:{result.message_id}",
        )
        payload = dict(
            schema_version=1,
            message_id=str(message_id),
            operation_id=str(operation),
            equestrian_id=str(equestrian_id),
            occurred_at=now.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            normalization_revision=revision.revision,
            result_message_id=str(result.message_id),
        )
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        finalized = MaintenanceMessage(
            equestrian_id, operation, message_id, "finalized", encoded, sha256(encoded.encode()).hexdigest()
        )
        checkpoint = await repo.commit_delete_batch(
            equestrian_id=equestrian_id,
            operation_id=operation,
            lease=lease,
            expected=checkpoint,
            result=message,
            finalized=finalized,
            now=now,
            limit=self._batch_size,
        )
        return operation, checkpoint
