"""Pure result validation shared by orchestration and the fenced storage boundary."""

import json
from hashlib import sha256
from typing import Literal

from pydantic import ValidationError

from core.entities.normalization import MaintenanceMessage, NormalizationRevision, ScanCheckpoint
from core.schemas.messaging.maintenance import CleanupCompleted, MergeCompleted

MERGE_CONSUMER = "seo-metrica-pages-merge-completed"
CLEANUP_CONSUMER = "seo-metrica-cleanup-completed"
ResultKind = Literal["merge_completed", "cleanup_completed"]


def parse_result(message: MaintenanceMessage) -> MergeCompleted | CleanupCompleted:
    """Never expose Pydantic's input-bearing errors to the transport diagnostics."""
    try:
        if message.part_index is not None or len(message.payload_json.encode()) > 262144:
            raise ValueError
        payload = json.loads(message.payload_json)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if sha256(encoded.encode()).hexdigest() != message.payload_hash:
            raise ValueError
        result: MergeCompleted | CleanupCompleted
        if message.kind == "merge_completed":
            result = MergeCompleted.model_validate(payload)
        elif message.kind == "cleanup_completed":
            result = CleanupCompleted.model_validate(payload)
        else:
            raise ValueError
        if (result.equestrian_id, result.operation_id, result.message_id) != (
            message.equestrian_id,
            message.operation_id,
            message.message_id,
        ):
            raise ValueError
        return result
    except ValueError, TypeError, RecursionError, ValidationError:
        raise ValueError("invalid maintenance result") from None


def validate_correlation(revision: NormalizationRevision, checkpoint: ScanCheckpoint, result: MergeCompleted) -> None:
    if (revision.equestrian_id, revision.operation_id, revision.revision) != (
        result.equestrian_id,
        result.operation_id,
        result.normalization_revision,
    ):
        raise ValueError("result correlation mismatch")
    if checkpoint.sealed and (
        revision.part_count != checkpoint.next_part_index
        or (result.part_count is not None and result.part_count != revision.part_count)  # noqa: W503
    ):
        raise ValueError("result fullness mismatch")
    if revision.cleanup_operation_id is not None and (result.cleanup_operation_id, result.cleanup_seq) != (
        revision.cleanup_operation_id,
        revision.cleanup_seq,
    ):
        raise ValueError("cleanup correlation mismatch")
