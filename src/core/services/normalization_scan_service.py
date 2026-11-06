"""One bounded, caller-owned transaction; no scheduler or transport activation."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import NAMESPACE_URL, UUID, uuid5

from core.entities.normalization import (
    MaintenanceMessage,
    NormalizationRevision,
    PageMapping,
    RevisionStatus,
    ScanCheckpoint,
)
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol
from core.protocols.repositories.page_repository import PageRepositoryProtocol
from core.services.page_normalizer import PageNormalizer

MERGE_SUBJECT = "commands.seo.pages.merge"
MAX_BODY_BYTES = 256 * 1024


class NormalizationScanService:
    def __init__(
        self,
        normalization_repository: NormalizationRepositoryProtocol,
        page_repository: PageRepositoryProtocol,
        page_normalizer: PageNormalizer,
        *,
        batch_size: int = 500,
        max_body_bytes: int = MAX_BODY_BYTES,
    ) -> None:
        if not 1 <= batch_size <= 500 or not 1 <= max_body_bytes <= MAX_BODY_BYTES:
            raise ValueError("invalid scan bounds")
        self._repository = normalization_repository
        self._pages = page_repository
        self._normalizer = page_normalizer
        self._batch_size = batch_size
        self._max_body_bytes = max_body_bytes

    def _message(
        self,
        revision: NormalizationRevision,
        checkpoint: ScanCheckpoint,
        now: datetime,
        mappings: list[PageMapping] | None = None,
    ) -> MaintenanceMessage:
        kind = "mappings" if mappings is not None else "seal"
        suffix = str(checkpoint.next_part_index) if mappings is not None else kind
        message_id = uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{MERGE_SUBJECT}:{revision.operation_id}:{suffix}")
        payload: dict[str, object] = dict(
            schema_version=1,
            message_id=str(message_id),
            operation_id=str(revision.operation_id),
            equestrian_id=str(revision.equestrian_id),
            occurred_at=now.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            normalization_revision=revision.revision,
            kind=kind,
        )
        if mappings is None:
            payload["part_count"] = checkpoint.next_part_index
        else:
            payload["part_index"] = checkpoint.next_part_index
            payload["mappings"] = [
                dict(source_page_id=str(item.source_id), target_page_id=str(item.target_id)) for item in mappings
            ]
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return MaintenanceMessage(
            revision.equestrian_id,
            revision.operation_id,
            message_id,
            kind,
            encoded,
            sha256(encoded.encode()).hexdigest(),
            checkpoint.next_part_index if mappings is not None else None,
        )

    async def run_batch(
        self, *, equestrian_id: UUID, now: datetime, duration: timedelta = timedelta(seconds=30)
    ) -> tuple[UUID, ScanCheckpoint] | None:
        """Activate/resume the oldest unfinished revision and commit at most one batch.

        Caller MUST commit on success and rollback on any exception (including
        target creation before the storage savepoint). No I/O across this TX.
        None means idle, leased, or waiting for S-3 result/finalization; it NEVER
        permits activation of a later revision. Restart calls this same method.
        A seal-only transaction follows the final page batch, including empty scans.
        """
        if now.utcoffset() is None or duration <= timedelta(0):
            raise ValueError("timezone and positive duration required")
        await self._repository.lock_tenant(equestrian_id=equestrian_id)
        revision = await self._repository.get_active_revision(equestrian_id=equestrian_id)
        if revision is None:
            return None
        if revision.status == RevisionStatus.PENDING:
            revision = await self._repository.activate_next(equestrian_id=equestrian_id)
        if revision is None or revision.status not in (RevisionStatus.SCANNING, RevisionStatus.AWAITING_CLEANUP):
            return None
        expected = await self._repository.get_checkpoint(
            equestrian_id=equestrian_id, operation_id=revision.operation_id
        )
        if expected.sealed:
            return None
        if expected.high_water is None:
            raise ValueError("active scan has no watermark")
        lease = await self._repository.claim_scan(
            equestrian_id=equestrian_id, operation_id=revision.operation_id, now=now, duration=duration
        )
        if lease is None:
            return None
        mappings: list[PageMapping] = []
        messages: list[MaintenanceMessage] = []
        if expected.cursor == expected.high_water:
            checkpoint = replace(expected, sealed=True)
            messages = [self._message(revision, expected, now)]
        else:
            pages = await self._repository.scan_pages(
                equestrian_id=equestrian_id, checkpoint=expected, limit=self._batch_size
            )
            cursor = expected.cursor
            message = None
            targets: dict[str, UUID] = {}  # At most batch_size paths, discarded after this TX.
            for page in pages:
                normalized = self._normalizer.normalize(
                    page.page,
                    blacklisted_params=frozenset(revision.snapshot.query_params),
                    blacklisted_hashes=frozenset(revision.snapshot.hashes),
                )
                if normalized != page.page:
                    # Use the same unique-path find-or-create as ordinary resolve,
                    # then follow UUID aliases, never mutable rules or URL rewrites.
                    if normalized not in targets:
                        ids = await self._pages.resolve(equestrian_id=equestrian_id, values=[normalized])
                        targets[normalized] = await self._repository.resolve_redirect(
                            equestrian_id=equestrian_id, page_id=ids[normalized]
                        )
                    target = targets[normalized]
                    if target != page.id:
                        item = PageMapping(page.id, target, page.scan_seq, expected.next_part_index)
                        candidate = self._message(revision, expected, now, [*mappings, item])
                        if len(candidate.payload_json.encode()) > self._max_body_bytes:
                            if not mappings:
                                raise ValueError("single mapping exceeds body bound")
                            break  # Do not advance past the unrecorded source.
                        mappings.append(item)
                        message = candidate
                cursor = page.scan_seq
            else:
                # Sequence gaps/deleted pages do not require an unbounded search.
                if len(pages) < self._batch_size:
                    cursor = expected.high_water
            checkpoint = replace(expected, cursor=cursor, next_part_index=expected.next_part_index + bool(mappings))
            if message is not None:
                messages.append(message)
        if any(len(message.payload_json.encode()) > self._max_body_bytes for message in messages):
            raise ValueError("seal exceeds body bound")
        await self._repository.commit_scan_batch(
            equestrian_id=equestrian_id,
            operation_id=revision.operation_id,
            lease=lease,
            expected=expected,
            checkpoint=checkpoint,
            mappings=mappings,
            messages=messages,
            now=now,
        )
        return revision.operation_id, checkpoint
