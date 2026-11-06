"""Bounded storage primitives; caller owns commit/rollback and transaction lifetime.

Runtime wiring deliberately lives elsewhere; no HTTP/NATS calls occur here.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Table, and_, delete, func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.normalization import (
    MaintenanceMessage,
    NormalizationRevision,
    NormalizationSnapshot,
    OutboxClaim,
    PageMapping,
    RevisionStatus,
    ScanCheckpoint,
    ScanPage,
    StorageLease,
)
from core.schemas.messaging.maintenance import CleanupCompleted, MergeCompleted, MergeFinalized
from core.services.normalization_results import (
    CLEANUP_CONSUMER,
    MERGE_CONSUMER,
    parse_result,
    validate_correlation,
)
from models.normalization import normalization_inbox as inbox
from models.normalization import normalization_mappings as mappings
from models.normalization import normalization_outbox as outbox
from models.normalization import normalization_redirects as redirects
from models.normalization import normalization_revisions as revisions
from models.normalization import normalization_scans as scans
from models.normalization import normalization_snapshots as snapshots
from models.normalization import normalization_tenants as tenants
from models.pages import pages
from repositories.abstract_repository import repository_errors

MAX_BATCH = 500
MAX_REDIRECT_HOPS = 64
MAX_PAYLOAD_BYTES = 256 * 1024


def _time(now: datetime, duration: timedelta | None = None) -> None:
    if now.utcoffset() is None or (duration is not None and duration <= timedelta(0)):
        raise ValueError("timezone and positive duration required")


def _payload(message: MaintenanceMessage) -> dict[str, Any]:
    # JSONB is not byte preserving; hash canonical content like metrica storage.
    if len(message.payload_json.encode()) > MAX_PAYLOAD_BYTES:
        raise ValueError("payload exceeds storage bound")
    try:
        payload = json.loads(message.payload_json)
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except ValueError, TypeError, RecursionError:
        raise ValueError("invalid maintenance JSON") from None
    if not isinstance(payload, dict) or sha256(canonical.encode()).hexdigest() != message.payload_hash:
        raise ValueError("payload hash mismatch")
    if len(canonical.encode()) > MAX_PAYLOAD_BYTES:
        raise ValueError("payload exceeds storage bound")
    return payload


def _checkpoint(row: Mapping[str, Any]) -> ScanCheckpoint:
    return ScanCheckpoint(
        row["cursor"], row["high_water"], row["delete_cursor"], row["next_part_index"], bool(row["sealed"])
    )


def _same_message(row: Mapping[str, Any], message: MaintenanceMessage, payload: dict[str, Any]) -> bool:
    return (
        all(
            row[key] == getattr(message, key)
            for key in ("equestrian_id", "operation_id", "message_id", "payload_hash")
        )
        and row["payload"] == payload  # noqa: W503
    )


def _scope(table: Table, tenant: UUID, operation: UUID):
    return (table.c.equestrian_id == tenant, table.c.operation_id == operation)


def _revision(row: Mapping[str, Any]) -> NormalizationRevision:
    return NormalizationRevision(
        equestrian_id=row["equestrian_id"],
        operation_id=row["operation_id"],
        revision=row["revision"],
        status=RevisionStatus(row["status"]),
        snapshot=NormalizationSnapshot(tuple(row["query_params"]), tuple(row["hashes"])),
        part_count=row["part_count"],
        cleanup_operation_id=row["cleanup_operation_id"],
        cleanup_seq=row["cleanup_seq"],
    )


def _limit(limit: int) -> None:
    if not 1 <= limit <= MAX_BATCH:
        raise ValueError("batch limit outside storage bounds")


class NormalizationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_work_tenants(self, *, now: datetime, after: UUID | None = None, limit: int = 100) -> Sequence[UUID]:
        """Read-only bounded discovery, including finalized-but-unpublished work."""
        _time(now)
        _limit(limit)
        pending = select(revisions.c.equestrian_id).where(revisions.c.status != RevisionStatus.FINALIZED.value)
        due = select(outbox.c.equestrian_id).where(
            outbox.c.published_at.is_(None),
            outbox.c.next_attempt_at <= now,
            or_(outbox.c.lease_until.is_(None), outbox.c.lease_until <= now),
        )
        if after is not None:
            pending = pending.where(revisions.c.equestrian_id > after)
            due = due.where(outbox.c.equestrian_id > after)
        # Limit each distinct branch before UNION, not just the result exposed to
        # Python. A tenant with many revisions/messages cannot consume the page.
        candidates = (
            pending.distinct()
            .order_by(revisions.c.equestrian_id)
            .limit(limit)
            .union(due.distinct().order_by(outbox.c.equestrian_id).limit(limit))
            .subquery()
        )
        statement = select(candidates.c.equestrian_id).order_by(candidates.c.equestrian_id).limit(limit)
        with repository_errors():
            return list((await self.session.execute(statement)).scalars())

    async def lock_tenant(self, *, equestrian_id: UUID) -> None:
        """Held until caller transaction ends; always before page sequence allocation."""
        with repository_errors():
            await self.session.execute(pg_insert(tenants).values(equestrian_id=equestrian_id).on_conflict_do_nothing())
            await self.session.execute(
                select(tenants.c.equestrian_id).where(tenants.c.equestrian_id == equestrian_id).with_for_update()
            )

    async def create_revision(
        self, *, equestrian_id: UUID, operation_id: UUID, snapshot: NormalizationSnapshot
    ) -> NormalizationRevision:
        await self.lock_tenant(equestrian_id=equestrian_id)
        with repository_errors():
            number = (
                await self.session.execute(
                    update(tenants)
                    .where(tenants.c.equestrian_id == equestrian_id)
                    .values(last_revision=tenants.c.last_revision + 1)
                    .returning(tenants.c.last_revision)
                )
            ).scalar_one()
            await self.session.execute(
                insert(revisions).values(equestrian_id=equestrian_id, operation_id=operation_id, revision=number)
            )
            await self.session.execute(
                insert(snapshots).values(
                    equestrian_id=equestrian_id,
                    operation_id=operation_id,
                    query_params=list(snapshot.query_params),
                    hashes=list(snapshot.hashes),
                )
            )
            await self.session.execute(insert(scans).values(equestrian_id=equestrian_id, operation_id=operation_id))
        return NormalizationRevision(equestrian_id, operation_id, number, RevisionStatus.PENDING, snapshot)

    async def get_revision(self, *, equestrian_id: UUID, operation_id: UUID) -> NormalizationRevision | None:
        with repository_errors():
            row = (
                (
                    await self.session.execute(
                        select(revisions, snapshots.c.query_params, snapshots.c.hashes)
                        .join(
                            snapshots,
                            and_(
                                revisions.c.equestrian_id == snapshots.c.equestrian_id,
                                revisions.c.operation_id == snapshots.c.operation_id,
                            ),
                        )
                        .where(*_scope(revisions, equestrian_id, operation_id))
                    )
                )
                .mappings()
                .first()
            )
        return _revision(dict(row)) if row is not None else None

    async def get_active_revision(self, *, equestrian_id: UUID) -> NormalizationRevision | None:
        """Caller holds tenant lock; unfinished predecessors block all later work."""
        with repository_errors():
            operation = await self.session.scalar(
                select(revisions.c.operation_id)
                .where(revisions.c.equestrian_id == equestrian_id, revisions.c.status != "finalized")
                .order_by(revisions.c.revision)
                .limit(1)
            )
        if operation is None:
            return None
        return await self.get_revision(equestrian_id=equestrian_id, operation_id=operation)

    async def activate_next(self, *, equestrian_id: UUID) -> NormalizationRevision | None:
        await self.lock_tenant(equestrian_id=equestrian_id)
        with repository_errors():
            # Oldest unfinished revision blocks all later revisions, including when blocked.
            row = (
                (
                    await self.session.execute(
                        select(revisions.c.operation_id, revisions.c.status)
                        .where(revisions.c.equestrian_id == equestrian_id, revisions.c.status != "finalized")
                        .order_by(revisions.c.revision)
                        .limit(1)
                    )
                )
                .mappings()
                .first()
            )
            if row is None or row["status"] != "pending":
                return None
            operation = row["operation_id"]
            high_water = await self.session.scalar(
                select(func.coalesce(func.max(pages.c.scan_seq), 0)).where(pages.c.equestrian_id == equestrian_id)
            )
            await self.session.execute(
                update(scans).where(*_scope(scans, equestrian_id, operation)).values(high_water=high_water)
            )
            await self.session.execute(
                update(revisions).where(*_scope(revisions, equestrian_id, operation)).values(status="scanning")
            )
        return await self.get_revision(equestrian_id=equestrian_id, operation_id=operation)

    async def get_checkpoint(self, *, equestrian_id: UUID, operation_id: UUID) -> ScanCheckpoint:
        with repository_errors():
            row = (
                (await self.session.execute(select(scans).where(*_scope(scans, equestrian_id, operation_id))))
                .mappings()
                .first()
            )
        if row is None:
            raise ValueError("unknown tenant operation")
        return ScanCheckpoint(
            row["cursor"], row["high_water"], row["delete_cursor"], row["next_part_index"], bool(row["sealed"])
        )

    async def scan_pages(self, *, equestrian_id: UUID, checkpoint: ScanCheckpoint, limit: int) -> Sequence[ScanPage]:
        _limit(limit)
        if checkpoint.high_water is None:
            raise ValueError("scan has not been activated")
        with repository_errors():
            rows = await self.session.execute(
                select(pages.c.id, pages.c.page, pages.c.scan_seq)
                .where(
                    pages.c.equestrian_id == equestrian_id,
                    pages.c.scan_seq > checkpoint.cursor,
                    pages.c.scan_seq <= checkpoint.high_water,
                )
                .order_by(pages.c.scan_seq)
                .limit(limit)
            )
            return [ScanPage(**dict(row)) for row in rows.mappings()]

    async def _walk(self, tenant: UUID, page_id: UUID) -> list[UUID]:
        path: list[UUID] = []
        current = page_id
        for _ in range(MAX_REDIRECT_HOPS + 1):
            if current in path:
                raise ValueError("redirect cycle")
            path.append(current)
            target = await self.session.scalar(
                select(redirects.c.target_id).where(
                    redirects.c.equestrian_id == tenant, redirects.c.source_id == current
                )
            )
            if target is None:
                exists = await self.session.scalar(
                    select(pages.c.id).where(pages.c.equestrian_id == tenant, pages.c.id == current)
                )
                if exists is None:
                    raise ValueError("redirect target is not a tenant page")
                return path
            current = target
        raise ValueError("redirect chain exceeds storage bound")

    async def resolve_redirect(self, *, equestrian_id: UUID, page_id: UUID) -> UUID:
        await self.lock_tenant(equestrian_id=equestrian_id)
        with repository_errors():
            path = await self._walk(equestrian_id, page_id)
            if len(path) > 2:
                await self.session.execute(
                    update(redirects)
                    .where(redirects.c.equestrian_id == equestrian_id, redirects.c.source_id.in_(path[:-1]))
                    .values(target_id=path[-1])
                )
            return path[-1]

    async def store_mappings(self, *, equestrian_id: UUID, operation_id: UUID, batch: Sequence[PageMapping]) -> None:
        """Validate entire bounded batch before effects; caller also fences/checkpoints it.

        Only direct canonical targets are admitted (no target that is another
        source). Existing exact mappings are idempotent, conflicting ones fail.
        Redirects survive source deletion; a later revision may extend a chain.
        """
        _limit(len(batch))
        staged = {m.source_id: m.target_id for m in batch}
        if len(staged) != len(batch):
            raise ValueError("duplicate source in batch")
        await self.lock_tenant(equestrian_id=equestrian_id)
        with repository_errors():
            revision = await self.get_revision(equestrian_id=equestrian_id, operation_id=operation_id)
            if revision is None or revision.status not in (RevisionStatus.SCANNING, RevisionStatus.AWAITING_CLEANUP):
                raise ValueError("operation is not scanning")
            checkpoint = await self.get_checkpoint(equestrian_id=equestrian_id, operation_id=operation_id)
            if checkpoint.sealed or checkpoint.high_water is None:
                raise ValueError("scan is not writable")
            ids = set(staged) | set(staged.values())
            rows = await self.session.execute(
                select(pages.c.id, pages.c.scan_seq).where(pages.c.equestrian_id == equestrian_id, pages.c.id.in_(ids))
            )
            membership = dict(rows.tuples().all())
            if membership.keys() != ids:
                raise ValueError("mapping page is not a tenant member")
            existing_rows = await self.session.execute(
                select(mappings).where(
                    *_scope(mappings, equestrian_id, operation_id), mappings.c.source_id.in_(staged)
                )
            )
            existing = {row["source_id"]: row for row in existing_rows.mappings()}
            aliases = await self.session.execute(
                select(redirects.c.source_id, redirects.c.target_id).where(
                    redirects.c.equestrian_id == equestrian_id, redirects.c.source_id.in_(ids)
                )
            )
            existing_aliases = dict(aliases.tuples().all())
            values = []
            for mapping in batch:
                if membership[mapping.source_id] != mapping.source_scan_seq:
                    raise ValueError("source scan sequence mismatch")
                if mapping.source_scan_seq > checkpoint.high_water:
                    raise ValueError("source outside revision watermark")
                # Membership already proves that all endpoints are live tenant pages.
                # A terminal target has no outgoing edge, in storage or this batch.
                # Therefore a new edge cannot introduce either a chain or a cycle.
                if mapping.target_id in staged or mapping.target_id in existing_aliases:
                    raise ValueError("mapping target is not canonical (chain or cycle)")
                if mapping.source_id in existing_aliases and existing_aliases[mapping.source_id] != mapping.target_id:
                    raise ValueError("source redirect conflict")
                value = dict(
                    equestrian_id=equestrian_id,
                    operation_id=operation_id,
                    source_id=mapping.source_id,
                    target_id=mapping.target_id,
                    source_scan_seq=mapping.source_scan_seq,
                    part_index=mapping.part_index,
                )
                previous = existing.get(mapping.source_id)
                if previous is not None and dict(previous) != value:
                    raise ValueError("source mapping conflict")
                values.append(value)
            await self.session.execute(
                pg_insert(mappings).values(values).on_conflict_do_nothing(constraint="uq_normalization_mapping_source")
            )
            await self.session.execute(
                pg_insert(redirects)
                .values(
                    [
                        {key: value[key] for key in ("equestrian_id", "operation_id", "source_id", "target_id")}
                        for value in values
                    ]
                )
                .on_conflict_do_nothing(constraint="uq_normalization_redirect_source")
            )

    async def get_mappings(
        self, *, equestrian_id: UUID, operation_id: UUID, after_seq: int = 0, limit: int = MAX_BATCH
    ) -> Sequence[PageMapping]:
        _limit(limit)
        if after_seq < 0:
            raise ValueError("negative mapping cursor")
        with repository_errors():
            rows = await self.session.execute(
                select(mappings.c.source_id, mappings.c.target_id, mappings.c.source_scan_seq, mappings.c.part_index)
                .where(*_scope(mappings, equestrian_id, operation_id), mappings.c.source_scan_seq > after_seq)
                .order_by(mappings.c.source_scan_seq)
                .limit(limit)
            )
            return [PageMapping(**dict(row)) for row in rows.mappings()]

    async def claim_scan(
        self, *, equestrian_id: UUID, operation_id: UUID, now: datetime, duration: timedelta
    ) -> StorageLease | None:
        """One operation lease, including later deletion; never hold across I/O."""
        _time(now, duration)
        await self.lock_tenant(equestrian_id=equestrian_id)
        with repository_errors():
            revision = await self.get_revision(equestrian_id=equestrian_id, operation_id=operation_id)
            if revision is None or revision.status not in (
                RevisionStatus.SCANNING,
                RevisionStatus.AWAITING_RESULT,
                RevisionStatus.AWAITING_CLEANUP,
                RevisionStatus.DELETING,
            ):
                return None
            token = uuid4()
            row = (
                await self.session.execute(
                    update(scans)
                    .where(
                        *_scope(scans, equestrian_id, operation_id),
                        scans.c.high_water.is_not(None),
                        or_(scans.c.lease_until.is_(None), scans.c.lease_until <= now),
                    )
                    .values(lease_token=token, lease_until=now + duration, attempts=scans.c.attempts + 1)
                    .returning(scans.c.attempts)
                )
            ).first()
            return StorageLease(token, now + duration, row[0]) if row is not None else None

    async def _fence_scan(
        self, tenant: UUID, operation: UUID, lease: StorageLease, expected: ScanCheckpoint, now: datetime
    ) -> None:
        _time(now)
        row = (
            (
                await self.session.execute(
                    select(scans)
                    .where(
                        *_scope(scans, tenant, operation),
                        scans.c.lease_token == lease.token,
                        scans.c.lease_until > now,
                        scans.c.lease_until > func.clock_timestamp(),
                        scans.c.attempts == lease.attempts,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .first()
        )
        if row is None or row["lease_until"] != lease.until or _checkpoint(dict(row)) != expected:
            raise ValueError("stale operation lease or checkpoint")

    async def commit_scan_batch(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        lease: StorageLease,
        expected: ScanCheckpoint,
        checkpoint: ScanCheckpoint,
        mappings: Sequence[PageMapping],
        messages: Sequence[MaintenanceMessage],
        now: datetime,
    ) -> None:
        """SAVEPOINT protects all effects even if caller catches a domain error.

        Outer transaction remains caller-owned. Final clock fence rolls back the
        entire batch on expiry while computing/writing it. Claim again after each
        committed batch (lease is released by the checkpoint update).
        """
        if len(mappings) > MAX_BATCH or len(messages) > 2:
            raise ValueError("batch exceeds storage bound")
        if (
            expected.sealed
            or expected.high_water is None  # noqa: W503
            or checkpoint.high_water != expected.high_water  # noqa: W503
            or checkpoint.cursor < expected.cursor  # noqa: W503
            or checkpoint.delete_cursor != expected.delete_cursor  # noqa: W503
            or checkpoint.next_part_index != expected.next_part_index + bool(mappings)  # noqa: W503
            or any(not expected.cursor < m.source_scan_seq <= checkpoint.cursor for m in mappings)  # noqa: W503
            or any(m.part_index != expected.next_part_index for m in mappings)  # noqa: W503
            or any(m.equestrian_id != equestrian_id or m.operation_id != operation_id for m in messages)  # noqa: W503
        ):
            raise ValueError("invalid scan checkpoint transition")
        parts = [message for message in messages if message.part_index is not None]
        terminals = [message for message in messages if message.part_index is None]
        if (
            len(parts) != bool(mappings)
            or len(terminals) != checkpoint.sealed  # noqa: W503
            or any(message.part_index != expected.next_part_index for message in parts)  # noqa: W503
        ):
            raise ValueError("outbox parts do not match scan checkpoint")
        for message in messages:
            _payload(message)
        await self.lock_tenant(equestrian_id=equestrian_id)
        with repository_errors():
            async with self.session.begin_nested():
                await self._fence_scan(equestrian_id, operation_id, lease, expected, now)
                revision = await self.get_revision(equestrian_id=equestrian_id, operation_id=operation_id)
                if revision is None or revision.status not in (
                    RevisionStatus.SCANNING,
                    RevisionStatus.AWAITING_CLEANUP,
                ):
                    raise ValueError("operation is not scanning")
                if mappings:
                    await self.store_mappings(equestrian_id=equestrian_id, operation_id=operation_id, batch=mappings)
                for message in messages:
                    await self.enqueue_outbox(message=message)
                if checkpoint.sealed:
                    # Same savepoint and final clock fence as aliases/outbox/cursor.
                    # Cleanup correlation remains intact; a seal is NOT finalization.
                    await self.session.execute(
                        update(revisions)
                        .where(*_scope(revisions, equestrian_id, operation_id))
                        .values(
                            part_count=checkpoint.next_part_index,
                            status=(
                                "awaiting_cleanup"
                                if revision.status == RevisionStatus.AWAITING_CLEANUP
                                else "awaiting_result"
                            ),
                        )
                    )
                changed = (
                    await self.session.execute(
                        update(scans)
                        .where(
                            *_scope(scans, equestrian_id, operation_id),
                            scans.c.lease_token == lease.token,
                            scans.c.lease_until > now,
                            scans.c.lease_until > func.clock_timestamp(),
                        )
                        .values(
                            cursor=checkpoint.cursor,
                            next_part_index=checkpoint.next_part_index,
                            sealed=int(checkpoint.sealed),
                            lease_token=None,
                            lease_until=None,
                        )
                        .returning(scans.c.operation_id)
                    )
                ).first()
                if changed is None:
                    raise ValueError("operation lease expired during batch")

    async def accept_inbox(self, *, consumer: str, message: MaintenanceMessage) -> bool:
        """Unknown early cleanup results may be retained, never authorize deletion."""
        if not consumer or len(consumer) > 64:
            raise ValueError("invalid inbox consumer")
        payload = _payload(message)
        with repository_errors():
            inserted = (
                await self.session.execute(
                    pg_insert(inbox)
                    .values(
                        equestrian_id=message.equestrian_id,
                        operation_id=message.operation_id,
                        consumer=consumer,
                        message_id=message.message_id,
                        payload_hash=message.payload_hash,
                        payload=payload,
                    )
                    .on_conflict_do_nothing(constraint="uq_normalization_inbox_message")
                    .returning(inbox.c.message_id)
                )
            ).first()
            row = (
                (
                    await self.session.execute(
                        select(inbox).where(inbox.c.consumer == consumer, inbox.c.message_id == message.message_id)
                    )
                )
                .mappings()
                .one()
            )
            if not _same_message(dict(row), message, payload):
                raise ValueError("inbox identity conflict")
            return inserted is not None

    async def mark_inbox_processed(self, *, consumer: str, message: MaintenanceMessage, now: datetime) -> None:
        """Caller commits this marker with correlated operation effects, then ACKs."""
        _time(now)
        payload = _payload(message)
        with repository_errors():
            row = (
                (
                    await self.session.execute(
                        select(inbox)
                        .where(inbox.c.consumer == consumer, inbox.c.message_id == message.message_id)
                        .with_for_update()
                    )
                )
                .mappings()
                .first()
            )
            if row is None or not _same_message(dict(row), message, payload):
                raise ValueError("unknown or conflicting inbox message")
            await self.session.execute(
                update(inbox)
                .where(
                    inbox.c.consumer == consumer,
                    inbox.c.message_id == message.message_id,
                    inbox.c.equestrian_id == message.equestrian_id,
                    inbox.c.processed_at.is_(None),
                )
                .values(processed_at=now)
            )

    async def enqueue_outbox(self, *, message: MaintenanceMessage) -> None:
        payload = _payload(message)
        await self.lock_tenant(equestrian_id=message.equestrian_id)
        with repository_errors():
            # Validate operation membership before insert; a foreign message ID
            # collision is compared below, never silently swallowed.
            if await self.get_revision(equestrian_id=message.equestrian_id, operation_id=message.operation_id) is None:
                raise ValueError("unknown tenant operation")
            await self.session.execute(
                pg_insert(outbox)
                .values(
                    equestrian_id=message.equestrian_id,
                    operation_id=message.operation_id,
                    message_id=message.message_id,
                    kind=message.kind,
                    part_index=message.part_index,
                    payload=payload,
                    payload_hash=message.payload_hash,
                )
                .on_conflict_do_nothing()
            )
            rows = (
                (
                    await self.session.execute(
                        select(outbox).where(
                            or_(
                                outbox.c.message_id == message.message_id,
                                and_(
                                    *_scope(outbox, message.equestrian_id, message.operation_id),
                                    outbox.c.kind == message.kind,
                                    outbox.c.part_index.is_not_distinct_from(message.part_index),
                                ),
                            )
                        )
                    )
                )
                .mappings()
                .all()
            )
            if (
                len(rows) != 1
                or not _same_message(dict(rows[0]), message, payload)  # noqa: W503
                or (rows[0]["kind"], rows[0]["part_index"]) != (message.kind, message.part_index)  # noqa: W503
            ):
                raise ValueError("outbox identity conflict")

    async def claim_outbox(
        self, *, equestrian_id: UUID, now: datetime, duration: timedelta, limit: int = 100
    ) -> Sequence[OutboxClaim]:
        _time(now, duration)
        _limit(limit)
        # No tenant lock: SKIP LOCKED permits independent relays, no operation
        # effects occur here. The transaction must commit before network publish.
        with repository_errors():
            rows = (
                (
                    await self.session.execute(
                        select(outbox)
                        .where(
                            outbox.c.equestrian_id == equestrian_id,
                            outbox.c.published_at.is_(None),
                            outbox.c.next_attempt_at <= now,
                            or_(outbox.c.lease_until.is_(None), outbox.c.lease_until <= now),
                        )
                        .order_by(outbox.c.next_attempt_at, outbox.c.message_id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                )
                .mappings()
                .all()
            )
            claims = []
            for row in rows:
                lease = StorageLease(uuid4(), now + duration, row["attempts"] + 1)
                await self.session.execute(
                    update(outbox)
                    .where(
                        outbox.c.message_id == row["message_id"],
                        outbox.c.equestrian_id == equestrian_id,
                    )
                    .values(lease_token=lease.token, lease_until=lease.until, attempts=lease.attempts)
                )
                message = MaintenanceMessage(
                    equestrian_id,
                    row["operation_id"],
                    row["message_id"],
                    row["kind"],
                    json.dumps(row["payload"], sort_keys=True, separators=(",", ":")),
                    row["payload_hash"],
                    row["part_index"],
                )
                claims.append(OutboxClaim(message, lease))
            return claims

    async def finish_outbox(
        self, *, claim: OutboxClaim, now: datetime, published: bool, retry_after: timedelta = timedelta(seconds=1)
    ) -> bool:
        """published=True ONLY after PubAck. False retains intent without retry cap."""
        _time(now, retry_after)
        message, lease = claim.message, claim.lease
        values: dict[str, Any] = dict(lease_token=None, lease_until=None)
        if published:
            values.update(published_at=now, last_error_code=None)
        else:
            if not timedelta(seconds=1) <= retry_after <= timedelta(seconds=60):
                raise ValueError("retry outside 1..60 seconds")
            values.update(next_attempt_at=now + retry_after, last_error_code="publish_retry")
        with repository_errors():
            row = (
                await self.session.execute(
                    update(outbox)
                    .where(
                        *_scope(outbox, message.equestrian_id, message.operation_id),
                        outbox.c.message_id == message.message_id,
                        outbox.c.payload_hash == message.payload_hash,
                        outbox.c.lease_token == lease.token,
                        outbox.c.attempts == lease.attempts,
                        outbox.c.lease_until == lease.until,
                        outbox.c.lease_until > now,
                        outbox.c.lease_until > func.clock_timestamp(),
                        outbox.c.published_at.is_(None),
                    )
                    .values(**values)
                    .returning(outbox.c.message_id)
                )
            ).first()
            return row is not None

    async def get_inbox_result(
        self, *, consumer: str, equestrian_id: UUID, operation_id: UUID
    ) -> MaintenanceMessage | None:
        with repository_errors():
            rows = (
                (
                    await self.session.execute(
                        select(inbox)
                        .where(*_scope(inbox, equestrian_id, operation_id), inbox.c.consumer == consumer)
                        .limit(2)
                    )
                )
                .mappings()
                .all()
            )
        if len(rows) > 1:
            raise ValueError("conflicting operation results")
        if not rows:
            return None
        row = rows[0]
        encoded = json.dumps(row["payload"], sort_keys=True, separators=(",", ":"), allow_nan=False)
        return MaintenanceMessage(
            equestrian_id,
            operation_id,
            row["message_id"],
            "merge_completed" if consumer == MERGE_CONSUMER else "cleanup_completed",
            encoded,
            row["payload_hash"],
        )

    async def record_result(self, *, message: MaintenanceMessage, now: datetime) -> bool:
        _time(now)
        parsed = parse_result(message)
        consumer = MERGE_CONSUMER if isinstance(parsed, MergeCompleted) else CLEANUP_CONSUMER
        await self.lock_tenant(equestrian_id=message.equestrian_id)
        with repository_errors():
            async with self.session.begin_nested():
                previous = await self.get_inbox_result(
                    consumer=consumer, equestrian_id=message.equestrian_id, operation_id=message.operation_id
                )
                if previous is not None and (
                    previous.message_id != message.message_id or previous.payload_hash != message.payload_hash
                ):
                    raise ValueError("conflicting operation result")
                if isinstance(parsed, MergeCompleted):
                    revision = await self.get_revision(
                        equestrian_id=message.equestrian_id, operation_id=message.operation_id
                    )
                    if revision is None or revision.status in (RevisionStatus.PENDING, RevisionStatus.BLOCKED):
                        raise ValueError("unknown or inactive result operation")
                    checkpoint = await self.get_checkpoint(
                        equestrian_id=message.equestrian_id, operation_id=message.operation_id
                    )
                    validate_correlation(revision, checkpoint, parsed)
                    if revision.status == RevisionStatus.FINALIZED and previous is None:
                        raise ValueError("unknown finalized result")
                inserted = await self.accept_inbox(consumer=consumer, message=message)
                if isinstance(parsed, MergeCompleted):
                    if parsed.status == "superseded_by_delete":
                        await self.session.execute(
                            update(revisions)
                            .where(
                                *_scope(revisions, message.equestrian_id, message.operation_id),
                                revisions.c.status.not_in(("finalized", "deleting")),
                            )
                            .values(
                                status="awaiting_cleanup",
                                cleanup_operation_id=parsed.cleanup_operation_id,
                                cleanup_seq=parsed.cleanup_seq,
                            )
                        )
                else:
                    # Retained processed cleanup is still a durable correlation ledger.
                    await self.mark_inbox_processed(consumer=consumer, message=message, now=now)
                return inserted

    async def _delete_result_ready(
        self, tenant: UUID, operation: UUID, checkpoint: ScanCheckpoint, message: MaintenanceMessage
    ) -> bool:
        parsed = parse_result(message)
        if not isinstance(parsed, MergeCompleted):
            raise ValueError("merge result required")
        saved = await self.get_inbox_result(consumer=MERGE_CONSUMER, equestrian_id=tenant, operation_id=operation)
        if saved is None or (saved.message_id, saved.payload_hash) != (message.message_id, message.payload_hash):
            raise ValueError("unknown merge result")
        revision = await self.get_revision(equestrian_id=tenant, operation_id=operation)
        if revision is None:
            raise ValueError("unknown tenant operation")
        validate_correlation(revision, checkpoint, parsed)
        if not checkpoint.sealed:
            return False
        if parsed.status == "superseded_by_delete":
            assert parsed.cleanup_operation_id is not None
            cleanup = await self.get_inbox_result(
                consumer=CLEANUP_CONSUMER, equestrian_id=tenant, operation_id=parsed.cleanup_operation_id
            )
            if cleanup is None:
                return False
            completed = parse_result(cleanup)
            if not isinstance(completed, CleanupCompleted) or completed.cleanup_seq != parsed.cleanup_seq:
                raise ValueError("cleanup correlation mismatch")
        return revision.status in (
            RevisionStatus.AWAITING_RESULT,
            RevisionStatus.AWAITING_CLEANUP,
            RevisionStatus.DELETING,
        )

    async def commit_delete_batch(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        lease: StorageLease,
        expected: ScanCheckpoint,
        result: MaintenanceMessage,
        finalized: MaintenanceMessage,
        now: datetime,
        limit: int = MAX_BATCH,
    ) -> ScanCheckpoint:
        _limit(limit)
        _time(now)
        await self.lock_tenant(equestrian_id=equestrian_id)
        with repository_errors():
            async with self.session.begin_nested():
                await self._fence_scan(equestrian_id, operation_id, lease, expected, now)
                if not await self._delete_result_ready(equestrian_id, operation_id, expected, result):
                    raise ValueError("deletion barriers not satisfied")
                revision = await self.get_revision(equestrian_id=equestrian_id, operation_id=operation_id)
                assert revision is not None
                terminal = MergeFinalized.model_validate(_payload(finalized))
                if (
                    finalized.kind != "finalized"
                    or finalized.part_index is not None  # noqa: W503
                    or (finalized.equestrian_id, finalized.operation_id, finalized.message_id)  # noqa: W503
                    != (equestrian_id, operation_id, terminal.message_id)  # noqa: W503
                    or (  # noqa: W503
                        terminal.equestrian_id,
                        terminal.operation_id,
                        terminal.normalization_revision,
                        terminal.result_message_id,
                    )
                    != (equestrian_id, operation_id, revision.revision, result.message_id)  # noqa: W503
                ):
                    raise ValueError("invalid finalization correlation")
                batch = await self.get_mappings(
                    equestrian_id=equestrian_id,
                    operation_id=operation_id,
                    after_seq=expected.delete_cursor,
                    limit=limit,
                )
                if batch:
                    ids = [item.source_id for item in batch]
                    # Defensive guard: never remove a target, even if the ledger is corrupt.
                    if (
                        await self.session.scalar(
                            select(redirects.c.target_id)
                            .where(*_scope(redirects, equestrian_id, operation_id), redirects.c.target_id.in_(ids))
                            .limit(1)
                        )
                        is not None
                    ):
                        raise ValueError("source is a canonical target")
                    await self.session.execute(
                        delete(pages).where(pages.c.equestrian_id == equestrian_id, pages.c.id.in_(ids))
                    )
                checkpoint = replace(expected, delete_cursor=batch[-1].source_scan_seq) if batch else expected
                # Exact-size batches deliberately finish in the next empty transaction.
                done = len(batch) < limit
                await self.session.execute(
                    update(revisions)
                    .where(*_scope(revisions, equestrian_id, operation_id))
                    .values(status="finalized" if done else "deleting")
                )
                if done:
                    await self.enqueue_outbox(message=finalized)
                    await self.mark_inbox_processed(consumer=MERGE_CONSUMER, message=result, now=now)
                changed = (
                    await self.session.execute(
                        update(scans)
                        .where(
                            *_scope(scans, equestrian_id, operation_id),
                            scans.c.lease_token == lease.token,
                            scans.c.lease_until > now,
                            scans.c.lease_until > func.clock_timestamp(),
                        )
                        .values(delete_cursor=checkpoint.delete_cursor, lease_token=None, lease_until=None)
                        .returning(scans.c.operation_id)
                    )
                ).first()
                if changed is None:
                    raise ValueError("operation lease expired during deletion")
                return checkpoint
