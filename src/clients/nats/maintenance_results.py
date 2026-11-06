"""Opt-in result transport. Importing this module starts no tasks or connections."""

import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError

from clients.nats.client import NatsJetstreamClient
from core.entities.normalization import MaintenanceMessage
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol
from core.schemas.messaging.maintenance import CleanupCompleted, MergeCompleted
from core.services.normalization_finalization_service import NormalizationFinalizationService
from settings import NatsSettings

logger = logging.getLogger(__name__)
MERGE_SUBJECT = "events.metrica.pages.merge.completed"
CLEANUP_SUBJECT = "events.metrica.cleanup.completed"


class InvalidMaintenanceResult(ValueError):
    """Safe permanent transport/schema/correlation failure, without input text."""


def decode_result(*, subject: str, payload: bytes, headers: Mapping[str, str] | None) -> MaintenanceMessage:
    try:
        if len(payload) > 262144:
            raise ValueError
        body: MergeCompleted | CleanupCompleted
        if subject == MERGE_SUBJECT:
            body = MergeCompleted.model_validate_json(payload)
            kind = "merge_completed"
        elif subject == CLEANUP_SUBJECT:
            body = CleanupCompleted.model_validate_json(payload)
            kind = "cleanup_completed"
        else:
            raise ValueError
        # Both result subjects use status as the protocol identity suffix,
        # including merge superseded_by_delete. Reject before opening storage.
        expected_id = uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{subject}:{body.operation_id}:{body.status}")
        if body.message_id != expected_id or headers is None or headers.get("Nats-Msg-Id") != str(expected_id):
            raise ValueError
        # Hash the actual JSON, not a DTO dump (which could introduce defaults).
        canonical = json.dumps(json.loads(payload), sort_keys=True, separators=(",", ":"), allow_nan=False)
        return MaintenanceMessage(
            equestrian_id=body.equestrian_id,
            operation_id=body.operation_id,
            message_id=body.message_id,
            kind=kind,
            payload_json=payload.decode("utf-8"),
            payload_hash=sha256(canonical.encode()).hexdigest(),
        )
    except ValueError, TypeError, RecursionError:
        raise InvalidMaintenanceResult("invalid maintenance result") from None


class MaintenanceResultHandler:
    def __init__(
        self,
        *,
        transaction: Callable[[], AbstractAsyncContextManager[NormalizationRepositoryProtocol]],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._transaction = transaction
        self._clock = clock

    async def handle(self, *, subject: str, payload: bytes, headers: Mapping[str, str] | None) -> None:
        message = decode_result(subject=subject, payload=payload, headers=headers)
        async with self._transaction() as repository:
            try:
                await NormalizationFinalizationService(repository).accept_result(message=message, now=self._clock())
            except ValueError:
                raise InvalidMaintenanceResult("invalid maintenance correlation") from None
        # Successful exit includes COMMIT, even for an exact duplicate.


class MaintenanceResultConsumer:
    """One bound durable loop. Supervisor must await run(), not detach it.

    Fetch/ack transport failures propagate to that supervisor and invalidate its
    readiness. A handler never scans/deletes. No shared mutable DB session.
    """

    def __init__(
        self,
        *,
        client: NatsJetstreamClient,
        settings: NatsSettings,
        handler: MaintenanceResultHandler,
        subject: str,
    ) -> None:
        if subject not in (MERGE_SUBJECT, CLEANUP_SUBJECT):
            raise ValueError("unsupported result subject")
        self._client = client
        self._settings = settings
        self._handler = handler
        self._subject = subject
        self._bound = False
        self._storage_ok = True

    @property
    def is_ready(self) -> bool:
        return self._bound and self._storage_ok

    async def process(self, message: Msg) -> None:
        try:
            if message.subject != self._subject:
                raise InvalidMaintenanceResult("unexpected result subject")
            await self._handler.handle(subject=message.subject, payload=message.data, headers=message.headers)
        except InvalidMaintenanceResult:
            logger.warning("Maintenance result rejected: invalid schema or correlation")
            await message.term()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never log input, exception text, credentials or headers.
            attempts = message.metadata.num_delivered
            log = logger.error if attempts >= 5 else logger.warning
            log("Maintenance result storage retry: attempt=%s", attempts)
            self._storage_ok = False
            await message.nak(delay=min(2 ** min(max(attempts - 1, 0), 6), 60))
            return
        await message.ack()
        self._storage_ok = True

    async def run(self) -> None:
        durable = (
            self._settings.nats_consumer_merge_completed
            if self._subject == MERGE_SUBJECT
            else self._settings.nats_consumer_cleanup_completed
        )
        subscription = await self._client.jetstream.pull_subscribe_bind(
            durable=durable, stream=self._settings.nats_stream_maintenance_results
        )
        self._bound = True
        try:
            while True:
                try:
                    messages = await subscription.fetch(batch=1, timeout=1)
                except NatsTimeoutError:
                    continue
                for message in messages:
                    await self.process(message)
        finally:
            self._bound = False
            await subscription.unsubscribe()
