import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from core.entities.parse_dispatch import ParseDispatchTrigger
from core.exceptions import ParseQueueUnavailableError, ParseTargetMissingError, ParseTargetsUnavailableError
from core.protocols.clients.parse_commands import ParseCommandPublisherProtocol
from core.protocols.parsing.provider import ParseProviderProtocol
from core.protocols.repositories.parse_dispatch_repository import ParseDispatchRepositoryProtocol
from core.schemas.messaging import ParseCommand, schedule_job_id
from core.services.parsing.registry import ParseProviderRegistry
from core.services.parsing.slots import SlotCalculator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AcceptedParseJob:
    job_id: UUID
    provider: str
    status: Literal["accepted"] = "accepted"


class ParseDispatchService:
    def __init__(
        self,
        *,
        publisher: ParseCommandPublisherProtocol,
        repository: ParseDispatchRepositoryProtocol,
        registry: ParseProviderRegistry,
        page_size: int,
        max_attempts: int,
        slots: SlotCalculator | None = None,
        lease: timedelta = timedelta(minutes=5),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if page_size < 1 or max_attempts < 1 or lease <= timedelta(0):
            raise ValueError("Invalid dispatch configuration")
        self._slots = slots
        self._publisher = publisher
        self._repository = repository
        self._registry = registry
        self._page_size = page_size
        self._max_attempts = max_attempts
        self._lease = lease
        self._clock = clock or (lambda: datetime.now(UTC))

    def _command(
        self, *, provider: ParseProviderProtocol, job_id: UUID, equestrian_id: UUID, trigger: ParseDispatchTrigger
    ) -> ParseCommand:
        return ParseCommand.model_validate(
            {
                "occurred_at": self._clock(),
                "job_id": job_id,
                "provider": provider.code,
                "equestrian_id": equestrian_id,
                "trigger": trigger.value,
                "params": provider.build_params(trigger=trigger),
            }
        )

    async def dispatch_manual(self, *, provider: str, equestrian_id: UUID) -> AcceptedParseJob:
        selected = self._registry.get(provider)
        if not await selected.is_target(equestrian_id=equestrian_id):
            raise ParseTargetMissingError()
        command = self._command(
            provider=selected, job_id=uuid4(), equestrian_id=equestrian_id, trigger=ParseDispatchTrigger.MANUAL
        )
        await self._publisher.publish(command=command)
        await self._repository.record_manual(
            job_id=command.job_id, provider=selected.code, equestrian_id=equestrian_id
        )
        return AcceptedParseJob(job_id=command.job_id, provider=selected.code)

    async def dispatch_scheduled_slot(self, *, provider: str, local_date: date) -> bool:
        """True only for a fully published pass; False requires another tick.

        A skipped claim is deliberately incomplete: it may be a live lease whose
        worker will crash. No ledger reads are needed; repeated passes are cheap.
        Listing errors propagate, so the runner must NOT cache this slot as done.
        """
        if self._slots is None:
            raise ValueError("Scheduled dispatch requires a slot calculator")
        selected = self._registry.get(provider)
        offset = 0
        complete = True
        while True:
            if not self._slot_is_current(local_date):
                return False
            targets, total = await selected.list_targets(offset=offset, limit=self._page_size)
            if not self._slot_is_current(local_date):
                return False
            if (not targets and offset < total) or len(targets) > self._page_size:
                raise ParseTargetsUnavailableError()
            for target in targets:
                if not self._slot_is_current(local_date, target.equestrian_id):
                    return False
                try:
                    if not await self._dispatch_target(
                        provider=selected, equestrian_id=target.equestrian_id, local_date=local_date
                    ):
                        complete = False
                except Exception:
                    # No exception text/traceback: peer/SQL errors may contain secrets.
                    logger.warning("Scheduled parse target failed")
                    complete = False
            offset += len(targets)
            if offset >= total:
                return complete

    def _slot_is_current(self, local_date: date, equestrian_id: UUID | None = None) -> bool:
        assert self._slots is not None
        return self._slots.is_current_slot(local_date=local_date, utc_now=self._clock(), equestrian_id=equestrian_id)

    async def _dispatch_target(
        self, *, provider: ParseProviderProtocol, equestrian_id: UUID, local_date: date
    ) -> bool:
        if not self._slot_is_current(local_date, equestrian_id):
            return False
        claim = await self._repository.claim_schedule(
            job_id=schedule_job_id(provider.code, equestrian_id, local_date),
            provider=provider.code,
            equestrian_id=equestrian_id,
            schedule_date=local_date,
            max_attempts=self._max_attempts,
            lease=self._lease,
        )
        if claim is None:
            return False
        if not self._slot_is_current(local_date, equestrian_id):
            await self._repository.mark_failed(job_id=claim.id, attempts=claim.attempts, error_code="slot_expired")
            return False
        try:
            command = self._command(
                provider=provider,
                job_id=claim.id,
                equestrian_id=equestrian_id,
                trigger=ParseDispatchTrigger.SCHEDULE,
            )
            if not self._slot_is_current(local_date, equestrian_id):
                await self._repository.mark_failed(job_id=claim.id, attempts=claim.attempts, error_code="slot_expired")
                return False
            await self._publisher.publish(command=command)
        except Exception as error:
            code = "nats_unavailable" if isinstance(error, ParseQueueUnavailableError) else "dispatch_failed"
            await self._repository.mark_failed(job_id=claim.id, attempts=claim.attempts, error_code=code)
            if claim.attempts >= self._max_attempts:
                logger.error("Scheduled parse dispatch attempts exhausted")
            else:
                logger.warning("Scheduled parse publication failed")
            return False
        # Mark failures leave pending for lease recovery, never overwrite PubAck with failed.
        return await self._repository.mark_published(job_id=claim.id, attempts=claim.attempts)
