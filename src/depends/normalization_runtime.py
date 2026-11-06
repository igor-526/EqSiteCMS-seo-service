"""Composition boundary for short scan/delete transactions, not HTTP startup.

Both repositories in a scan share exactly one fresh session. No ContextLocalResource
or request session is reused, and no service escapes its transaction lifetime.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.protocols.normalization_runtime import NormalizationBatchProtocol
from core.services.normalization_finalization_service import NormalizationFinalizationService
from core.services.normalization_scan_service import NormalizationScanService
from core.services.page_normalizer import PageNormalizer
from repositories.normalization_repository import NormalizationRepository
from repositories.page_repository import PageRepository

if TYPE_CHECKING:
    from maintenance.runtime import MaintenanceRuntime
    from settings import NormalizationSettings


class NormalizationBatchTransactions:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        normalizer: PageNormalizer,
        scan_batch_size: int = 500,
        delete_batch_size: int = 500,
    ) -> None:
        if not 1 <= scan_batch_size <= 500 or not 1 <= delete_batch_size <= 500:
            raise ValueError("invalid batch bounds")
        self._session_factory = session_factory
        self._normalizer = normalizer
        self._scan_batch_size = scan_batch_size
        self._delete_batch_size = delete_batch_size

    @asynccontextmanager
    async def scan(self) -> AsyncIterator[NormalizationBatchProtocol]:
        async with self._session_factory() as session, session.begin():
            yield NormalizationScanService(
                NormalizationRepository(session),
                PageRepository(session),
                self._normalizer,
                batch_size=self._scan_batch_size,
            )

    @asynccontextmanager
    async def finalization(self) -> AsyncIterator[NormalizationBatchProtocol]:
        async with self._session_factory() as session, session.begin():
            yield NormalizationFinalizationService(
                NormalizationRepository(session), batch_size=self._delete_batch_size
            )


def build_runtime(*, settings: NormalizationSettings) -> MaintenanceRuntime:
    """Separate process composition; no HTTP/request resources or parse runner."""
    from clients.nats.maintenance_results import (
        CLEANUP_SUBJECT,
        MERGE_SUBJECT,
        MaintenanceResultConsumer,
        MaintenanceResultHandler,
    )
    from containers.application import clients_container, settings_container
    from core.services.normalization_dispatcher import NormalizationDispatcher
    from core.services.normalization_outbox_relay import NormalizationOutboxRelay
    from maintenance.runtime import MaintenanceRuntime
    from repositories.normalization_transaction import NormalizationTransaction
    from utils.database import SessionFactory

    transaction = NormalizationTransaction(SessionFactory)
    batches = NormalizationBatchTransactions(
        SessionFactory,
        normalizer=PageNormalizer(),
        scan_batch_size=settings.normalization_scan_batch_size,
        delete_batch_size=settings.normalization_delete_batch_size,
    )
    client = clients_container.nats_client()
    handler = MaintenanceResultHandler(transaction=transaction)
    consumers = [
        MaintenanceResultConsumer(
            client=client, settings=settings_container.nats_settings(), handler=handler, subject=subject
        )
        for subject in (MERGE_SUBJECT, CLEANUP_SUBJECT)
    ]
    relay = NormalizationOutboxRelay(
        transaction=transaction,
        publisher=clients_container.maintenance_publisher(),
        batch_size=settings.normalization_relay_batch_size,
    )
    return MaintenanceRuntime(
        client=client,
        consumers=consumers,
        dispatcher=NormalizationDispatcher(
            transaction=transaction,
            scan=batches.scan,
            finalization=batches.finalization,
            relay=relay,
            tenant_page_size=settings.normalization_tenant_page_size,
        ),
        settings=settings,
    )
