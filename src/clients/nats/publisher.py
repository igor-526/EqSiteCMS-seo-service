import json
import logging
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from core.entities.normalization import MaintenanceMessage
from core.exceptions.parsing import ParseQueueUnavailableError
from core.protocols.clients.parse_commands import ParseCommandTransportProtocol, PublishedParseCommand
from core.schemas.messaging import ParseCommand, msg_id
from core.schemas.messaging.maintenance import MergeFinalized, MergeMappings, MergeSeal, merge_command

logger = logging.getLogger(__name__)


class ParseCommandPublisher:
    def __init__(self, *, client: ParseCommandTransportProtocol) -> None:
        self._client = client

    async def publish(self, *, command: ParseCommand) -> PublishedParseCommand:
        subject = f"commands.parse.{command.provider}.run"
        message_id = msg_id(command.provider, command.job_id)
        payload = command.model_dump_json(exclude_none=True).encode("utf-8")
        try:
            ack = await self._client.publish(
                subject=subject, payload=payload, headers={"Nats-Msg-Id": str(message_id)}
            )
        except Exception as error:
            # Never include transport errors/payloads in public messages or logs.
            logger.warning("Parse command publication failed: provider=%s job_id=%s", command.provider, command.job_id)
            raise ParseQueueUnavailableError() from error
        duplicate = bool(ack.duplicate)
        if duplicate:
            logger.warning(
                "Parse command deduplicated: provider=%s job_id=%s message_id=%s",
                command.provider,
                command.job_id,
                message_id,
            )
        return PublishedParseCommand(job_id=command.job_id, message_id=message_id, duplicate=duplicate)


class MaintenancePublisher:
    """Publish the saved body/ID, never a freshly serialized envelope."""

    def __init__(self, *, client: ParseCommandTransportProtocol) -> None:
        self._client = client

    async def publish(self, *, message: MaintenanceMessage) -> bool:
        body: MergeMappings | MergeSeal | MergeFinalized
        payload = message.payload_json.encode("utf-8")
        if len(payload) > 262144:
            raise ValueError("maintenance body too large")
        if message.kind in ("mappings", "seal"):
            body = merge_command.validate_json(payload)
            subject = "commands.seo.pages.merge"
            suffix = str(body.part_index) if isinstance(body, MergeMappings) else "seal"
            if body.kind != message.kind or message.part_index != (
                body.part_index if isinstance(body, MergeMappings) else None
            ):
                raise ValueError("maintenance part mismatch")
        elif message.kind == "finalized":
            body = MergeFinalized.model_validate_json(payload)
            subject = "events.seo.pages.merge.finalized"
            suffix = f"finalized:{body.result_message_id}"
            if message.part_index is not None:
                raise ValueError("finalized has no part")
        else:
            raise ValueError("unsupported maintenance kind")
        canonical = json.dumps(json.loads(payload), sort_keys=True, separators=(",", ":"), allow_nan=False)
        expected_id = uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{subject}:{message.operation_id}:{suffix}")
        if any(
            (
                body.message_id != message.message_id,
                expected_id != message.message_id,
                body.operation_id != message.operation_id,
                body.equestrian_id != message.equestrian_id,
                sha256(canonical.encode()).hexdigest() != message.payload_hash,
            )
        ):
            raise ValueError("maintenance identity/hash mismatch")
        ack = await self._client.publish(
            subject=subject, payload=payload, headers={"Nats-Msg-Id": str(message.message_id)}
        )
        duplicate = bool(ack.duplicate)
        if duplicate:
            logger.warning("Maintenance message deduplicated: message_id=%s", message.message_id)
        return duplicate
