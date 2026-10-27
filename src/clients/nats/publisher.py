import logging

from core.exceptions.parsing import ParseQueueUnavailableError
from core.protocols.clients.parse_commands import ParseCommandTransportProtocol, PublishedParseCommand
from core.schemas.messaging import ParseCommand, msg_id

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
