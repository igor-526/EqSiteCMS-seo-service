"""Smoke tests for TaskPublisher (without real NATS infrastructure)."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from infrastructure.nats.client import NatsJetstreamClient
from infrastructure.nats.publisher import TaskPublisher
from settings import NatsSettings


@pytest.fixture
def mock_nats_client() -> NatsJetstreamClient:
    """Create a mock NATS client."""
    mock_client = MagicMock(spec=NatsJetstreamClient)
    
    # Mock publish method
    mock_ack = MagicMock()
    mock_ack.duplicate = False
    mock_client.publish = AsyncMock(return_value=mock_ack)
    
    return mock_client


@pytest.fixture
def nats_settings() -> NatsSettings:
    """Create NATS settings."""
    return NatsSettings()


@pytest.fixture
def task_publisher(mock_nats_client: NatsJetstreamClient, nats_settings: NatsSettings) -> TaskPublisher:
    """Create TaskPublisher with mocked NATS client."""
    return TaskPublisher(mock_nats_client, nats_settings)


@pytest.mark.asyncio
async def test_publish_task_correct_subject(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task generates correct subject."""
    await task_publisher.publish_task(
        parser_type="yandex_metrics",
        task_id="test-task-123",
        site_id=42,
        params={"test": "value"},
        trace_id="trace-123",
    )
    
    # Verify publish was called
    mock_nats_client.publish.assert_called_once()
    
    # Check subject
    call_kwargs = mock_nats_client.publish.call_args.kwargs
    assert call_kwargs["subject"] == "seo.tasks.yandex_metrics"


@pytest.mark.asyncio
async def test_publish_task_correct_payload(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task generates correct payload."""
    await task_publisher.publish_task(
        parser_type="yandex_metrics",
        task_id="test-task-123",
        site_id=42,
        params={"test": "value", "nested": {"key": "value"}},
        trace_id="trace-123",
    )
    
    # Verify publish was called
    mock_nats_client.publish.assert_called_once()
    
    # Check payload
    call_kwargs = mock_nats_client.publish.call_args.kwargs
    payload_bytes = call_kwargs["payload"]
    payload = json.loads(payload_bytes.decode("utf-8"))
    
    assert payload == {
        "task_id": "test-task-123",
        "site_id": 42,
        "params": {"test": "value", "nested": {"key": "value"}},
    }


@pytest.mark.asyncio
async def test_publish_task_correct_headers(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task generates correct headers."""
    await task_publisher.publish_task(
        parser_type="yandex_metrics",
        task_id="test-task-123",
        site_id=42,
        params={"test": "value"},
        trace_id="trace-123",
    )
    
    # Verify publish was called
    mock_nats_client.publish.assert_called_once()
    
    # Check headers
    call_kwargs = mock_nats_client.publish.call_args.kwargs
    headers = call_kwargs["headers"]
    
    assert headers["trace_id"] == "trace-123"
    assert headers["Nats-Msg-Id"] == "test-task-123"


@pytest.mark.asyncio
async def test_publish_task_auto_generates_trace_id(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task auto-generates trace_id if not provided."""
    await task_publisher.publish_task(
        parser_type="yandex_metrics",
        task_id="test-task-123",
        site_id=42,
        params={"test": "value"},
        # trace_id не передан
    )
    
    # Verify publish was called
    mock_nats_client.publish.assert_called_once()
    
    # Check headers have trace_id
    call_kwargs = mock_nats_client.publish.call_args.kwargs
    headers = call_kwargs["headers"]
    
    assert "trace_id" in headers
    assert len(headers["trace_id"]) > 0  # UUID should be generated


@pytest.mark.asyncio
async def test_publish_task_returns_task_id_and_duplicate_false(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task returns correct task_id and duplicate flag."""
    message_id, duplicate = await task_publisher.publish_task(
        parser_type="yandex_metrics",
        task_id="test-task-123",
        site_id=42,
        params={"test": "value"},
        trace_id="trace-123",
    )
    
    assert message_id == "test-task-123"
    assert duplicate is False


@pytest.mark.asyncio
async def test_publish_task_detects_duplicate(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task detects duplicate messages."""
    # Setup mock to return duplicate=True
    mock_ack = MagicMock()
    mock_ack.duplicate = True
    mock_nats_client.publish.return_value = mock_ack
    
    message_id, duplicate = await task_publisher.publish_task(
        parser_type="yandex_metrics",
        task_id="test-task-123",
        site_id=42,
        params={"test": "value"},
        trace_id="trace-123",
    )
    
    assert message_id == "test-task-123"
    assert duplicate is True


@pytest.mark.asyncio
async def test_publish_task_different_parser_types(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task works with different parser types."""
    parser_types = ["yandex_metrics", "google_analytics", "custom_parser"]
    
    for parser_type in parser_types:
        mock_nats_client.reset_mock()
        
        await task_publisher.publish_task(
            parser_type=parser_type,
            task_id=f"test-task-{parser_type}",
            site_id=42,
            params={},
            trace_id="trace-123",
        )
        
        # Check subject
        call_kwargs = mock_nats_client.publish.call_args.kwargs
        expected_subject = f"seo.tasks.{parser_type}"
        assert call_kwargs["subject"] == expected_subject


@pytest.mark.asyncio
async def test_publish_task_propagates_exception(
    task_publisher: TaskPublisher,
    mock_nats_client: MagicMock,
) -> None:
    """Test that publish_task propagates exceptions from NATS client."""
    # Setup mock to raise exception
    mock_nats_client.publish.side_effect = Exception("NATS connection error")
    
    with pytest.raises(Exception, match="NATS connection error"):
        await task_publisher.publish_task(
            parser_type="yandex_metrics",
            task_id="test-task-123",
            site_id=42,
            params={"test": "value"},
            trace_id="trace-123",
        )
