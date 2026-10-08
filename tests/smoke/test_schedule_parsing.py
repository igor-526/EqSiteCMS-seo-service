"""Smoke tests for schedule_parsing Celery task (without real infrastructure)."""

from datetime import datetime
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from models.parsing_schedule import Base, ParsingSchedule
from tasks.schedule_parsing import _schedule_parsing_async, _should_run_now


@pytest.fixture
async def async_session() -> AsyncGenerator[AsyncSession, None]:
    """Create in-memory SQLite async session for testing."""
    # In-memory SQLite for testing
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )
    
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Create session
    async_session_maker = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    async with async_session_maker() as session:
        yield session
    
    await engine.dispose()


@pytest.fixture
def mock_task_publisher() -> MagicMock:
    """Create mock TaskPublisher."""
    mock_publisher = MagicMock()
    mock_publisher.publish_task = AsyncMock(return_value=("task-id-123", False))
    return mock_publisher


@pytest.mark.asyncio
async def test_schedule_parsing_with_no_schedules(
    async_session: AsyncSession,
    mock_task_publisher: MagicMock,
) -> None:
    """Test schedule_parsing when there are no schedules in DB."""
    with patch("tasks.schedule_parsing.SessionFactory") as mock_session_factory, \
         patch("tasks.schedule_parsing.get_task_publisher", return_value=mock_task_publisher):
        
        # Mock SessionFactory to return our test session
        mock_session_factory.return_value.__aenter__.return_value = async_session
        
        result = await _schedule_parsing_async("test-task-123")
        
        assert result["schedules_checked"] == 0
        assert result["tasks_published"] == 0
        assert result["errors"] == 0
        
        # Publisher should not be called
        mock_task_publisher.publish_task.assert_not_called()


@pytest.mark.asyncio
async def test_schedule_parsing_with_inactive_schedule(
    async_session: AsyncSession,
    mock_task_publisher: MagicMock,
) -> None:
    """Test schedule_parsing ignores inactive schedules."""
    # Add inactive schedule
    schedule = ParsingSchedule(
        site_id=1,
        parser_type="yandex_metrics",
        cron_expression="*/5 * * * *",  # Every 5 minutes
        is_active=False,
    )
    async_session.add(schedule)
    await async_session.commit()
    
    with patch("tasks.schedule_parsing.SessionFactory") as mock_session_factory, \
         patch("tasks.schedule_parsing.get_task_publisher", return_value=mock_task_publisher):
        
        mock_session_factory.return_value.__aenter__.return_value = async_session
        
        result = await _schedule_parsing_async("test-task-123")
        
        assert result["schedules_checked"] == 0  # Inactive schedules are not checked
        assert result["tasks_published"] == 0
        assert result["errors"] == 0
        
        mock_task_publisher.publish_task.assert_not_called()


@pytest.mark.asyncio
async def test_schedule_parsing_with_active_schedule_should_run(
    async_session: AsyncSession,
    mock_task_publisher: MagicMock,
) -> None:
    """Test schedule_parsing publishes task when cron matches."""
    # Add active schedule that should run now (every 5 minutes)
    schedule = ParsingSchedule(
        site_id=42,
        parser_type="yandex_metrics",
        cron_expression="*/5 * * * *",  # Every 5 minutes
        is_active=True,
    )
    async_session.add(schedule)
    await async_session.commit()
    
    with patch("tasks.schedule_parsing.SessionFactory") as mock_session_factory, \
         patch("tasks.schedule_parsing.get_task_publisher", return_value=mock_task_publisher), \
         patch("tasks.schedule_parsing._should_run_now", return_value=True):
        
        mock_session_factory.return_value.__aenter__.return_value = async_session
        
        result = await _schedule_parsing_async("test-task-123")
        
        assert result["schedules_checked"] == 1
        assert result["tasks_published"] == 1
        assert result["errors"] == 0
        
        # Publisher should be called once
        mock_task_publisher.publish_task.assert_called_once()
        
        # Check call arguments
        call_kwargs = mock_task_publisher.publish_task.call_args.kwargs
        assert call_kwargs["parser_type"] == "yandex_metrics"
        assert call_kwargs["site_id"] == 42
        assert isinstance(call_kwargs["task_id"], str)
        assert len(call_kwargs["task_id"]) > 0  # UUID should be generated
        assert isinstance(call_kwargs["trace_id"], str)


@pytest.mark.asyncio
async def test_schedule_parsing_with_active_schedule_should_not_run(
    async_session: AsyncSession,
    mock_task_publisher: MagicMock,
) -> None:
    """Test schedule_parsing skips task when cron doesn't match."""
    # Add active schedule that should NOT run now
    schedule = ParsingSchedule(
        site_id=42,
        parser_type="yandex_metrics",
        cron_expression="0 0 * * *",  # Daily at midnight
        is_active=True,
    )
    async_session.add(schedule)
    await async_session.commit()
    
    with patch("tasks.schedule_parsing.SessionFactory") as mock_session_factory, \
         patch("tasks.schedule_parsing.get_task_publisher", return_value=mock_task_publisher), \
         patch("tasks.schedule_parsing._should_run_now", return_value=False):
        
        mock_session_factory.return_value.__aenter__.return_value = async_session
        
        result = await _schedule_parsing_async("test-task-123")
        
        assert result["schedules_checked"] == 1
        assert result["tasks_published"] == 0  # No tasks published
        assert result["errors"] == 0
        
        mock_task_publisher.publish_task.assert_not_called()


@pytest.mark.asyncio
async def test_schedule_parsing_with_multiple_schedules(
    async_session: AsyncSession,
    mock_task_publisher: MagicMock,
) -> None:
    """Test schedule_parsing with multiple active schedules."""
    # Add multiple active schedules
    schedules = [
        ParsingSchedule(
            site_id=1,
            parser_type="yandex_metrics",
            cron_expression="*/5 * * * *",
            is_active=True,
        ),
        ParsingSchedule(
            site_id=2,
            parser_type="google_analytics",
            cron_expression="*/10 * * * *",
            is_active=True,
        ),
        ParsingSchedule(
            site_id=3,
            parser_type="custom_parser",
            cron_expression="0 */6 * * *",
            is_active=True,
        ),
    ]
    
    for schedule in schedules:
        async_session.add(schedule)
    await async_session.commit()
    
    with patch("tasks.schedule_parsing.SessionFactory") as mock_session_factory, \
         patch("tasks.schedule_parsing.get_task_publisher", return_value=mock_task_publisher), \
         patch("tasks.schedule_parsing._should_run_now", return_value=True):
        
        mock_session_factory.return_value.__aenter__.return_value = async_session
        
        result = await _schedule_parsing_async("test-task-123")
        
        assert result["schedules_checked"] == 3
        assert result["tasks_published"] == 3
        assert result["errors"] == 0
        
        # Publisher should be called 3 times
        assert mock_task_publisher.publish_task.call_count == 3


@pytest.mark.asyncio
async def test_schedule_parsing_handles_publisher_error(
    async_session: AsyncSession,
    mock_task_publisher: MagicMock,
) -> None:
    """Test schedule_parsing handles errors during publishing gracefully."""
    # Add schedule
    schedule = ParsingSchedule(
        site_id=42,
        parser_type="yandex_metrics",
        cron_expression="*/5 * * * *",
        is_active=True,
    )
    async_session.add(schedule)
    await async_session.commit()
    
    # Mock publisher to raise exception
    mock_task_publisher.publish_task.side_effect = Exception("NATS connection error")
    
    with patch("tasks.schedule_parsing.SessionFactory") as mock_session_factory, \
         patch("tasks.schedule_parsing.get_task_publisher", return_value=mock_task_publisher), \
         patch("tasks.schedule_parsing._should_run_now", return_value=True):
        
        mock_session_factory.return_value.__aenter__.return_value = async_session
        
        result = await _schedule_parsing_async("test-task-123")
        
        assert result["schedules_checked"] == 1
        assert result["tasks_published"] == 0
        assert result["errors"] == 1  # Error should be recorded


@pytest.mark.asyncio
async def test_schedule_parsing_continues_after_error(
    async_session: AsyncSession,
    mock_task_publisher: MagicMock,
) -> None:
    """Test schedule_parsing continues processing after one schedule fails."""
    # Add multiple schedules
    schedules = [
        ParsingSchedule(
            site_id=1,
            parser_type="yandex_metrics",
            cron_expression="*/5 * * * *",
            is_active=True,
        ),
        ParsingSchedule(
            site_id=2,
            parser_type="google_analytics",
            cron_expression="*/10 * * * *",
            is_active=True,
        ),
    ]
    
    for schedule in schedules:
        async_session.add(schedule)
    await async_session.commit()
    
    # Mock publisher to fail on first call, succeed on second
    mock_task_publisher.publish_task.side_effect = [
        Exception("NATS connection error"),
        ("task-id-123", False),
    ]
    
    with patch("tasks.schedule_parsing.SessionFactory") as mock_session_factory, \
         patch("tasks.schedule_parsing.get_task_publisher", return_value=mock_task_publisher), \
         patch("tasks.schedule_parsing._should_run_now", return_value=True):
        
        mock_session_factory.return_value.__aenter__.return_value = async_session
        
        result = await _schedule_parsing_async("test-task-123")
        
        assert result["schedules_checked"] == 2
        assert result["tasks_published"] == 1  # Second one succeeded
        assert result["errors"] == 1  # First one failed


def test_should_run_now_with_every_5_minutes():
    """Test _should_run_now with */5 cron (every 5 minutes)."""
    # Test time: 12:05:00 (exactly at scheduled time)
    current_time = datetime(2024, 1, 1, 12, 5, 0)
    
    # Should run at 12:05 (within 5 minute window from 12:05)
    assert _should_run_now("*/5 * * * *", current_time) is True
    
    # Test time: 12:07:00 (2 minutes after 12:05 scheduled time)
    current_time = datetime(2024, 1, 1, 12, 7, 0)
    
    # Should run at 12:07 (still within 5 minute window from 12:05)
    assert _should_run_now("*/5 * * * *", current_time) is True
    
    # Test time: 12:12:00 (7 minutes after 12:05, but 2 minutes after 12:10)
    current_time = datetime(2024, 1, 1, 12, 12, 0)
    
    # Should run at 12:12 (within 5 minute window from 12:10)
    assert _should_run_now("*/5 * * * *", current_time) is True


def test_should_run_now_with_daily_cron():
    """Test _should_run_now with daily cron (0 0 * * *)."""
    # Test time: 00:03:00 (3 minutes after midnight)
    current_time = datetime(2024, 1, 1, 0, 3, 0)
    
    # Should run (within 5 minute window from midnight)
    assert _should_run_now("0 0 * * *", current_time) is True
    
    # Test time: 00:06:00 (6 minutes after midnight)
    current_time = datetime(2024, 1, 1, 0, 6, 0)
    
    # Should NOT run (outside 5 minute window)
    assert _should_run_now("0 0 * * *", current_time) is False
    
    # Test time: 12:00:00 (noon)
    current_time = datetime(2024, 1, 1, 12, 0, 0)
    
    # Should NOT run (not midnight)
    assert _should_run_now("0 0 * * *", current_time) is False


def test_should_run_now_with_invalid_cron():
    """Test _should_run_now with invalid cron expression."""
    current_time = datetime(2024, 1, 1, 12, 0, 0)
    
    # Invalid cron should return False and not raise exception
    assert _should_run_now("invalid cron", current_time) is False
    assert _should_run_now("", current_time) is False
    assert _should_run_now("* * * *", current_time) is False  # Missing field


def test_should_run_now_edge_cases():
    """Test _should_run_now edge cases."""
    # Create datetime with previous scheduled time
    # For "0 12 * * *" (daily at noon), we need to be within 5 minutes AFTER the scheduled time
    
    # Just before scheduled time (should not run - diff is negative)
    current_time = datetime(2024, 1, 1, 11, 59, 0)
    assert _should_run_now("0 12 * * *", current_time) is False
    
    # Exactly 1 minute after scheduled time (should run - within 5 min window)
    current_time = datetime(2024, 1, 1, 12, 1, 0)
    assert _should_run_now("0 12 * * *", current_time) is True
    
    # 4 minutes after scheduled time (should run - within 5 min window)
    current_time = datetime(2024, 1, 1, 12, 4, 0)
    assert _should_run_now("0 12 * * *", current_time) is True
    
    # Exactly 5 minutes after (boundary - should run)
    current_time = datetime(2024, 1, 1, 12, 5, 0)
    assert _should_run_now("0 12 * * *", current_time) is True
    
    # Just over 5 minutes after (should not run - outside window)
    current_time = datetime(2024, 1, 1, 12, 5, 1)
    assert _should_run_now("0 12 * * *", current_time) is False
