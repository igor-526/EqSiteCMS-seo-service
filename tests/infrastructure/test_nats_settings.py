"""Unit tests for NATS settings."""

from settings import NatsSettings


def test_nats_settings_defaults():
    """Test that NatsSettings has correct default values."""
    settings = NatsSettings()

    assert settings.nats_servers_raw == "nats://localhost:4222"
    assert settings.nats_servers == ["nats://localhost:4222"]
    assert settings.nats_stream_seo_tasks == "SEO_TASKS"
    assert settings.nats_error_report_after_attempts == 3


def test_nats_settings_multiple_servers():
    """Test parsing multiple NATS servers."""
    settings = NatsSettings(nats_servers_raw="nats://server1:4222, nats://server2:4222, nats://server3:4222")

    assert len(settings.nats_servers) == 3
    assert "nats://server1:4222" in settings.nats_servers
    assert "nats://server2:4222" in settings.nats_servers
    assert "nats://server3:4222" in settings.nats_servers


def test_nats_settings_custom_stream():
    """Test custom stream name."""
    settings = NatsSettings(nats_stream_seo_tasks="CUSTOM_STREAM")

    assert settings.nats_stream_seo_tasks == "CUSTOM_STREAM"
