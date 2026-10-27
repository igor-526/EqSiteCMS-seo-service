from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from core.entities.parse_dispatch import ParseDispatchTrigger
from core.exceptions import (
    ParseProviderUnknownError,
    ParseQueueUnavailableError,
    ParseTargetMissingError,
    ParseTargetsUnavailableError,
)
from core.protocols.parsing.provider import ParseProviderProtocol, ParseTarget
from core.services.parsing.registry import ParseProviderRegistry
from providers.ya_metrica import YaMetricaParseProvider


@dataclass
class DemoProvider:
    code: str = "demo"
    subject: str = "commands.parse.demo.run"

    async def list_targets(self, *, offset: int, limit: int) -> tuple[list[ParseTarget], int]:
        return [], 0

    async def is_target(self, *, equestrian_id) -> bool:
        return True

    def build_params(self, *, trigger: ParseDispatchTrigger) -> dict[str, object]:
        return {}


def test_registry_is_generic_and_preserves_enabled_order() -> None:
    demo: ParseProviderProtocol = DemoProvider()
    metrica: ParseProviderProtocol = YaMetricaParseProvider(client=AsyncMock())
    registry = ParseProviderRegistry(enabled_codes=["demo", "ya_metrica"])
    registry.register(metrica)
    registry.register(demo)
    assert registry.registered_codes == ("ya_metrica", "demo")
    assert registry.get("demo") is demo
    assert registry.enabled() == [demo, metrica]
    with pytest.raises(ValueError, match="already registered"):
        registry.register(demo)


@pytest.mark.parametrize("code", ["missing", "demo"])
def test_unknown_and_disabled_are_safe_400(code: str) -> None:
    registry = ParseProviderRegistry(enabled_codes=["ya_metrica"])
    registry.register(DemoProvider())
    with pytest.raises(ParseProviderUnknownError) as caught:
        registry.get(code)
    assert caught.value.status_code == 400
    assert str(caught.value) == ParseProviderUnknownError.message


@pytest.mark.parametrize("provider", [DemoProvider("bad-secret!"), DemoProvider(subject="wrong-secret")])
def test_registration_rejects_invalid_identity(provider: DemoProvider) -> None:
    registry = ParseProviderRegistry(enabled_codes=["demo"])
    with pytest.raises(ValueError) as caught:
        registry.register(provider)
    assert "secret" not in str(caught.value)
    assert registry.registered_codes == ()


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ParseProviderUnknownError, 400),
        (ParseTargetMissingError, 400),
        (ParseTargetsUnavailableError, 502),
        (ParseQueueUnavailableError, 503),
    ],
)
def test_errors_have_constant_safe_messages(error, status: int) -> None:
    assert error().status_code == status
    assert str(error()) == error.message
