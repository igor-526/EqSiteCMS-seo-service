from dependency_injector import containers, providers

from clients.nats import NatsJetstreamClient, ParseCommandPublisher
from clients.ya_metrica import YaMetricaClient
from clients.yandex_service import YandexServiceClient
from containers.settings import SettingsContainer
from core.protocols.parsing.provider import ParseProviderProtocol
from core.services.parsing.registry import ParseProviderRegistry
from providers.ya_metrica import YaMetricaParseProvider
from settings import ParseSettings


def build_parse_registry(*, settings: ParseSettings, provider: ParseProviderProtocol) -> ParseProviderRegistry:
    registry = ParseProviderRegistry(enabled_codes=settings.parse_providers_enabled)
    registry.register(provider)
    settings.validate_registered_providers(registry.registered_codes)
    return registry


class ClientsContainer(containers.DeclarativeContainer):
    settings_container = providers.Container(SettingsContainer)

    nats_client = providers.Singleton(NatsJetstreamClient, settings=settings_container.nats_settings)
    parse_command_publisher = providers.Factory(ParseCommandPublisher, client=nats_client)

    ya_metrica_client = providers.Factory(
        YaMetricaClient,
        base_url=settings_container.app_settings.provided.ya_metrica_service_url,
        timeout=settings_container.app_settings.provided.ya_metrica_timeout_seconds,
    )

    yandex_service_client = providers.Factory(
        YandexServiceClient,
        base_url=settings_container.parse_settings.provided.yandex_service_url,
        timeout=settings_container.parse_settings.provided.yandex_service_timeout_seconds,
    )
    ya_metrica_parse_provider = providers.Factory(YaMetricaParseProvider, client=yandex_service_client)
    parse_provider_registry = providers.Singleton(
        build_parse_registry,
        settings=settings_container.parse_settings,
        provider=ya_metrica_parse_provider,
    )
