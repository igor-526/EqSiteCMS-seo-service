from dependency_injector import containers, providers

from clients.ya_metrica import YaMetricaClient
from containers.settings import SettingsContainer


class ClientsContainer(containers.DeclarativeContainer):
    settings_container = providers.Container(SettingsContainer)

    ya_metrica_client = providers.Factory(
        YaMetricaClient,
        base_url=settings_container.app_settings.provided.ya_metrica_service_url,
        timeout=settings_container.app_settings.provided.ya_metrica_timeout_seconds,
    )
