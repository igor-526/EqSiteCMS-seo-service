from dependency_injector import containers, providers

from settings import NatsSettings, ParseSettings, Settings


class SettingsContainer(containers.DeclarativeContainer):
    app_settings = providers.Singleton(Settings)

    nats_settings = providers.Singleton(NatsSettings)

    parse_settings = providers.Singleton(ParseSettings)
