from containers.clients import ClientsContainer
from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer

settings_container = SettingsContainer()
repositories_container = RepositoriesContainer()
clients_container = ClientsContainer(settings_container=settings_container)
services_container = ServicesContainer(
    repositories_container=repositories_container,
    settings_container=settings_container,
    clients_container=clients_container,
)

# Fail configuration early, without opening database or network connections.
clients_container.parse_provider_registry()
