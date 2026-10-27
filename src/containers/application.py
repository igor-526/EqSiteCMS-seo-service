from containers.clients import ClientsContainer
from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer

settings_container = SettingsContainer()
repositories_container = RepositoriesContainer()
clients_container = ClientsContainer(settings_container=settings_container)
services_container = ServicesContainer(
    repositories_container=repositories_container, settings_container=settings_container
)
