from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer

settings_container = SettingsContainer()
repositories_container = RepositoriesContainer()
services_container = ServicesContainer(repositories_container=repositories_container)
