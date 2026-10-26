from dependency_injector import containers, providers

from depends.utils import get_session
from repositories import (
    BlacklistedHashRepository,
    BlacklistedQueryParamRepository,
    PageRepository,
    SearchQueryRepository,
)


class RepositoriesContainer(containers.DeclarativeContainer):
    session = providers.ContextLocalResource(get_session)

    search_query_repository = providers.Factory(SearchQueryRepository, session=session)
    page_repository = providers.Factory(PageRepository, session=session)
    blacklisted_query_param_repository = providers.Factory(BlacklistedQueryParamRepository, session=session)
    blacklisted_hash_repository = providers.Factory(BlacklistedHashRepository, session=session)
