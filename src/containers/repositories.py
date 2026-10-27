from dependency_injector import containers, providers

from depends.utils import get_session
from repositories import (
    BlacklistedHashRepository,
    BlacklistedQueryParamRepository,
    PageRepository,
    ParseDispatchRepository,
    SearchQueryRepository,
)
from utils.database import SessionFactory


class RepositoriesContainer(containers.DeclarativeContainer):
    session = providers.ContextLocalResource(get_session)

    search_query_repository = providers.Factory(SearchQueryRepository, session=session)
    page_repository = providers.Factory(PageRepository, session=session)
    blacklisted_query_param_repository = providers.Factory(BlacklistedQueryParamRepository, session=session)
    blacklisted_hash_repository = providers.Factory(BlacklistedHashRepository, session=session)

    parse_dispatch_repository = providers.Factory(
        ParseDispatchRepository, session_factory=providers.Object(SessionFactory)
    )
