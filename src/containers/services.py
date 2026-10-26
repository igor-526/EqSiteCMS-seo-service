from dependency_injector import containers, providers

from core.services import (
    BlacklistedHashService,
    BlacklistedQueryParamService,
    PageNormalizer,
    PageService,
    SearchQueryNormalizer,
    SearchQueryService,
)


class ServicesContainer(containers.DeclarativeContainer):
    repositories_container = providers.DependenciesContainer()

    search_query_normalizer = providers.Factory(SearchQueryNormalizer)
    page_normalizer = providers.Factory(PageNormalizer)

    search_query_service = providers.Factory(
        SearchQueryService,
        search_query_repository=repositories_container.search_query_repository,
        search_query_normalizer=search_query_normalizer,
    )
    page_service = providers.Factory(
        PageService,
        page_repository=repositories_container.page_repository,
        page_normalizer=page_normalizer,
        blacklisted_query_param_repository=repositories_container.blacklisted_query_param_repository,
        blacklisted_hash_repository=repositories_container.blacklisted_hash_repository,
    )
    blacklisted_query_param_service = providers.Factory(
        BlacklistedQueryParamService,
        blacklisted_query_param_repository=repositories_container.blacklisted_query_param_repository,
    )
    blacklisted_hash_service = providers.Factory(
        BlacklistedHashService,
        blacklisted_hash_repository=repositories_container.blacklisted_hash_repository,
    )
