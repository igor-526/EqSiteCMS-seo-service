from dependency_injector import containers, providers

from containers.settings import SettingsContainer
from core.services import (
    BlacklistedHashService,
    BlacklistedQueryParamService,
    PageNormalizer,
    PageService,
    SearchQueryNormalizer,
    SearchQueryService,
)
from core.services.metrics_period import MetricsPeriodResolver
from core.services.metrics_table.builder import MetricsTableBuilder
from core.services.metrics_table.header_formatter import PeriodHeaderFormatter
from core.services.metrics_table.registry import MetricRegistry
from core.services.metrics_table.value_formatter import MetricValueFormatter


class ServicesContainer(containers.DeclarativeContainer):
    repositories_container = providers.DependenciesContainer()
    settings_container = providers.Container(SettingsContainer)

    metrics_period_resolver = providers.Factory(
        MetricsPeriodResolver, utc_offset_hours=settings_container.app_settings.provided.seo_utc_offset_hours
    )
    metric_registry = providers.Factory(MetricRegistry)
    metric_value_formatter = providers.Factory(MetricValueFormatter)
    period_header_formatter = providers.Factory(PeriodHeaderFormatter)
    metrics_table_builder = providers.Factory(
        MetricsTableBuilder,
        registry=metric_registry,
        value_formatter=metric_value_formatter,
        header_formatter=period_header_formatter,
    )

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
