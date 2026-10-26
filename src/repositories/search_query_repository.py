from core.entities import SearchQuery
from models import search_queries

from .abstract_repository import ResolvableRepository


class SearchQueryRepository(ResolvableRepository[SearchQuery]):
    table = search_queries
    entity = SearchQuery
    value_column = "query"
    hash_column = "query_hash"
