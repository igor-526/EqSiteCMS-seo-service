from core.entities import Page
from models import pages

from .abstract_repository import ResolvableRepository


class PageRepository(ResolvableRepository[Page]):
    table = pages
    entity = Page
    value_column = "page"
    hash_column = "page_hash"
