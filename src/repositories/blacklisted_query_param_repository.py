from core.entities import BlacklistedQueryParam
from models import blacklisted_query_params

from .abstract_repository import AbstractRepository


class BlacklistedQueryParamRepository(AbstractRepository[BlacklistedQueryParam]):
    table = blacklisted_query_params
    entity = BlacklistedQueryParam
    unique_constraint = "ux_blacklisted_query_params_equestrian_id_value"
