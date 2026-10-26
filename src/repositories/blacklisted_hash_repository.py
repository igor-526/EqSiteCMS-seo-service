from core.entities import BlacklistedHash
from models import blacklisted_hashes

from .abstract_repository import AbstractRepository


class BlacklistedHashRepository(AbstractRepository[BlacklistedHash]):
    table = blacklisted_hashes
    entity = BlacklistedHash
    unique_constraint = "ux_blacklisted_hashes_equestrian_id_value"
