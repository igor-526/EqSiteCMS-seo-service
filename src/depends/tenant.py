from typing import Annotated
from uuid import UUID

from fastapi import Header

from core.exceptions import UnauthorizedError


def get_equestrian_id(
    selector: Annotated[str | None, Header(alias="X-Equestrian-Id")] = None,
) -> UUID:
    """Selector is an identity hint, never a credential or registry lookup."""
    if selector is None or not selector.strip():
        raise UnauthorizedError
    try:
        return UUID(selector)
    except ValueError:
        raise UnauthorizedError from None
