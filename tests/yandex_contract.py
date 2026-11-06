"""Synthetic private peer wire fixture; never contains real OAuth credentials."""

from typing import Any
from uuid import UUID


def connected_counter_payload(*, tenant: UUID, present: bool = True) -> dict[str, Any]:
    return {
        "binding_generation": 3,
        "cleanup_requested_seq": 2,
        "cleanup_completed_seq": 1,
        "counter": {
            "id": "11111111-1111-4111-8111-111111111111",
            "equestrian_id": str(tenant),
            "counter_id": "123",
            "yandex_account": {
                "id": "33333333-3333-4333-8333-333333333333",
                "yandex_id": "synthetic-id",
                "yandex_login": "synthetic-login",
                "display_name": "Synthetic account",
                "email": None,
                "yandex_phone": None,
            },
        }
        if present
        else None,
    }
