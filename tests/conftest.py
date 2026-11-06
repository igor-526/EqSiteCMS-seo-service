import os

os.environ["SENTRY_ENABLED"] = "false"


# Some older PG tests have no explicit infrastructure marker. Classify by the
# transitive fixture dependency before pytest applies -m selection: autonomous
# make test must never bootstrap PG, and mandatory acceptance must never skip.
import pytest  # noqa: E402


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    for item in items:
        if "qa_infra_context" in item.fixturenames:
            item.add_marker(pytest.mark.infrastructure)
