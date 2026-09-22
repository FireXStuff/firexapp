#
# Fixtures shared by the firexkit unit tests.
#
# The firex repo has a richer set in src/firex/tests/conftest.py; the ones that
# don't reach for firex-only modules (firexuid, testsuites, microservices.celery)
# are named identically here so that the two test trees share a vocabulary.
#
import pytest

from firexkit.firex_celery import FireXCelery
from firexkit.testing import UtClient, ut_backed_celery_app


@pytest.fixture
def ut_app() -> FireXCelery:
    """A throwaway app whose backend stores to memory instead of redis."""
    return ut_backed_celery_app()


@pytest.fixture
def ut_client(ut_app: FireXCelery) -> UtClient:
    """The in-memory store behind :func:`ut_app`'s backend."""
    return ut_app.backend.client
