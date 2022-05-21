import pytest

from mayim.registry import Registry


@pytest.fixture(autouse=True)
def reset_registry():
    Registry().reset()
