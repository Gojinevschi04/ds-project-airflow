import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag() -> DagBag:
    return DagBag(include_examples=False)
