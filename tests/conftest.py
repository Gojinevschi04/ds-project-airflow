from typing import Any

import pytest
from airflow.models import DagBag

REGIONS_LIST = dict[str, list[dict[str, str]]]
COVID_LIST = dict[str, dict[str, int]]
WEATHER_LIST = dict[str, dict[str, list[dict[str, Any]]]]


@pytest.fixture()
def dagbag() -> DagBag:
    return DagBag(include_examples=False)


@pytest.fixture()
def mock_regions() -> REGIONS_LIST:
    return {
        "data": [{"name": "Moldova", "iso": "MD"}]
    }


@pytest.fixture()
def mock_covid_data() -> COVID_LIST:
    return {
        "data": {"confirmed": 100, "deaths": 5}
    }


@pytest.fixture()
def mock_weather_data() -> WEATHER_LIST:
    return {
        "forecast": {
            "forecastday": [
                {"date": "2024-04-01", "day": {"maxtemp_c": 15.0}},
                {"date": "2024-04-02", "day": {"maxtemp_c": 17.5}}
            ]
        }
    }
