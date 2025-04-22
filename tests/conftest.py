import datetime
from typing import Any

import pytest
from airflow.models import DagBag

REGIONS_LIST = dict[str, list[dict[str, str]]]
COVID_LIST = dict[str, dict[str, int]]
WEATHER_LIST = dict[str, dict[str, list[dict[str, Any]]]]
LOG_LIST = list(dict[str, Any])
WEATHER_INPUT = list(dict[str, list[dict[str, Any]]])


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


@pytest.fixture()
def mock_log_data() -> LOG_LIST:
    return [{
        "country_id": 1,
        "start_time": datetime.datetime(2025, 4, 1, 10, 0, 0),
        "end_time": datetime.datetime(2025, 4, 1, 10, 5, 0),
        "code_response": 200,
        "error_messages": "OK"
    }]


@pytest.fixture
def sample_weather_input() -> WEATHER_INPUT:
    return [
        {
            "date": "2024-04-10",
            "day": {
                "maxtemp_c": 15.0,
                "mintemp_c": 5.0,
                "avgtemp_c": 10.0,
                "condition": {"text": "Sunny"}
            }
        },
        {
            "date": "2024-04-11",
            "day": {
                "maxtemp_c": 18.0,
                "mintemp_c": 7.0,
                "avgtemp_c": 12.0,
                "condition": {"text": "Cloudy"}
            }
        }
    ]


@pytest.fixture
def expected_weather_output() -> list[dict[str, Any]]:
    return [
        {
            "date": "2024-04-10",
            "country_id": "MD",
            "maxtemp_c": 15.0,
            "mintemp_c": 5.0,
            "avgtemp_c": 10.0
        },
        {
            "date": "2024-04-11",
            "country_id": "MD",
            "maxtemp_c": 18.0,
            "mintemp_c": 7.0,
            "avgtemp_c": 12.0
        }
    ]
