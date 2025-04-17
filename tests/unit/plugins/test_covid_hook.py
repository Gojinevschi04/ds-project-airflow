from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
from hooks.covid import CovidApiHook


def test_get_countries_success(mock_regions: dict[str, list[dict[str, str]]]) -> None:
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_regions

    with patch("hooks.covid.requests.request", return_value=mock_response):
        hook = CovidApiHook()
        result = hook.get_countries()

    assert isinstance(result, pd.DataFrame)
    assert result.iloc[0]["name"] == "Moldova"
    assert result.iloc[0]["iso"] == "MD"


def test_get_countries_error() -> None:
    mock_response = Mock()
    mock_response.status_code = 500

    with patch("hooks.covid.requests.request", return_value=mock_response):
        hook = CovidApiHook()
        result = hook.get_countries()

    assert result.empty


def test_get_covid_success(mock_covid_data: dict[str, dict[str, int]]) -> None:
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_covid_data

    with patch("hooks.covid.requests.request", return_value=mock_response):
        hook = CovidApiHook()
        result = hook.get_covid("MD", date(2021, 5, 1))

    assert result["confirmed"] == 100
    assert result["deaths"] == 5


def test_get_covid_error() -> None:
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.reason = "Not Found"

    with patch("hooks.covid.requests.request", return_value=mock_response):
        hook = CovidApiHook()
        result = hook.get_covid("MD", date(2021, 5, 1))

    assert result == {}
    assert hook.errors["code_response"] == 404
    assert hook.errors["error_messages"] == "Not Found"
