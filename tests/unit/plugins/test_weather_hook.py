from datetime import date
from unittest.mock import Mock, patch

from hooks.weather import WeatherApiHook

from tests.conftest import WEATHER_LIST


def test_get_weather_success(mock_weather_data: WEATHER_LIST) -> None:

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_weather_data

    mock_parsed_data = [
        {"date": "2024-04-01", "temp": 15.0},
        {"date": "2024-04-02", "temp": 17.5}
    ]

    with patch("hooks.weather.requests.request", return_value=mock_response), \
         patch("hooks.weather.build_weather_data", return_value=mock_parsed_data):

        hook = WeatherApiHook(api_key="test-key")
        result = hook.get_weather("MD", "MDA", date(2024, 4, 1), date(2024, 4, 2))

    assert result == mock_parsed_data


def test_get_weather_error() -> None:
    mock_response = Mock()
    mock_response.status_code = 403
    mock_response.reason = "Forbidden"

    with patch("hooks.weather.requests.request", return_value=mock_response):
        hook = WeatherApiHook(api_key="test-key")
        result = hook.get_weather("MD", "MDA", date(2024, 4, 1), date(2024, 4, 2))

    assert result == []
    assert hook.errors["code"] == 403
    assert hook.errors["message"] == "Forbidden"
