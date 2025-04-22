from typing import Any

from helpers.weather import build_weather_data

from tests.conftest import WEATHER_INPUT


def test_build_weather_data_basic(
        sample_weather_input: WEATHER_INPUT,
        expected_weather_output: dict[str, Any],
) -> None:
    result = build_weather_data(sample_weather_input, "MD")
    assert result == expected_weather_output
