from typing import Any


def build_weather_data(
        data: list[dict[str, Any]], iso: str
) -> list[dict[str, Any]]:
    weather_data = []

    for item in data:
        item["day"].pop("condition")
        weather_data.append({"date": item["date"], "country_id": iso, **item["day"]})

    return weather_data
