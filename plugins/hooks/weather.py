import datetime
import logging
import os
from http import HTTPMethod, HTTPStatus
from typing import Any

import requests
from airflow.hooks.base import BaseHook
from helpers.weather import build_weather_data


class WeatherApiHook(BaseHook):
    base_url: str = "https://api.weatherapi.com/v1/"
    logger = logging.getLogger(__name__)

    def __init__(self, api_key: str, **kwargs) -> None:
        self.api_key = api_key
        super().__init__(**kwargs)

    def get_weather(
            self, region_iso: str, country_id: str, start_date: datetime.date, end_date: datetime.date,
    ) -> list[dict[str, Any]]:
        params = {
            "dt": start_date.strftime("%Y-%m-%d"),
            "end_dt": end_date.strftime("%Y-%m-%d"),
            "hour": "12",
            "q": region_iso,
            "key": self.api_key,
        }

        response = requests.request(
            HTTPMethod.GET, os.path.join(self.base_url, "history.json"), params=params
        )

        if response.status_code != HTTPStatus.OK:
            self.logger.error(f"Cant open url. Response code: {response.status_code}.")
            return []

        self.logger.info("Beginning request")
        result = response.json()
        raw_data = result.get("forecast", {}).get("forecastday", [])
        weather_data = build_weather_data(raw_data, country_id)

        return weather_data
