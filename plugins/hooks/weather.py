import datetime
import logging
import os
from http import HTTPMethod, HTTPStatus
from typing import Any

import requests
from airflow.hooks.base import BaseHook
from helpers.weather import build_weather_data


class WeatherApiHook(BaseHook):
    """
    A custom Airflow hook for retrieving historical weather data using the WeatherAPI.

    This hook provides a method to fetch weather data for a specific region and time period.

    Attributes:
        base_url (str): The base URL for the WeatherAPI service.
        logger (Logger): Logger instance for logging information and errors.
        errors (dict): Stores error details from failed API requests.

    Methods:
        get_weather(region_iso: str, country_id: str, start_date: date, end_date: date) -> list[dict[str, Any]]:
            Fetches historical weather data between two dates for the specified region.
            Returns a list of weather records in dictionary format or an empty list on failure.
    """

    base_url: str = "https://api.weatherapi.com/v1/"
    logger = logging.getLogger(__name__)
    errors = {}

    def __init__(self, api_key: str, **kwargs) -> None:
        self.api_key = api_key
        super().__init__(**kwargs)

    def get_weather(
        self,
        region_iso: str,
        country_id: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[dict[str, Any]]:
        params = {
            "dt": start_date.strftime("%Y-%m-%d"),
            "end_dt": end_date.strftime("%Y-%m-%d"),
            "hour": "12",
            "q": region_iso,
            "key": self.api_key,
        }

        start_time = datetime.datetime.now()
        response = requests.request(
            HTTPMethod.GET, os.path.join(self.base_url, "history.json"), params=params
        )
        end_time = datetime.datetime.now()

        if response.status_code != HTTPStatus.OK:
            self.logger.error(f"Cant open url. Response code: {response.status_code}.")
            self.errors = {
                "code_response": response.status_code,
                "error_messages": response.reason,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
            return []
        self.errors = {
            "code_response": "200",
            "error_messages": "success extraction",
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
        }

        self.logger.info("Beginning request")
        result = response.json()
        raw_data = result.get("forecast", {}).get("forecastday", [])
        weather_data = build_weather_data(raw_data, country_id)

        return weather_data
