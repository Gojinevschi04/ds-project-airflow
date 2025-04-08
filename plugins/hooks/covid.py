import datetime
import logging
import os
from http import HTTPMethod, HTTPStatus
from typing import Any

import pandas as pd
import requests
from airflow.hooks.base import BaseHook


class CovidApiHook(BaseHook):
    """
    A custom Airflow hook for interacting with the COVID-19 API.

    This hook provides methods to:
    - Fetch the list of available countries/regions from the COVID-19 API.
    - Retrieve daily COVID-19 statistics for a specific region and date.

    Attributes:
        base_url (str): The base URL for the COVID-19 API.
        logger (Logger): Logger instance for logging information and errors.
        errors (dict): Stores error information from failed API requests.

    Methods:
        get_countries() -> pd.DataFrame:
            Sends a GET request to fetch the list of countries/regions.
            Returns a pandas DataFrame with region information.

        get_covid(region_iso: str, date: datetime.date) -> dict[str, Any]:
            Sends a GET request to fetch COVID-19 statistics for the given region and date.
            Returns a dictionary with the statistics or an empty dict on failure.
    """

    base_url: str = "https://covid-api.com/api/"
    logger = logging.getLogger(__name__)
    errors = {}

    def get_countries(self) -> pd.DataFrame:
        response = requests.request(
            HTTPMethod.GET, os.path.join(self.base_url, "regions")
        )

        if response.status_code != HTTPStatus.OK:
            self.logger.error(f"Cant open url. Response code: {response.status_code}.")
            return pd.DataFrame()

        self.logger.info("Beginning request")
        data = response.json()

        return pd.DataFrame(data.get("data", []))

    def get_covid(self, region_iso: str, date: datetime.date) -> dict[str, Any]:
        params = {
            "date": date.strftime("%Y-%m-%d"),
            "iso": region_iso,
        }

        start_time = datetime.datetime.now()
        response = requests.request(
            HTTPMethod.GET, os.path.join(self.base_url, "reports/total"), params=params
        )
        end_time = datetime.datetime.now()

        if response.status_code != HTTPStatus.OK:
            self.logger.error(f"Cant open url. Response code: {response.status_code}.")
            self.errors = {
                "code_response": response.status_code,
                "error_messages": response.reason,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            return {}

        self.logger.info("Beginning request")
        data = response.json()

        return data.get("data", [])
