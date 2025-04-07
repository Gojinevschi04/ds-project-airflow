import datetime
import logging
import os
from http import HTTPMethod, HTTPStatus
from typing import Any

import pandas as pd
import requests
from airflow.hooks.base import BaseHook


class CovidApiHook(BaseHook):
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
