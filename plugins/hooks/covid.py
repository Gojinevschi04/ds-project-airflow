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

        response = requests.request(
            HTTPMethod.GET, os.path.join(self.base_url, "reports/total"), params=params
        )

        if response.status_code != HTTPStatus.OK:
            self.logger.error(f"Cant open url. Response code: {response.status_code}.")
            return {}

        self.logger.info("Beginning request")
        data = response.json()

        return data.get("data", [])
