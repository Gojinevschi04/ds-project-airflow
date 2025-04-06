import datetime
import os
import shutil
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helpers.db import select
from hooks.weather import WeatherApiHook
from operators.db_insert import DbInsertOperator

RUN_HISTORICAL = Variable.get("RUN_HISTORICAL", "false") == "true"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2025, 2, 1)
    if RUN_HISTORICAL
    else datetime.datetime(2025, 2, 1),
    "schedule_interval": "@daily",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}


@dag(
    dag_id="weather",
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=60),
    template_searchpath="/opt/project/",
    description="Extract data for past dates",
    catchup=RUN_HISTORICAL,
    params={
        "start_date": Param(
            "2025-02-01",
            type="string",
            format="date",
        ),
        "end_date": Param(
            "2025-02-03",
            type="string",
            format="date",
        ),
    },
)
def Weather() -> None:
    init = SQLExecuteQueryOperator(
        task_id="create_weather_table",
        conn_id="pg_conn",
        sql="sql/create_weather_table.sql",
    )

    @task
    def extract(**context) -> None:
        path = "../data/weather/raw/"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        api_key = Variable.get("WEATHER_API_KEY", "")
        weather_hook = WeatherApiHook(api_key)
        regions = select("pg_conn", "regions")
        start_date = datetime.datetime.strptime(
            context["params"]["start_date"], "%Y-%m-%d"
        ).date()
        end_date = datetime.datetime.strptime(
            context["params"]["end_date"], "%Y-%m-%d"
        ).date()

        for index, iso, _ in regions:
            result = weather_hook.get_weather(iso, index, start_date, end_date)
            if not result:
                continue
            pd.DataFrame(result).to_json(
                f"{path}/weather_{iso}.json",
                indent=2,
                orient="records"
            )

    @task
    def transform(**kwargs) -> None:
        source_path = Path("../data/weather/raw/")
        destination_path_success = "../data/weather/success/"
        destination_path_error = "../data/weather/error/"
        os.makedirs(os.path.dirname(destination_path_success), exist_ok=True)
        os.makedirs(os.path.dirname(destination_path_error), exist_ok=True)

        data = []
        for file_path in source_path.glob("*.json"):
            file_name = file_path.name
            try:
                weather_data = (pd.read_json(file_path, convert_dates=False)
                                .to_dict(orient="records"))
                if not weather_data:
                    raise ValueError("Empty file.")
                data += weather_data
                shutil.move(file_path, os.path.join(destination_path_success, file_name))

            except Exception:
                shutil.move(file_path, os.path.join(destination_path_error, file_name))

        kwargs["ti"].xcom_push("weather_data", data)

    load = DbInsertOperator(
        task_id="load",
        ti_id="transform",
        ti_key="weather_data",
        table="weather",
        postgres_conn_id="pg_conn",
        unique_columns=["date", "country_id"],
    )

    init >> extract() >> transform() >> load


dag = Weather()

if __name__ == "__main__":
    dag.test()
