import datetime
import os
import shutil
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helpers.db import select
from helpers.logs import LogTable, insert_log
from hooks.weather import WeatherApiHook
from operators.db_insert import DbInsertOperator

RUN_HISTORICAL = Variable.get("RUN_HISTORICAL", "false") == "true"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (
        datetime.datetime(2025, 2, 1)
        if RUN_HISTORICAL
        else datetime.datetime(2025, 2, 1)
    ),
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
            "2025-03-02",
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

    init_dw = SQLExecuteQueryOperator(
        task_id="create_dw_covid_weather_fact_table",
        conn_id="pg_conn",
        sql="sql/create_dw_covid_weather_fact_table.sql",
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
        ti = context.get("ti")
        api_import_log = []
        import_log = []

        for index, iso, _ in regions:
            result = weather_hook.get_weather(iso, index, start_date, end_date)
            if weather_hook.errors:
                api_import_log.append({"country_id": index, **weather_hook.errors})
            if not result:
                continue
            file_name = f"weather_{iso}.json"

            try:
                pd.DataFrame(result).to_json(
                    f"{path}/{file_name}", indent=2, orient="records"
                )
            except Exception:
                import_log.append(
                    {
                        "country_id": index,
                        "batch_date": datetime.datetime.now().strftime("%Y-%m-%d"),
                        "import_directory_name": path,
                        "import_file_name": file_name,
                        "file_created_date": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "file_last_modified_date": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "rows_count": len(result),
                    }
                )
        insert_log("pg_conn", api_import_log, LogTable.API_IMPORT_LOG)
        insert_log("pg_conn", import_log, LogTable.IMPORT_LOG)

    @task
    def transform(**kwargs) -> None:
        source_path = Path("../data/weather/raw/")
        destination_path_success = "../data/weather/success/"
        destination_path_error = "../data/weather/error/"
        os.makedirs(os.path.dirname(destination_path_success), exist_ok=True)
        os.makedirs(os.path.dirname(destination_path_error), exist_ok=True)

        transform_log = []
        data = []
        for file_path in source_path.glob("*.json"):
            file_name = file_path.name
            weather_data = []
            try:
                weather_data = pd.read_json(file_path, convert_dates=False).to_dict(
                    orient="records"
                )
                if not weather_data:
                    raise ValueError("Empty file.")
                data += weather_data
                shutil.move(
                    file_path, os.path.join(destination_path_success, file_name)
                )

            except Exception:
                transform_log.append(
                    {
                        "batch_date": datetime.datetime.now().strftime("%Y-%m-%d"),
                        "country_id": weather_data[0]["country_id"],
                        "processed_directory_name": file_path,
                        "processed_file_name": file_name,
                        "rows_count": len(weather_data),
                        "status": "error",
                    }
                )
                shutil.move(file_path, os.path.join(destination_path_error, file_name))

        kwargs["ti"].xcom_push("weather_data", data)
        insert_log("pg_conn", transform_log, LogTable.TRANSFORM_LOG)

    load = DbInsertOperator(
        task_id="load",
        ti_id="transform",
        ti_key="weather_data",
        table="weather",
        postgres_conn_id="pg_conn",
        unique_columns=["date", "country_id"],
    )

    load_dw_weather_fact = SQLExecuteQueryOperator(
        task_id="load_dw_weather_fact",
        conn_id="pg_conn",
        sql="sql/load_dw_weather_fact.sql",
    )

    init >> init_dw >> extract() >> transform() >> load, load_dw_weather_fact


dag = Weather()

if __name__ == "__main__":
    dag.test()
