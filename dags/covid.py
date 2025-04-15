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
from hooks.covid import CovidApiHook
from operators.db_insert import DbInsertOperator

RUN_HISTORICAL = Variable.get("RUN_HISTORICAL", "false") == "true"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (
        datetime.datetime(2020, 2, 1)
        if RUN_HISTORICAL
        else datetime.datetime(2022, 2, 1)
    ),
    "schedule_interval": "@daily",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}


@dag(
    dag_id="covid",
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=60),
    template_searchpath="/opt/project/",
    description="Extract data for past dates",
    catchup=RUN_HISTORICAL,
    params={
        "start_date": Param(
            "2020-02-01",
            type="string",
            format="date",
        ),
        "end_date": Param(
            "2020-03-02",
            type="string",
            format="date",
        ),
    },
)
def Covid() -> None:
    init = SQLExecuteQueryOperator(
        task_id="create_covid_table",
        conn_id="pg_conn",
        sql="sql/create_covid_table.sql",
    )

    @task
    def extract(**context) -> None:
        path = "../data/covid/raw/"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        covid_hook = CovidApiHook()
        regions = select("pg_conn", "regions")
        current_date = datetime.datetime.strptime(
            context["params"]["start_date"], "%Y-%m-%d"
        ).date()
        end_date = datetime.datetime.strptime(
            context["params"]["end_date"], "%Y-%m-%d"
        ).date()
        step = datetime.timedelta(days=1)
        api_import_log = []
        import_log = []

        while current_date <= end_date:
            for index, iso, _ in regions:
                dfs = []
                result = covid_hook.get_covid(iso, current_date)
                if covid_hook.errors:
                    api_import_log.append({"country_id": index, **covid_hook.errors})
                if not result:
                    continue
                result["country_id"] = index
                dfs.append(result)
                file_name = f"covid_{current_date.strftime('%Y-%m-%d')}_{iso}.json"

                try:
                    pd.DataFrame(dfs).to_json(
                        f"{path}/{file_name}", indent=2, orient="records"
                    )
                except Exception:
                    pass

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
                        "rows_count": len(dfs),
                    }
                )
            current_date += step
        insert_log("pg_conn", api_import_log, LogTable.API_IMPORT_LOG)
        insert_log("pg_conn", import_log, LogTable.IMPORT_LOG)

    @task
    def transform(**kwargs) -> None:
        source_path = Path("../data/covid/raw/")
        destination_path_success = "../data/covid/success/"
        destination_path_error = "../data/covid/error/"
        os.makedirs(os.path.dirname(destination_path_success), exist_ok=True)
        os.makedirs(os.path.dirname(destination_path_error), exist_ok=True)

        transform_log = []
        data = []
        for file_path in source_path.glob("*.json"):
            file_name = file_path.name
            covid_data = []
            try:
                covid_data = pd.read_json(file_path, convert_dates=False).to_dict(
                    orient="records"
                )
                if not covid_data:
                    raise ValueError("Empty file.")
                data += covid_data
                shutil.move(
                    file_path, os.path.join(destination_path_success, file_name)
                )
                status = "success"

            except Exception:
                status = "error"
                shutil.move(file_path, os.path.join(destination_path_error, file_name))

            transform_log.append(
                {
                    "batch_date": datetime.datetime.now().strftime("%Y-%m-%d"),
                    "country_id": covid_data[0]["country_id"],
                    "processed_directory_name": str(file_path),
                    "processed_file_name": file_name,
                    "rows_count": len(covid_data),
                    "status": status,
                }
            )

        kwargs["ti"].xcom_push("covid_data", data)
        insert_log("pg_conn", transform_log, LogTable.TRANSFORM_LOG)

    load = DbInsertOperator(
        task_id="load",
        ti_id="transform",
        ti_key="covid_data",
        table="covid",
        postgres_conn_id="pg_conn",
        unique_columns=["date", "country_id"],
    )

    init >> extract() >> transform() >> load


dag = Covid()

if __name__ == "__main__":
    dag.test()
