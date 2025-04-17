import logging
import os
import shutil

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from hooks.covid import CovidApiHook
from operators.db_insert import DbInsertOperator

task_logger = logging.getLogger("airflow.task")


@dag(
    dag_id="regions",
    template_searchpath="/opt/project/",
)
def Regions() -> None:
    init = SQLExecuteQueryOperator(
        task_id="create_regions_table",
        conn_id="pg_conn",
        sql=[
            "sql/create_regions_table.sql",
            "sql/create_api_import_log_table.sql",
            "sql/create_import_log_table.sql",
            "sql/create_transform_log_table.sql",
        ],
    )

    @task
    def extract() -> None:
        path = "../data/common/raw/"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        regions_hook = CovidApiHook()
        data = regions_hook.get_countries()
        data.to_json(f"{path}/regions.json", orient="records", indent=2)

    @task
    def transform(**kwargs) -> None:
        destination_path_success = "../data/common/success/"
        destination_path_error = "../data/common/error/"
        os.makedirs(os.path.dirname(destination_path_success), exist_ok=True)
        os.makedirs(os.path.dirname(destination_path_error), exist_ok=True)
        regions_path = "../data/common/raw/regions.json"
        file_name = "regions.json"

        try:
            data = pd.read_json(regions_path)
            if data.empty:
                raise ValueError("Empty file.")
            data.rename(columns={"iso": "code"}, inplace=True)
            shutil.move(regions_path, os.path.join(destination_path_success, file_name))
            kwargs["ti"].xcom_push("regions_data", data.to_dict(orient="records"))

        except Exception:
            shutil.move(regions_path, os.path.join(destination_path_error, file_name))

    load = DbInsertOperator(
        task_id="load",
        ti_id="transform",
        ti_key="regions_data",
        table="regions",
        postgres_conn_id="pg_conn",
    )

    init >> extract() >> transform() >> load


dag = Regions()

if __name__ == "__main__":
    dag.test()
