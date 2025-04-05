import logging
import os

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
        sql="sql/create_regions_table.sql",
    )

    @task
    def extract() -> None:
        path = "../data/common/"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        regions_hook = CovidApiHook()
        data = regions_hook.get_countries()
        data.to_json(f"{path}/regions.json", orient="records")

    @task
    def transform(**kwargs) -> None:
        df = pd.read_json("../data/common/regions.json")
        df.rename(columns={"iso": "code"}, inplace=True)
        kwargs["ti"].xcom_push("regions_data", df.to_dict(orient="records"))

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
