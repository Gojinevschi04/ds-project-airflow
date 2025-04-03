import logging

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from plugins.hooks.covid import CovidApiHook
from plugins.operators.db_insert import DbInsertOperator

task_logger = logging.getLogger("airflow.task")


@dag(
    dag_id="regions",
    template_searchpath="/opt/project/",
)
def Regions() -> None:
    init = SQLExecuteQueryOperator(
        task_id="create_regions_table",
        conn_id="tutorial_pg_conn",
        sql="sql/create_regions_table.sql",
    )

    @task
    def extract() -> None:
        regions_hook = CovidApiHook()
        data = regions_hook.get_countries()
        path = "../data/common"
        data.to_csv(f"{path}/regions.csv", index_label="index")

    @task
    def transform(**kwargs) -> None:
        df = pd.read_csv("../data/common/regions.csv")
        df.drop(columns="index", inplace=True)
        df.rename(columns={"iso": "code"}, inplace=True)
        kwargs["ti"].xcom_push("regions_data", df.to_dict(orient="records"))

    load = DbInsertOperator(
        task_id="load",
        ti_id="transform",
        ti_key="regions_data",
        table="regions",
        postgres_conn_id="tutorial_pg_conn"
    )

    init >> extract() >> transform() >> load


dag = Regions()

if __name__ == "__main__":
    dag.test()
