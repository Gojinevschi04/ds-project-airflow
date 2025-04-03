import datetime
import os
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from plugins.hooks.covid import CovidApiHook
from plugins.operators.db_insert import DbInsertOperator

RUN_HISTORICAL = os.getenv("RUN_HISTORICAL", "false").lower() == "true"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime.date(2020, 2, 1)
    # if RUN_HISTORICAL else datetime.date(2022, 2, 1),
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
    "2020-03-01",
            type="string",
            format="date",
        ),
    }
)
def Covid() -> None:
    init = SQLExecuteQueryOperator(
        task_id="create_covid_table",
        conn_id="tutorial_pg_conn",
        sql="sql/create_covid_table.sql",
    )

    @task
    def extract(**context) -> None:
        path = "../data/covid"
        covid_hook = CovidApiHook()
        regions = pd.read_csv("../data/common/regions.csv")
        current_date = datetime.datetime.strptime(context["params"]["start_date"], "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(context["params"]["end_date"], "%Y-%m-%d").date()
        step = datetime.timedelta(days=1)

        while current_date <= end_date:
            dfs = []

            for index, row in regions.iterrows():
                result = covid_hook.get_covid(row["iso"], current_date)
                if not result:
                    continue
                result["country_id"] = int(index) + 1
                dfs.append(result)

            pd.DataFrame(dfs).to_csv(
                f"{path}/covid_{current_date.strftime('%Y-%m-%d')}.csv",
                index=False)
            current_date += step

    @task
    def transform(**kwargs) -> None:
        folder_path = Path("../data/covid/")

        data = []
        for file_path in folder_path.glob("*.csv"):
            covid_data = pd.read_csv(file_path)
            data += covid_data.to_dict("records")

        kwargs["ti"].xcom_push("covid_data", data)

    load = DbInsertOperator(
        task_id="load",
        ti_id="transform",
        ti_key="covid_data",
        table="covid",
        postgres_conn_id="tutorial_pg_conn",
    )

    init >> extract() >> transform() >> load


dag = Covid()

if __name__ == "__main__":
    dag.test()
