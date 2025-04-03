import datetime
import os
from http import HTTPStatus

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="regions",
    dagrun_timeout=datetime.timedelta(minutes=60),
    template_searchpath="/opt/project/",
)
def Regions() -> None:
    create_regions_table = SQLExecuteQueryOperator(
        task_id="create_regions_table",
        conn_id="tutorial_pg_conn",
        sql="sql/create_regions_table.sql",
    )

    @task
    def get_data() -> None:
        data_path = "../data/common/regions.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://covid-api.com/api/regions"

        response = requests.request("GET", url)

        if response.status_code != HTTPStatus.OK:
            print(f"Cant open url. Response code: {response.status_code}.")
        else:
            print("Beginning request")
        data = response.json()

        df = pd.DataFrame(data.get("data", []))
        df.to_csv(data_path, index_label="index")

    @task
    def insert_data() -> int:
        raw_data = pd.read_csv("../data/common/regions.csv")
        raw_data.drop(columns="index", inplace=True)
        raw_data.rename(columns={"iso": "code"}, inplace=True)

        table = "regions"
        records = raw_data.to_dict(orient="records")
        columns = [] if not records else list(records[0].keys())
        data = [list(record.values()) for record in records]
        columns_names = ", ".join(columns)
        values = ", ".join(["%s"] * len(columns))

        query = f"insert into {table} ({columns_names}) values ({values})"

        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.executemany(query, data)
            conn.commit()
            return 0
        except Exception:
            return 1

    [create_regions_table] >> get_data() >> insert_data()
    # get_data() >> insert_data()


dag = Regions()

if __name__ == "__main__":
    dag.test()
