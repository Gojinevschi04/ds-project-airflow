import logging

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


class DbInsertOperator(BaseOperator):
    def __init__(
        self,
        table: str,
        ti_id: str,
        ti_key: str,
        postgres_conn_id: str,
        unique_columns: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.ti_id = ti_id
        self.ti_key = ti_key
        self.postgres_conn_id = postgres_conn_id
        self.unique_columns = unique_columns
        self.logger = logging.getLogger(__name__)

    def execute(self, context: Context) -> None:
        records = context["ti"].xcom_pull(task_ids=self.ti_id, key=self.ti_key)
        if not records:
            self.logger.info("No records to insert.")
            return

        columns = list(records[0].keys())
        data = [list(record.values()) for record in records]
        columns_names = ", ".join(columns)
        values = ", ".join(["%s"] * len(columns))

        query = f"""
                  INSERT INTO {self.table} ({columns_names})
                  VALUES ({values})
                  """
        if self.unique_columns:
            conflict_values = ", ".join(
                [f"{column} = EXCLUDED.{column}" for column in columns]
            )
            conflict_columns = ", ".join(self.unique_columns)
            query += f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_values}"
        else:
            query += "ON CONFLICT DO NOTHING"
        try:
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.executemany(query, data)
            conn.commit()
            self.logger.info(f"Inserted {len(data)} records into {self.table} table.")
        except Exception as e:
            self.logger.error(f"An error occurred while inserting data. Error: {e}.")
