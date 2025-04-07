import datetime
import logging
from typing import TypedDict

from airflow.providers.postgres.hooks.postgres import PostgresHook

date_format = "%Y/%m/%d %H:%M:%S"
logger = logging.getLogger(__name__)


class ApiImportLog(TypedDict):
    country_id: int
    start_time: datetime.datetime
    end_time: datetime.datetime
    code_response: int
    error_messages: str


class ImportLog(TypedDict):
    batch_date: datetime.datetime
    country_id: int
    import_directory_name: str
    import_file_name: str
    file_created_date: datetime.datetime
    file_last_modified_date: datetime.datetime
    rows_count: int


class TransformLog(TypedDict):
    batch_date: datetime.datetime
    country_id: int
    processed_directory_name: str
    processed_file_name: str
    rows_count: int
    status: str


def insert(conn_id: str, data: list[dict[str, str]], table_name: str) -> None:
    columns = list(data[0].keys())
    data = [list(record.values()) for record in data]
    columns_names = ", ".join(columns)
    values = ", ".join(["%s"] * len(columns))

    query = f"""
            INSERT INTO dbo.{table_name} ({columns_names})
            VALUES ({values})
            """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.executemany(query, data)
        conn.commit()
        logger.info(f"Inserted {len(data)} logs into {table_name} table.")
    except Exception as e:
        logger.error(f"An error occurred while inserting log data. Error: {e}.")
