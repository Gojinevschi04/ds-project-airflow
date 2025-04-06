import datetime
from typing import TypedDict

from airflow.providers.postgres.hooks.postgres import PostgresHook

date_format = "%Y/%m/%d %H:%M:%S"


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


def add_api_import_log(data: ApiImportLog, conn_id: str) -> None:
    log_info = {
        **data,
        "start_time": data["start_time"].strftime(date_format),
        "end_time": data["end_time"].strftime(date_format),
    }
    _insert_log(conn_id, log_info, "log_api_import")


def add_import_log(data: ImportLog, conn_id: str) -> None:
    log_info = {
        **data,
        "batch_date": data["batch_date"].strftime(date_format),
        "file_created_date": data["file_created_date"].strftime(date_format),
        "file_last_modified_date": data["file_last_modified_date"].strftime(
            date_format
        ),
    }
    _insert_log(conn_id, log_info, "log_import")


def add_transform_log(data: TransformLog, conn_id: str) -> None:
    log_info = {
        **data,
        "batch_date": data["batch_date"].strftime(date_format),
    }
    _insert_log(conn_id, log_info, "log_transform")


def _insert_log(conn_id: str, data: dict[str, str], table_name: str) -> None:
    columns = [] if not data else list(data.keys())
    data = list(data.values())
    columns_names = ", ".join(columns)
    values = ", ".join(["%s"] * len(columns))
    query = f"insert into dbo.{table_name} ({columns_names}) values ({values})"
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.executemany(query, data)
