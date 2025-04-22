from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


def select(conn_id: str, table: str, fields: list[str] | str = "*") -> Any:
    if isinstance(fields, str):
        fields = [fields]
    hook = PostgresHook(postgres_conn_id=conn_id)
    query = f"SELECT {', '.join(fields)} FROM {table} LIMIT 20"
    return hook.get_records(query)
