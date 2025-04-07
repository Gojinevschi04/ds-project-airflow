import logging

from airflow.models import BaseOperator
from airflow.utils.context import Context
from helpers.logs import insert


class LogInsertOperator(BaseOperator):
    def __init__(
        self,
        postgres_conn_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.logger = logging.getLogger(__name__)

    def execute(self, context: Context) -> None:
        log_data: list = []
        import_log = context["ti"].xcom_pull(task_ids="extract", key="log_import")
        api_import_log = context["ti"].xcom_pull(
            task_ids="extract", key="log_api_import"
        )
        transform_log = context["ti"].xcom_pull(
            task_ids="transform", key="log_transform"
        )

        if import_log:
            log_data.append(
                {
                    "log_import": import_log,
                }
            )

        if api_import_log:
            log_data.append(
                {
                    "log_api_import": api_import_log,
                }
            )

        if transform_log:
            log_data.append({"log_transform": transform_log})

        if not log_data:
            self.logger.info("No records to insert.")
            return

        for item in log_data:
            for table, records in item.items():
                insert(self.postgres_conn_id, records, table)
