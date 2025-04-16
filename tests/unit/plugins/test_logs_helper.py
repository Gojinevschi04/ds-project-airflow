import datetime
from unittest.mock import MagicMock, patch

from helpers.logs import LogTable, insert_log

from tests.conftest import LOG_LIST


def sample_api_log() -> LOG_LIST:
    return [{
        "country_id": 1,
        "start_time": datetime.datetime(2024, 1, 1, 10, 0, 0),
        "end_time": datetime.datetime(2024, 1, 1, 10, 5, 0),
        "code_response": 200,
        "error_messages": "OK"
    }]


@patch("helpers.logs.PostgresHook")
def test_insert_log_success(mock_postgres_hook: MagicMock) -> None:
    # Arrange
    mock_hook = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_hook.get_conn.return_value = mock_conn
    mock_postgres_hook.return_value = mock_hook

    logs = sample_api_log()
    expected_query = """
            INSERT INTO dbo.log_api_import (country_id, start_time, end_time, code_response, error_messages)
            VALUES (%s, %s, %s, %s, %s)
            """

    # Act
    insert_log("test_conn", logs, LogTable.API_IMPORT_LOG)

    # Assert
    mock_postgres_hook.assert_called_once_with(postgres_conn_id="test_conn")
    mock_cursor.executemany.assert_called_once_with(expected_query, [
        [1, datetime.datetime(2024, 1, 1, 10, 0, 0),
         datetime.datetime(2024, 1, 1, 10, 5, 0),
         200, "OK"]
    ])
    mock_conn.commit.assert_called_once()


@patch("helpers.logs.PostgresHook")
def test_insert_log_empty_data(mock_postgres_hook: MagicMock) -> None:
    insert_log("conn_id", [], LogTable.IMPORT_LOG)
    mock_postgres_hook.assert_not_called()
