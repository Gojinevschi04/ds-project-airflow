from unittest.mock import Mock, patch

from operators.db_insert import DbInsertOperator


def test_db_insert_operator_with_records() -> None:
    records = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
    mock_context = {
        "ti": Mock(
            xcom_pull=Mock(return_value=records)
        )
    }

    mock_cursor = Mock()
    mock_conn = Mock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("operators.db_insert.PostgresHook") as mock_hook:
        mock_hook.return_value.get_conn.return_value = mock_conn

        op = DbInsertOperator(
            table="test_table",
            ti_id="extract",
            ti_key="data",
            postgres_conn_id="postgres_test",
            task_id="db_insert",
            unique_columns=["id"]
        )
        op.execute(mock_context)

        assert mock_cursor.executemany.called
        assert mock_conn.commit.called
        query_passed = mock_cursor.executemany.call_args[0][0]
        data_passed = mock_cursor.executemany.call_args[0][1]

        assert "INSERT INTO dbo.test_table" in query_passed
        assert len(data_passed) == 2


def test_db_insert_operator_without_records(caplog) -> None:
    mock_context = {
        "ti": Mock(
            xcom_pull=Mock(return_value=None)
        )
    }

    with patch("operators.db_insert.PostgresHook") as mock_hook:
        op = DbInsertOperator(
            table="test_table",
            ti_id="extract",
            ti_key="data",
            postgres_conn_id="postgres_test",
            task_id="db_insert"
        )
        op.execute(mock_context)

        mock_hook.assert_not_called()
        assert "No records to insert." in caplog.text
