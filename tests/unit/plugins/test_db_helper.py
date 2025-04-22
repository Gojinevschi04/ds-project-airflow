from unittest.mock import MagicMock, patch

from helpers.db import select


@patch("helpers.db.PostgresHook")
def test_select_single_field(mock_postgres_hook: MagicMock) -> None:
    # Arrange
    mock_hook_instance = MagicMock()
    mock_postgres_hook.return_value = mock_hook_instance
    mock_hook_instance.get_records.return_value = [("result",)]

    # Act
    result = select("fake_conn", "some_table", "field1")

    # Assert
    mock_postgres_hook.assert_called_once_with(postgres_conn_id="fake_conn")
    mock_hook_instance.get_records.assert_called_once_with("SELECT field1 FROM some_table LIMIT 20")
    assert result == [("result",)]


@patch("helpers.db.PostgresHook")
def test_select_multiple_fields(mock_postgres_hook: MagicMock) -> None:
    # Arrange
    mock_hook_instance = MagicMock()
    mock_postgres_hook.return_value = mock_hook_instance
    mock_hook_instance.get_records.return_value = [("row1", "row2")]

    # Act
    result = select("conn", "another_table", ["field1", "field2"])

    # Assert
    mock_postgres_hook.assert_called_once_with(postgres_conn_id="conn")
    mock_hook_instance.get_records.assert_called_once_with("SELECT field1, field2 FROM another_table LIMIT 20")
    assert result == [("row1", "row2")]
