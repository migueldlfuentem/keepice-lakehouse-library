from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from keepice_lakehouse.connectors.athena_connector import AthenaConnector
from keepice_lakehouse.models.models import AthenaConfigModel


@pytest.fixture
def config():
    """Fixture to provide a sample configuration."""
    event = {
        "region_name": "us-west-2",
        "s3_staging_dir": "s3://my-bucket/path/",
        "workgroup": "primary",
        "warehouse": "my_warehouse",
        "catalog_name": "my_catalog",
    }
    return AthenaConfigModel(**event)


def test_initialization(config):
    """Test the initialization of AthenaConnector."""
    connector = AthenaConnector(config.model_dump(mode="json"))
    print(connector)
    assert connector.region_name == "us-west-2"
    assert connector.s3_staging_dir == "s3://my-bucket/path/"
    assert connector.workgroup == "primary"
    assert connector.warehouse == "my_warehouse"
    assert connector.catalog_name == "my_catalog"


@patch("keepice_lakehouse.connectors.athena_connector.athena_connect")
def test_connect(mock_athena_connect, config):
    """Test the connect method of AthenaConnector."""
    mock_connection = MagicMock()
    mock_athena_connect.return_value = mock_connection

    connector = AthenaConnector(config.model_dump(mode="json"))
    connector.connect()

    # Verify that athena_connect was called with the correct parameters
    mock_athena_connect.assert_called_once_with(s3_staging_dir="s3://my-bucket/path/", region_name="us-west-2")


@patch("keepice_lakehouse.connectors.athena_connector.athena_connect")
def test_query(mock_athena_connect, config):
    """Test the query method of AthenaConnector."""
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_athena_connect.return_value = mock_connection

    connector = AthenaConnector(config.model_dump(mode="json"))
    connector.connect()  # Ensure the connection is established

    # Set up the mock cursor to simulate query execution
    mock_cursor.execute.return_value = None

    query = "SELECT * FROM my_table"
    cursor = connector.query(query)

    # Verify that the cursor's execute method was called with the query
    mock_cursor.execute.assert_called_once_with(query)

    # Verify that the method returns the cursor
    assert cursor == mock_cursor
