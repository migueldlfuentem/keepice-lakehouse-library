from unittest.mock import MagicMock

import pytest

from keepice_lakehouse.application.iceberg_manager import IcebergManager
from keepice_lakehouse.connectors.spark_connector import SparkConnector
from keepice_lakehouse.exceptions.exceptions import DatabaseCreationError
from keepice_lakehouse.exceptions.exceptions import InvalidTablePropertyError
from keepice_lakehouse.exceptions.exceptions import MetadataRetrievalError
from keepice_lakehouse.exceptions.exceptions import TableCreationError
from keepice_lakehouse.exceptions.exceptions import TableDropError


@pytest.fixture
def mock_connector():
    """Fixture to provide a mocked BaseConnector."""
    connector = MagicMock(spec=SparkConnector)
    connector.catalog_name = "test_catalog"
    return connector


def test_list_databases(mock_connector):
    """Test list_databases method."""
    # Setup
    mock_connector.query.return_value = ["db1", "db2"]
    iceberg_manager = IcebergManager(connector=mock_connector)

    # Exercise
    databases = iceberg_manager.list_databases()

    # Verify
    assert databases == ["db1", "db2"]


def test_list_tables(mock_connector):
    """Test list_tables method."""
    mock_connector.query.return_value = ["table1", "table2"]
    iceberg_manager = IcebergManager(connector=mock_connector)

    tables = iceberg_manager.list_tables("test_db")

    assert tables == ["table1", "table2"]


def test_create_database_success(mock_connector):
    """Test create_database method with successful execution."""
    iceberg_manager = IcebergManager(connector=mock_connector)

    iceberg_manager.create_database("new_db")

    mock_connector.query.assert_called_once()


def test_create_database_failure(mock_connector):
    """Test create_database method when an exception is raised."""
    mock_connector.query.side_effect = Exception("Creation error")
    iceberg_manager = IcebergManager(connector=mock_connector)

    with pytest.raises(DatabaseCreationError, match="Creation error"):
        iceberg_manager.create_database("new_db")


def test_create_table_success(mock_connector):
    """Test create_table method with successful execution."""
    iceberg_manager = IcebergManager(connector=mock_connector)
    columns = {"id": "INT", "name": "STRING"}
    iceberg_manager.create_table("test_db", "test_table", columns, "s3://path/to/data", "id")

    mock_connector.query.assert_called_once()


def test_create_table_failure(mock_connector):
    """Test create_table method when an exception is raised."""
    mock_connector.query.side_effect = Exception("Creation error")
    iceberg_manager = IcebergManager(connector=mock_connector)

    columns = {"id": "INT", "name": "STRING"}
    with pytest.raises(TableCreationError, match="Creation error"):
        iceberg_manager.create_table("test_db", "test_table", columns, "s3://path/to/data", "id")


def test_drop_table_success(mock_connector):
    """Test drop_table method with successful execution."""
    iceberg_manager = IcebergManager(connector=mock_connector)

    iceberg_manager.drop_table("test_db", "test_table")

    mock_connector.query.assert_called_once()


def test_drop_table_failure(mock_connector):
    """Test drop_table method when an exception is raised."""
    mock_connector.query.side_effect = Exception("Drop error")
    iceberg_manager = IcebergManager(connector=mock_connector)

    with pytest.raises(TableDropError, match="Drop error"):
        iceberg_manager.drop_table("test_db", "test_table")


def test_get_property_success(mock_connector):
    """Test get_property method with successful execution."""
    mock_connector.query.return_value = ["property_value"]
    iceberg_manager = IcebergManager(connector=mock_connector)

    property_value = iceberg_manager.get_property("test_db", "test_table", "snapshots")

    mock_connector.query.assert_called_once()
    assert property_value == ["property_value"]


def test_get_property_invalid_property(mock_connector):
    """Test get_property method with invalid table_property."""
    iceberg_manager = IcebergManager(connector=mock_connector)

    with pytest.raises(InvalidTablePropertyError, match="Invalid table_property: invalid_property"):
        iceberg_manager.get_property("test_db", "test_table", "invalid_property")


def test_get_property_failure(mock_connector):
    """Test get_property method when an exception is raised."""
    mock_connector.query.side_effect = Exception("Metadata error")
    iceberg_manager = IcebergManager(connector=mock_connector)

    with pytest.raises(MetadataRetrievalError, match="Metadata error"):
        iceberg_manager.get_property("test_db", "test_table", "snapshots")


def test_insert_bulk_table_data(mock_connector):
    """Test insert_bulk_table_data method."""
    iceberg_manager = IcebergManager(connector=mock_connector)

    iceberg_manager.insert_bulk_table_data("source_table", "test_db", "test_table")
    assert mock_connector.query.call_count == 2


def test_insert_incremental_table_data(mock_connector):
    """Test insert_incremental_table_data method."""
    iceberg_manager = IcebergManager(connector=mock_connector)

    iceberg_manager.insert_incremental_table_data("source_table", "test_db", "test_table")

    mock_connector.query.assert_called()


def test_upsert_delta_table_data(mock_connector):
    """Test upsert_delta_table_data method."""
    iceberg_manager = IcebergManager(connector=mock_connector)

    iceberg_manager.upsert_delta_table_data("source_table", "test_db", "test_table", "id", "timestamp")

    mock_connector.query.assert_called_once()


def test_close_connection(mock_connector):
    """Test close method."""
    mock_connector.connect.return_value.stop = MagicMock()
    iceberg_manager = IcebergManager(connector=mock_connector)

    iceberg_manager.close()

    mock_connector.connect.return_value.stop.assert_called_once()
