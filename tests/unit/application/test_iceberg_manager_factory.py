from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from keepice_lakehouse.application.iceberg_manager_factory import IcebergManagerFactory
from keepice_lakehouse.containers.containers import ConnectorsContainer
from keepice_lakehouse.utils.enums import ConnectorType


@pytest.fixture
def mock_config():
    """Fixture to provide mock configuration data."""
    return {
        "connectors": {
            "spark_iceberg": {
                "app_name": "test_app",
                "master": "local[*]",
                "config": {"spark.some.config.option": "config_value"},
                "catalog_name": "test_catalog",
            }
        }
    }


@pytest.fixture
def mock_config_error():
    """Fixture to provide mock configuration data."""
    return {
        "spark_iceberg": {
            "app_name": "test_app",
            "master": "local[*]",
            "config": {"spark.some.config.option": "config_value"},
            "catalog_name": "test_catalog",
        }
    }


@pytest.fixture
def mock_container(mocker):
    """Fixture to provide a mocked ConnectorsContainer."""
    container = mocker.MagicMock(spec=ConnectorsContainer)
    return container


@patch("keepice_lakehouse.utils.utils.find_config_folder")
@patch("keepice_lakehouse.utils.utils.Path.open")
@patch("yaml.safe_load")
@patch("keepice_lakehouse.containers.containers.ConnectorsContainer")
def test_initialization(mock_connectors_container, mock_yaml_load, mock_path_open, mock_find_config_folder, mock_config):
    """Test the initialization and configuration of IcebergManagerFactory."""

    # Set up mocks
    mock_find_config_folder.return_value = Path("/mock/path")
    mock_yaml_load.return_value = mock_config
    mock_file = MagicMock()
    mock_path_open.return_value.__enter__.return_value = mock_file

    # Mock ConnectorsContainer
    mock_connectors_container_instance = MagicMock(spec=ConnectorsContainer)
    mock_connectors_container.return_value = mock_connectors_container_instance

    # Instantiate IcebergManagerFactory
    IcebergManagerFactory()

    # Assertions
    mock_path_open.assert_called_once()
    mock_yaml_load.assert_called_once()

    # Validate that ConnectorsContainer was instantiated
    assert isinstance(mock_connectors_container_instance, ConnectorsContainer)


@patch("keepice_lakehouse.application.iceberg_manager_factory.create_iceberg_manager")
@patch("keepice_lakehouse.utils.enums.ConnectorType")
def test_get_manager(mock_connector_type, mock_create_iceberg_manager, mock_container):
    """Test the get_manager method of IcebergManagerFactory."""
    # Set up mocks
    mock_connector_type.SPARK_ICEBERG = "SPARK_ICEBERG"
    mock_create_iceberg_manager.return_value = MagicMock(name="IcebergManager")

    # Set up IcebergManagerFactory
    factory = IcebergManagerFactory()
    factory.container = mock_container  # Use the mocked container

    # Test valid connector type
    manager = factory.get_manager("SPARK_ICEBERG")
    mock_create_iceberg_manager.assert_called_once_with(ConnectorType.SPARK_ICEBERG, container=mock_container)
    assert manager == mock_create_iceberg_manager.return_value

    # Test unknown connector type
    with pytest.raises(ValueError, match="Unknown connector type: UNKNOWN"):
        factory.get_manager("UNKNOWN")


@patch("keepice_lakehouse.utils.utils.find_config_folder")
@patch("keepice_lakehouse.utils.utils.Path.open")
@patch("yaml.safe_load")
def test_initialization_invalid_config(mock_yaml_load, mock_path_open, mock_find_config_folder):
    """Test IcebergManagerFactory with invalid configuration."""

    # Set up mocks
    mock_find_config_folder.return_value = Path("/mock/path")
    mock_path_open.return_value.__enter__.return_value = MagicMock()

    # Simulate a validation error in ConfigModel
    mock_yaml_load.return_value = {
        "spark_iceberg": {
            "app_name": "test_app",
            "master": "local[*]",
            "config": {"spark.some.config.option": "config_value"},
            "catalog_name": "test_catalog",
        }
    }

    with patch("keepice_lakehouse.models.models.ConfigModel"):
        # Test that IcebergManagerFactory raises a ValueError due to invalid config
        with pytest.raises(ValueError):
            IcebergManagerFactory()
