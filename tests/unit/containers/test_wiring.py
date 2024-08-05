from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from keepice_lakehouse.application.iceberg_manager_factory import create_iceberg_manager
from keepice_lakehouse.containers.containers import ConnectorsContainer
from keepice_lakehouse.utils.enums import ConnectorType


@pytest.fixture
def mock_container():
    """Fixture to provide a mocked ConnectorsContainer with mock connectors."""
    container = MagicMock(spec=ConnectorsContainer)

    # Mock connectors
    container.spark_iceberg_config.return_value = MagicMock(name="SparkIcebergConnector")
    container.athena_config.return_value = MagicMock(name="AthenaConnector")
    container.pyiceberg_config.return_value = MagicMock(name="PyIcebergConnector")

    return container


@patch("keepice_lakehouse.application.iceberg_manager.IcebergManager")
def test_create_iceberg_manager_with_valid_connector_type(mock_iceberg_manager, mock_container):
    """Test create_iceberg_manager with a valid connector type."""
    # Set up mock IcebergManager
    mock_iceberg_manager_instance = MagicMock()
    mock_iceberg_manager.return_value = mock_iceberg_manager_instance

    # Test each connector type
    for connector_type in [ConnectorType.SPARK_ICEBERG, ConnectorType.ATHENA, ConnectorType.PYICEBERG]:
        # Ensure IcebergManager is called with the correct connector
        mock_container.reset_mock()
        mock_iceberg_manager.reset_mock()

        # Call the function
        create_iceberg_manager(connector_type, container=mock_container)

        # Check if the appropriate connector method was called
        connector_method = {
            ConnectorType.SPARK_ICEBERG: mock_container.spark_iceberg_config,
            ConnectorType.ATHENA: mock_container.athena_config,
            ConnectorType.PYICEBERG: mock_container.pyiceberg_config,
        }[connector_type]

        # Assert connector method was called
        connector_method.assert_called_once()


def test_create_iceberg_manager_with_invalid_connector_type(mocker, mock_container):
    """Test create_iceberg_manager with an invalid connector type."""
    with pytest.raises(ValueError, match="Unknown connector type: unknown_connector"):
        create_iceberg_manager("unknown_connector", container=mock_container)
