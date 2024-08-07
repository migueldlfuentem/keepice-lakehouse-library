from dependency_injector.wiring import Provide
from dependency_injector.wiring import inject

from ..application.iceberg_manager import IcebergManager
from ..utils.enums import ConnectorType
from .containers import ConnectorsContainer


@inject
def create_iceberg_manager(connector_type: ConnectorType, container: ConnectorsContainer = Provide[ConnectorsContainer]) -> IcebergManager:
    providers = {
        ConnectorType.SPARK_ICEBERG: container.spark_iceberg_config,
        ConnectorType.ATHENA: container.athena_config,
        ConnectorType.PYICEBERG: container.pyiceberg_config,
    }

    connector_provider = providers.get(connector_type)

    if not connector_provider:
        raise ValueError(f"Unknown connector type: {connector_type}")

    connector = connector_provider()
    return IcebergManager(connector)
