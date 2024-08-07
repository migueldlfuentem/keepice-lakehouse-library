from dependency_injector import containers
from dependency_injector import providers

from ..connectors.athena_connector import AthenaConnector
from ..connectors.pyiceberg_connector import PyIcebergConnector
from ..connectors.spark_connector import SparkConnector
from ..utils.enums import ConnectorType


class ConnectorsContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    spark_iceberg_config = providers.Singleton(SparkConnector, config=config.connectors.spark_iceberg)

    athena_config = providers.Singleton(AthenaConnector, config=config.connectors.athena)

    pyiceberg_config = providers.Singleton(PyIcebergConnector, config=config.connectors.pyiceberg)

    connector_map = providers.Dict(
        {
            ConnectorType.SPARK_ICEBERG: spark_iceberg_config,
            ConnectorType.ATHENA: athena_config,
            ConnectorType.PYICEBERG: pyiceberg_config,
        }
    )

    # Static provider setups
    spark_session = providers.Singleton(lambda container: container.spark_iceberg_config().connect())

    athena_connection = providers.Singleton(lambda container: container.athena_config().connect())

    pyiceberg_catalog = providers.Singleton(lambda container: container.pyiceberg_config().connect())
