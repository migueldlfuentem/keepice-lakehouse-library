from enum import Enum


class ConnectorType(Enum):
    """
    Enumeration for different types of connectors.

    This Enum defines the different types of connectors that can be used within the system.

    Attributes:
        SPARK_ICEBERG (str): Represents a Spark Iceberg connector.
        ATHENA (str): Represents an Athena connector.
        PYICEBERG (str): Represents a PyIceberg connector.
    """
    SPARK_ICEBERG = "spark_iceberg"
    ATHENA = "athena"
    PYICEBERG = "pyiceberg"
