from enum import Enum


class ConnectorType(Enum):
    SPARK_ICEBERG = "spark_iceberg"
    ATHENA = "athena"
    PYICEBERG = "pyiceberg"
