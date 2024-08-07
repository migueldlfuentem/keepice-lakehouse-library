from pyathena import connect as athena_connect
from sqlalchemy import create_engine

from ..models.models import AthenaConfigModel
from .base_connector import BaseConnector


class AthenaConnector(BaseConnector):
    """
    Connector class for AWS Athena using SQLAlchemy and PyAthena.

    This class provides methods to connect to AWS Athena, execute queries, and manage connection properties.

    Attributes:
        region_name (str): The AWS region name.
        s3_staging_dir (str): The S3 staging directory for query results.
        workgroup (str): The Athena workgroup.
        warehouse (str): The data warehouse name.
        __catalog_name (str): The catalog name for Athena.

    Args:
        config (AthenaConfigModel): Configuration model containing necessary connection parameters.
    """

    def __init__(self, config: AthenaConfigModel):
        """
        Initializes the AthenaConnector with the given configuration.

        Args:
            config (AthenaConfigModel): The configuration model with parameters for the connection.
        """
        self.region_name = config.get("region_name")
        self.s3_staging_dir = config.get("s3_staging_dir")
        self.workgroup = config.get("workgroup")
        self.warehouse = config.get("warehouse")
        self.__catalog_name = config.get("catalog_name")

    @property
    def catalog_name(self):
        """
        The catalog name property.

        Returns:
            str: The catalog name used by Athena.
        """
        return self.__catalog_name

    def connect(self):
        """
        Establishes a connection to AWS Athena and creates a SQLAlchemy engine.

        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy engine for executing SQL queries.
        """
        self.connection = athena_connect(s3_staging_dir=self.s3_staging_dir, region_name=self.region_name)
        return create_engine("awsathena://", creator=lambda: self.connection)

    def query(self, query: str):
        """
        Executes a SQL query on Athena and returns the result cursor.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            pyathena.cursor.Cursor: The cursor with the results of the query.
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor
