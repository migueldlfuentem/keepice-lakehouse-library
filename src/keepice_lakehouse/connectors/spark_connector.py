from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from ..models.models import SparkIcebergConfigModel
from .base_connector import BaseConnector


class SparkConnector(BaseConnector):
    """
    Connector class for Apache Spark using PySpark.

    This class provides methods to connect to a Spark cluster, execute SQL queries, and manage connection properties.

    Attributes:
        __app_name (str): The application name for the Spark session.
        __master (str): The master URL for the Spark cluster.
        __spark_config (dict): Additional Spark configuration properties.
        __catalog_name (str): The catalog name for Spark.

    Args:
        config (SparkIcebergConfigModel): Configuration model containing necessary connection parameters.
    """

    def __init__(self, config: SparkIcebergConfigModel):
        """
        Initializes the SparkConnector with the given configuration.

        Args:
            config (SparkIcebergConfigModel): The configuration model with parameters for the connection.
        """
        self.__app_name = config.get("app_name")
        self.__master = config.get("master")
        self.__spark_config = config.get("config")
        self.__catalog_name = config.get("catalog_name")

    @property
    def catalog_name(self):
        """
        The catalog name property.

        Returns:
            str: The catalog name used by Spark.
        """
        return self.__catalog_name

    def connect(self):
        """
        Establishes a connection to the Spark cluster and creates a SparkSession.

        Returns:
            pyspark.sql.SparkSession: Spark session for executing SQL queries.
        """
        conf = SparkConf().setAppName(self.__app_name).setMaster(self.__master)
        for key, value in self.__spark_config.items():
            conf.set(key, value)
        self.session = SparkSession.builder.config(conf=conf).getOrCreate()
        return self.session

    def query(self, query: str):
        """
        Executes a SQL query on Spark and returns the result DataFrame.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            pyspark.sql.DataFrame: The DataFrame with the results of the query.
        """
        return self.session.sql(query)
