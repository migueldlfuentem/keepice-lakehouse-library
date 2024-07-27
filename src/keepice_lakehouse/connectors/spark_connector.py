from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from ..models.models import SparkIcebergConfigModel
from .base_connector import BaseConnector


class SparkConnector(BaseConnector):
    def __init__(self, config: SparkIcebergConfigModel):
        self.__app_name = config.get("app_name")
        self.__master = config.get("master")
        self.__spark_config = config.get("config")
        self.__catalog_name = config.get("catalog_name")

    @property
    def catalog_name(self):
        return self.__catalog_name

    def connect(self):
        conf = SparkConf().setAppName(self.__app_name).setMaster(self.__master)
        for key, value in self.__spark_config.items():
            conf.set(key, value)
        self.session = SparkSession.builder.config(conf=conf).getOrCreate()
        return self.session

    def query(self, query: str):
        return self.session.sql(query)
