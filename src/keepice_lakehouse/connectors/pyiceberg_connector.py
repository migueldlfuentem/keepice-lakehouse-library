from pyiceberg.catalog import load_catalog

from ..models.models import PyIcebergConfigModel
from .base_connector import BaseConnector


class PyIcebergConnector(BaseConnector):
    def __init__(self, config: PyIcebergConfigModel):
        self.warehouse = config.get("warehouse")
        self.uri = config.get("uri")
        self.__catalog_name = config.get("catalog_name")

    @property
    def catalog_name(self):
        return self.__catalog_name

    def connect(self):
        self.catalog = load_catalog(catalog_name=self.catalog_name, uri=self.uri, warehouse=self.warehouse)
        return self.catalog

    def query(self, query: str):
        # Implement query execution if applicable
        pass
