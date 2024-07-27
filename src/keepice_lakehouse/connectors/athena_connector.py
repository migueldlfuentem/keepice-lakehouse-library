from pyathena import connect as athena_connect
from sqlalchemy import create_engine

from ..models.models import AthenaConfigModel
from .base_connector import BaseConnector


class AthenaConnector(BaseConnector):
    def __init__(self, config: AthenaConfigModel):
        self.region_name = config.get("region_name")
        self.s3_staging_dir = config.get("s3_staging_dir")
        self.workgroup = config.get("workgroup")
        self.warehouse = config.get("warehouse")
        self.__catalog_name = config.get("catalog_name")

    @property
    def catalog_name(self):
        return self.__catalog_name

    def connect(self):
        self.connection = athena_connect(s3_staging_dir=self.s3_staging_dir, region_name=self.region_name)
        return create_engine("awsathena://", creator=lambda: self.connection)

    def query(self, query: str):
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor
