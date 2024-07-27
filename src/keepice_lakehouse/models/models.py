from typing import Dict
from typing import Optional

from pydantic import BaseModel


class SparkIcebergConfigModel(BaseModel):
    app_name: str
    master: str
    config: Dict[str, str]


class AthenaConfigModel(BaseModel):
    region_name: str
    s3_staging_dir: str
    workgroup: str
    iceberg_catalog: Optional[str] = None
    warehouse: Optional[str] = None


class PyIcebergConfigModel(BaseModel):
    catalog_name: str
    warehouse: str
    uri: str


class ConnectorsConfigModel(BaseModel):
    spark_iceberg: Optional[SparkIcebergConfigModel] = None
    athena: Optional[AthenaConfigModel] = None
    pyiceberg: Optional[PyIcebergConfigModel] = None


class ConfigModel(BaseModel):
    connectors: ConnectorsConfigModel
