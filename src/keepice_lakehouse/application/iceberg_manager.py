from typing import List
from typing import Optional

from ..connectors.base_connector import BaseConnector
from ..exceptions.exceptions import DatabaseCreationError
from ..exceptions.exceptions import InvalidTablePropertyError
from ..exceptions.exceptions import MetadataRetrievalError
from ..exceptions.exceptions import TableCreationError
from ..exceptions.exceptions import TableDropError


class IcebergManager:
    def __init__(self, connector: BaseConnector):
        self.connector = connector
        self.connection = self.connector.connect()

    def list_databases(self) -> List[str]:
        list_databases_query = """
            SHOW DATABASES;
        """
        df = self.connector.query(query=list_databases_query)
        return df

    def list_tables(self, database_name: str) -> List[str]:
        list_tables_query = f"SHOW TABLES IN {database_name};"
        df = self.connector.query(query=list_tables_query)
        return df

    def create_database(self, database_name: str):
        try:
            create_database_query = f"""CREATE DATABASE IF NOT EXISTS {database_name};"""
            self.connector.query(query=create_database_query)
        except Exception as e:
            raise DatabaseCreationError(str(e)) from e

    def check_if_database_exist(self, database_name: str) -> bool:
        database_list = self.list_databases()
        return database_name in database_list

    def check_if_table_exist(self, database_name: str, table_name: str) -> bool:
        table_list = self.list_tables(database_name=database_name)
        return table_name in table_list

    def get_table_ddl(self, database_name: str, table_name: str) -> str:
        get_ddl_query = f"SHOW CREATE TABLE {self.connector.catalog_name}.{database_name}.{table_name};"
        df = self.connector.query(query=get_ddl_query)
        return df

    def create_table(self, database_name: str, table_name: str, columns: dict, s3_folder_location: str, partition_column: Optional[str]):
        """
        database_name: data base name where table will be created
        table_name: name of the table
        columns: dict. iceberg columns {column_name:column_type,..., column_name_n:column_type_n}
        s3_folder_location: str. S3 prefix where data will be stored
        partition_column: str. Optional. Indicates the partition. It need to exist inside columns dict
        """
        try:
            column_str = ", ".join([f"{column} {data_type}" for column, data_type in columns.items()])

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.connector.catalog_name}.{database_name}.{table_name} (
                    {column_str}
                )
                USING iceberg
                LOCATION '{s3_folder_location}'
            """

            if partition_column is not None:
                create_table_query += f"\n PARTITIONED BY ({partition_column})"
            create_table_query += ";"

            self.connector.query(create_table_query)

        except Exception as e:
            raise TableCreationError(str(e)) from e

    def drop_table(self, database_name: str, table_name: str):
        try:
            drop_table_query = f"""
                DROP TABLE {self.connector.catalog_name}.{database_name}.{table_name};
            """
            self.connector.query(query=drop_table_query)
        except Exception as e:
            raise TableDropError(str(e)) from e

    def get_property(self, database_name: str, table_name: str, table_property: str):
        # Check if the table property is valid
        permitted_values = {"partitions", "snapshots", "history", "files", "manifests", "refs"}
        if table_property not in permitted_values:
            raise InvalidTablePropertyError(f"Invalid table_property: {table_property}. Allowed values are {', '.join(permitted_values)}.")

        get_property_query = f"SELECT * FROM {self.connector.catalog_name}.{database_name}.{table_name}${table_property};"

        try:
            return self.connector.query(query=get_property_query)
        except Exception as e:
            raise MetadataRetrievalError(str(e)) from e

    def close(self):
        if hasattr(self.connection, "stop"):
            self.connection.stop()
