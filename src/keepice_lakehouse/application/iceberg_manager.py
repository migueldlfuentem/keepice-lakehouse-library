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
        results = self.connector.query(query=list_databases_query)
        return results

    def list_tables(self, database_name: str) -> List[str]:
        list_tables_query = f"SHOW TABLES IN {database_name};"
        results = self.connector.query(query=list_tables_query)
        return results

    def create_database(self, database_name: str):
        try:
            create_database_query = f"""CREATE DATABASE IF NOT EXISTS {database_name};"""
            self.connector.query(query=create_database_query)
        except Exception as e:
            raise DatabaseCreationError(str(e)) from e

    def get_table_ddl(self, database_name: str, table_name: str):
        get_ddl_query = f"SHOW CREATE TABLE {self.connector.catalog_name}.{database_name}.{table_name};"
        results = self.connector.query(query=get_ddl_query)
        return results

    def create_table(
        self, database_name: str, table_name: str, columns: dict, s3_folder_location: str, partition_column: Optional[str] = None
    ):
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

    def insert_bulk_table_data(self, source_table, database_name: str, table_name: str):
        truncate_table_query = f"""DELETE FROM {self.connector.catalog_name}.{database_name}.{table_name};"""

        insert_table_query = f"""
            INSERT INTO {self.connector.catalog_name}.{database_name}.{table_name}
            SELECT * FROM {source_table}
            """

        self.connector.query(truncate_table_query)
        self.connector.query(insert_table_query)

    def insert_incremental_table_data(self, source_table, database_name: str, table_name: str):
        insert_table_query = f"""
            INSERT INTO {self.connector.catalog_name}.{database_name}.{table_name}
            SELECT * FROM {source_table}
            """

        self.connector.query(insert_table_query)

    def upsert_delta_table_data(
        self, source_table, database_name: str, table_name: str, primary_key: str, order_col: str, source_table_pk=None
    ):
        if source_table_pk is None or source_table_pk == "":
            source_table_pk = primary_key

        merge_delta_query = f"""MERGE INTO {self.connector.catalog_name}.{database_name}.{table_name} AS iceberg_table
                USING (
                    SELECT *
                        FROM (
                            SELECT
                                *,
                                ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {order_col} DESC ) AS row_rank
                            FROM
                                {source_table}
                        )
                    WHERE row_rank = 1
                ) AS temp_table
                ON iceberg_table.{primary_key} = temp_table.{source_table_pk}
                WHEN MATCHED AND avro_table.__action = 'd' THEN DELETE
                WHEN MATCHED AND avro_table.__action = 'u' THEN UPDATE SET *
                WHEN NOT MATCHED AND avro_table.__action != 'd' THEN INSERT *"""

        self.connector.query(merge_delta_query)

    def close(self):
        if hasattr(self.connection, "stop"):
            self.connection.stop()
