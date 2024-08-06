from typing import List
from typing import Optional

from ..connectors.base_connector import BaseConnector
from ..exceptions.exceptions import DatabaseCreationError
from ..exceptions.exceptions import InvalidTablePropertyError
from ..exceptions.exceptions import MetadataRetrievalError
from ..exceptions.exceptions import TableCreationError
from ..exceptions.exceptions import TableDropError


class IcebergManager:
    """
    A manager class for performing operations on Iceberg tables via a database connector.

    This class provides methods to interact with databases and tables, including listing databases and tables,
    creating and dropping tables, retrieving table properties, and inserting or updating data.

    Attributes:
        connector (BaseConnector): The connector used to interact with the database.
        connection: The database connection established through the connector.
    """

    def __init__(self, connector: BaseConnector):
        """
        Initializes the IcebergManager instance.

        Args:
            connector (BaseConnector): An instance of BaseConnector used for connecting to the database.
        """
        self.connector = connector
        self.connection = self.connector.connect()

    def list_databases(self) -> List[str]:
        """
        Lists all databases available in the connected database system.

        Returns:
            List[str]: A list of database names.

        Raises:
            Exception: If the query execution fails.
        """
        list_databases_query = """
            SHOW DATABASES;
        """
        results = self.connector.query(query=list_databases_query)
        return results

    def list_tables(self, database_name: str) -> List[str]:
        """
        Lists all tables within a specified database.

        Args:
            database_name (str): The name of the database to list tables from.

        Returns:
            List[str]: A list of table names within the specified database.

        Raises:
            Exception: If the query execution fails.
        """
        list_tables_query = f"SHOW TABLES IN {database_name};"
        results = self.connector.query(query=list_tables_query)
        return results

    def create_database(self, database_name: str):
        """
        Creates a new database if it does not already exist.

        Args:
            database_name (str): The name of the database to create.

        Raises:
            DatabaseCreationError: If the database creation query fails.
        """
        try:
            create_database_query = f"""CREATE DATABASE IF NOT EXISTS {database_name};"""
            self.connector.query(query=create_database_query)
        except Exception as e:
            raise DatabaseCreationError(str(e)) from e

    def get_table_ddl(self, database_name: str, table_name: str):
        """
        Retrieves the DDL (Data Definition Language) statement for creating a specified table.

        Args:
            database_name (str): The name of the database containing the table.
            table_name (str): The name of the table to retrieve the DDL for.

        Returns:
            str: The DDL statement for the specified table.

        Raises:
            Exception: If the query execution fails.
        """
        get_ddl_query = f"SHOW CREATE TABLE {self.connector.catalog_name}.{database_name}.{table_name};"
        results = self.connector.query(query=get_ddl_query)
        return results

    def create_table(
        self, database_name: str, table_name: str, columns: dict, s3_folder_location: str, partition_column: Optional[str] = None
    ):
        """
        Creates a new table in the specified database with the given columns and configuration.

        Args:
            database_name (str): The name of the database where the table will be created.
            table_name (str): The name of the table to create.
            columns (dict): A dictionary of column names and their types.
            s3_folder_location (str): The S3 location where table data will be stored.
            partition_column (Optional[str]): The column by which to partition the table. Must be one of the columns.

        Raises:
            TableCreationError: If the table creation query fails.
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
        """
        Drops a specified table from the database.

        Args:
            database_name (str): The name of the database containing the table.
            table_name (str): The name of the table to drop.

        Raises:
            TableDropError: If the table drop query fails.
        """
        try:
            drop_table_query = f"""
                DROP TABLE {self.connector.catalog_name}.{database_name}.{table_name};
            """
            self.connector.query(query=drop_table_query)
        except Exception as e:
            raise TableDropError(str(e)) from e

    def get_property(self, database_name: str, table_name: str, table_property: str):
        """
        Retrieves a specific property of a table.

        Args:
            database_name (str): The name of the database containing the table.
            table_name (str): The name of the table to retrieve the property from.
            table_property (str): The property to retrieve. Must be one of "partitions", "snapshots", "history", "files", "manifests", "refs".

        Returns:
            list: A list of values for the specified property.

        Raises:
            InvalidTablePropertyError: If the table_property is not one of the permitted values.
            MetadataRetrievalError: If the query execution fails.
        """
        permitted_values = {"partitions", "snapshots", "history", "files", "manifests", "refs"}
        if table_property not in permitted_values:
            raise InvalidTablePropertyError(f"Invalid table_property: {table_property}. Allowed values are {', '.join(permitted_values)}.")

        get_property_query = f"SELECT * FROM {self.connector.catalog_name}.{database_name}.{table_name}${table_property};"

        try:
            return self.connector.query(query=get_property_query)
        except Exception as e:
            raise MetadataRetrievalError(str(e)) from e

    def insert_bulk_table_data(self, source_table, database_name: str, table_name: str):
        """
        Inserts data from a source table into a specified table, deleting existing data first.

        Args:
            source_table: The source table to copy data from.
            database_name (str): The name of the database containing the target table.
            table_name (str): The name of the target table.
        """
        truncate_table_query = f"""DELETE FROM {self.connector.catalog_name}.{database_name}.{table_name};"""

        insert_table_query = f"""
            INSERT INTO {self.connector.catalog_name}.{database_name}.{table_name}
            SELECT * FROM {source_table}
            """

        self.connector.query(truncate_table_query)
        self.connector.query(insert_table_query)

    def insert_incremental_table_data(self, source_table, database_name: str, table_name: str):
        """
        Inserts new data from a source table into a specified table without deleting existing data.

        Args:
            source_table: The source table to copy data from.
            database_name (str): The name of the database containing the target table.
            table_name (str): The name of the target table.
        """
        insert_table_query = f"""
            INSERT INTO {self.connector.catalog_name}.{database_name}.{table_name}
            SELECT * FROM {source_table}
            """

        self.connector.query(insert_table_query)

    def upsert_delta_table_data(
        self, source_table, database_name: str, table_name: str, primary_key: str, order_col: str, source_table_pk: Optional[str] = None
    ):
        """
        Performs an upsert operation to merge data from a source table into a specified table.

        Args:
            source_table: The source table to merge data from.
            database_name (str): The name of the database containing the target table.
            table_name (str): The name of the target table.
            primary_key (str): The primary key column used for matching rows.
            order_col (str): The column used for ordering rows.
            source_table_pk (Optional[str]): The primary key column in the source table. Defaults to `primary_key` if not provided.

        Raises:
            Exception: If the merge query execution fails.
        """
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
