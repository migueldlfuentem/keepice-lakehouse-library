=====
Usage
=====

Overview
=============================

The keepice-lakehouse library provides a flexible and powerful way to manage Iceberg tables using different backends such as Spark and Athena. This guide will help you understand how to use the library effectively.

Connection Setup
===================================

To use the ``keepice_lakehouse_library`` effectively, it is crucial to set up a `connectors_config.yaml` file within your project. This file should be placed in a folder named `config` within your project directory. The `connectors_config.yaml` file defines the configuration settings for different Iceberg managers and ensures that the library connects to the appropriate backends with the correct parameters.

The `connectors_config.yaml` file should be located at:

.. code-block:: text

    <project_root>/
    └── config/
        └── connectors_config.yaml

Here is an example of a `connectors_config.yaml` file:

.. code-block:: yaml

    connectors:
      spark_iceberg:
        app_name: "MySparkIcebergApp"
        master: "local[*]"
        config:
          spark.executor.memory: "2g"
          spark.driver.memory: "1g"
          spark.executor.cores: "2"
          spark.sql.catalog.my_catalog: "org.apache.iceberg.spark.SparkCatalog"
          spark.sql.catalog.my_catalog.type: "hadoop"
          spark.sql.catalog.my_catalog.warehouse: "s3://my-warehouse/"
          spark.sql.catalog.my_catalog.uri: "http://localhost:8080"
        catalog_name: "demo"
      athena:
        region_name: "us-west-2"
        s3_staging_dir: "s3://my-athena-query-results/"
        workgroup: "primary"
        catalog_name: "my_catalog"

Example of use
=============================

Here's a step-by-step guide on how to use the keepice-lakehouse library to create and manage Iceberg tables.


.. code-block:: python

    import keepice_lakehouse_library

    # Create an instance of IcebergManagerFactory
    factory = IcebergManagerFactory()

    # Get a Spark Iceberg manager
    spark_manager = factory.get_manager('spark_iceberg')
    athena_manager = factory.get_manager('athena')

    # Create a database
    spark_manager.create_database(database_name='test')

    # Define schema for the table
    schema_dict = {
        "VendorID": "bigint",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "double",
        "trip_distance": "double",
        "RatecodeID": "double",
        "store_and_fwd_flag": "string",
        "PULocationID": "bigint",
        "DOLocationID": "bigint",
        "payment_type": "bigint",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double"
    }

    # Create a table
    spark_manager.create_table(
        database_name='test',
        table_name='taxi_test_table',
        columns=schema_dict,
        s3_folder_location="s3://warehouse/test/taxi-test-table",
        partition_column="days(tpep_pickup_datetime)"
    )

    # List databases
    print(spark_manager.list_databases().show())

    # List tables in a database
    print(spark_manager.list_tables(database_name='test').show())

    # Get table DDL
    df = spark_manager.get_table_ddl(database_name='test', table_name='taxi_test_table')
    ddl = df.select('createtab_stmt').rdd.flatMap(lambda x: x).collect()[0]
    print(ddl)

    # Insert incremental data into a table
    for filename in [
        "yellow_tripdata_2022-04.parquet",
        "yellow_tripdata_2022-03.parquet",
        "yellow_tripdata_2022-02.parquet",
        "yellow_tripdata_2022-01.parquet",
        "yellow_tripdata_2021-12.parquet",
    ]:
        df = spark.read.parquet(f"/home/iceberg/data/{filename}")
        df.createOrReplaceTempView("temporal_table")
        spark_manager.insert_incremental_table_data(
            source_table="temporal_table",
            database_name="test",
            table_name="taxi_test_table"
        )

    # Insert bulk data into a table
    for filename in [
        "yellow_tripdata_2021-04.parquet",
        "yellow_tripdata_2021-07.parquet"
    ]:
        df = spark.read.parquet(f"/home/iceberg/data/{filename}")
        df.createOrReplaceTempView("temporal_table")
        spark_manager.insert_bulk_table_data(
            source_table="temporal_table",
            database_name="test",
            table_name="taxi_test_table"
        )


Summary of Code Functionality
=============================

The provided code utilizes the ``keepice_lakehouse_library`` to manage Iceberg tables using a Spark backend. Here’s a detailed breakdown of what the code does:

1. **Import Library and Create Factory Instance**:
   - The code starts by importing the ``keepice_lakehouse_library``.
   - It then creates an instance of ``IcebergManagerFactory`` to manage different types of Iceberg managers.

2. **Get Spark and Athena Managers**:
   - The factory is used to create a Spark Iceberg manager (``spark_manager``) and an Athena manager (``athena_manager``).

3. **Create Database**:
   - A new database named ``test`` is created using the Spark Iceberg manager.

4. **Define Table Schema**:
   - A schema for a table is defined in a dictionary (``schema_dict``). This schema includes columns like ``VendorID``, ``tpep_pickup_datetime``, ``passenger_count``, etc., with their corresponding data types.

5. **Create Table**:
   - A table named ``taxi_test_table`` is created in the ``test`` database using the previously defined schema. The table is stored at the specified S3 location and partitioned by the ``tpep_pickup_datetime`` column.

6. **List Databases and Tables**:
   - The code lists all databases managed by the Spark manager.
   - It also lists all tables within the ``test`` database.

7. **Get Table DDL**:
   - The Data Definition Language (DDL) statement for the ``taxi_test_table`` in the ``test`` database is retrieved and printed. This DDL includes the SQL statement used to create the table.

8. **Insert Incremental Data**:
   - The code reads multiple Parquet files (representing yellow taxi trip data) into Spark DataFrames.
   - For each DataFrame, a temporary table named ``temporal_table`` is created.
   - Incremental data from these temporary tables is inserted into the ``taxi_test_table`` in the ``test`` database.

9. **Insert Bulk Data**:
   - Similarly, the code reads additional Parquet files into Spark DataFrames.
   - Temporary tables are created for these DataFrames.
   - Bulk data from these temporary tables is inserted into the ``taxi_test_table`` in the ``test`` database.

Overall, this code demonstrates how to use the ``keepice_lakehouse_library`` to create and manage Iceberg tables with Spark, including creating databases and tables, defining schemas, listing databases and tables, retrieving table DDL, and inserting both incremental and bulk data into tables.

Testing `keepice_lakehouse` Locally with Spark
==========================================================

This guide will help you set up a local Spark and Iceberg environment using Docker. This setup will allow you to test the `keepice_lakehouse` library and its integration with Spark.

Setting Up with Docker-Compose
-------------------------------

The quickest way to get started is by using a `docker-compose` file that sets up a local Spark cluster with an Iceberg catalog. Ensure you have Docker and Docker Compose installed on your machine.

1. **Create a `docker-compose.yml` File**

   Save the following YAML configuration into a file named `docker-compose.yml`:

   .. code-block:: yaml

       version: "3"

       services:
         spark-iceberg:
           image: tabulario/spark-iceberg
           container_name: spark-iceberg
           build: spark/
           networks:
             iceberg_net:
           depends_on:
             - rest
             - minio
           volumes:
             - ./warehouse:/home/iceberg/warehouse
             - ./notebooks:/home/iceberg/notebooks/notebooks
           environment:
             - AWS_ACCESS_KEY_ID=admin
             - AWS_SECRET_ACCESS_KEY=password
             - AWS_REGION=us-east-1
           ports:
             - 8888:8888
             - 8080:8080
             - 10000:10000
             - 10001:10001
         rest:
           image: tabulario/iceberg-rest
           container_name: iceberg-rest
           networks:
             iceberg_net:
           ports:
             - 8181:8181
           environment:
             - AWS_ACCESS_KEY_ID=admin
             - AWS_SECRET_ACCESS_KEY=password
             - AWS_REGION=us-east-1
             - CATALOG_WAREHOUSE=s3://warehouse/
             - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
             - CATALOG_S3_ENDPOINT=http://minio:9000
         minio:
           image: minio/minio
           container_name: minio
           environment:
             - MINIO_ROOT_USER=admin
             - MINIO_ROOT_PASSWORD=password
             - MINIO_DOMAIN=minio
           networks:
             iceberg_net:
               aliases:
                 - warehouse.minio
           ports:
             - 9001:9001
             - 9000:9000
           command: ["server", "/data", "--console-address", ":9001"]
         mc:
           depends_on:
             - minio
           image: minio/mc
           container_name: mc
           networks:
             iceberg_net:
           environment:
             - AWS_ACCESS_KEY_ID=admin
             - AWS_SECRET_ACCESS_KEY=password
             - AWS_REGION=us-east-1
           entrypoint: >
             /bin/sh -c "
             until (/usr/bin/mc config host add minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
             /usr/bin/mc rm -r --force minio/warehouse;
             /usr/bin/mc mb minio/warehouse;
             /usr/bin/mc policy set public minio/warehouse;
             tail -f /dev/null
             "
       networks:
         iceberg_net:

2. **Start the Docker Containers**

   Run the following command to start up the Docker containers:

   .. code-block:: bash

       docker-compose up

Running Spark Commands
----------------------

Once the containers are up and running, you can start a Spark session using the following commands:

- **Spark SQL CLI**

  .. code-block:: bash

      docker exec -it spark-iceberg spark-sql

- **Spark Shell**

  .. code-block:: bash

      docker exec -it spark-iceberg spark-shell

- **PySpark**

  .. code-block:: bash

      docker exec -it spark-iceberg pyspark

Additional Notes
----------------

- **Notebook Server**

  You can also launch a Jupyter notebook server to interact with Spark by running:

  .. code-block:: bash

      docker exec -it spark-iceberg notebook

  The notebook server will be available at `http://localhost:8888 <http://localhost:8888>`_.

- **Docker Image Information**

  For more details on the Docker image used, visit the `Tabulario Spark-Iceberg Docker Hub page <https://hub.docker.com/r/tabulario/spark-iceberg>`_.
