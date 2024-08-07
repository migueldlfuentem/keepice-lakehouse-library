from src.keepice_lakehouse import IcebergManagerFactory

factory = IcebergManagerFactory()

spark_manager = factory.get_manager("spark_iceberg")

print("Spark Iceberg Manager created successfully!")

spark_manager.create_database(database_name="test")

print(spark_manager.list_databases().show())

athena_manager = factory.get_manager("athena")

print("Athena Manager created successfully!")
