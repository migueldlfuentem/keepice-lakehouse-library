from src.keepice_lakehouse import IcebergManagerFactory

factory = IcebergManagerFactory()

spark_manager = factory.get_manager("spark_iceberg")

print("Spark Iceberg Manager created successfully!")
