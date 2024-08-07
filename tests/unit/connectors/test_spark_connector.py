import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from keepice_lakehouse.connectors.spark_connector import SparkConnector
from keepice_lakehouse.models.models import SparkIcebergConfigModel


class TestSparkConnector(unittest.TestCase):
    @patch("keepice_lakehouse.connectors.spark_connector.SparkSession")
    def test_connect(self, mock_spark_session):
        input = {
            "app_name": "test_app",
            "master": "local",
            "config": {"spark.some.config.option": "some-value"},
            "catalog_name": "test_catalog",
        }
        config = SparkIcebergConfigModel(**input)

        connector = SparkConnector(config.model_dump(mode="json"))
        self.assertEqual(connector.catalog_name, "test_catalog")

        mock_spark_session.builder.config.return_value.getOrCreate.return_value = MagicMock()
        session = connector.connect()

        mock_spark_session.builder.config.assert_called_once()
        self.assertIsNotNone(session)

    @patch("keepice_lakehouse.connectors.spark_connector.SparkSession")
    def test_query(self, mock_spark_session):
        input = {
            "app_name": "test_app",
            "master": "local",
            "config": {"spark.some.config.option": "some-value"},
            "catalog_name": "test_catalog",
        }
        config = SparkIcebergConfigModel(**input)

        connector = SparkConnector(config.model_dump(mode="json"))
        mock_session = MagicMock()
        mock_spark_session.builder.config.return_value.getOrCreate.return_value = mock_session

        connector.connect()
        query = "SELECT * FROM table"
        connector.query(query)
        mock_session.sql.assert_called_with(query)
