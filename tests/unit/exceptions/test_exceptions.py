from keepice_lakehouse.exceptions.exceptions import DatabaseCreationError
from keepice_lakehouse.exceptions.exceptions import InvalidTablePropertyError
from keepice_lakehouse.exceptions.exceptions import MetadataRetrievalError
from keepice_lakehouse.exceptions.exceptions import TableCreationError
from keepice_lakehouse.exceptions.exceptions import TableDropError


def test_database_creation_error():
    message = "Failed to create the database."
    error = DatabaseCreationError(message)

    assert isinstance(error, DatabaseCreationError)
    assert str(error) == f"Database Creation Error: {message}"


def test_table_creation_error():
    message = "Failed to create the table."
    error = TableCreationError(message)

    assert isinstance(error, TableCreationError)
    assert str(error) == f"Table Creation Error: {message}"


def test_table_drop_error():
    message = "Failed to drop the table."
    error = TableDropError(message)

    assert isinstance(error, TableDropError)
    assert str(error) == f"Table Drop Error: {message}"


def test_metadata_retrieval_error():
    message = "Failed to retrieve metadata."
    error = MetadataRetrievalError(message)

    assert isinstance(error, MetadataRetrievalError)
    assert str(error) == f"Metadata Retrieval Error: {message}"


def test_invalid_table_property_error():
    message = "The table property is invalid."
    error = InvalidTablePropertyError(message)

    assert isinstance(error, InvalidTablePropertyError)
    assert str(error) == f"Invalid Table Property: {message}"
