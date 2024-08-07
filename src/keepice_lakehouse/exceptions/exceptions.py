class IcebergManagerError(Exception):
    """Base class for exceptions in IcebergManager."""


class DatabaseCreationError(IcebergManagerError):
    """Exception raised for errors in database creation."""

    def __init__(self, message: str):
        super().__init__(f"Database Creation Error: {message}")


class TableCreationError(IcebergManagerError):
    """Exception raised for errors in table creation."""

    def __init__(self, message: str):
        super().__init__(f"Table Creation Error: {message}")


class TableDropError(IcebergManagerError):
    """Exception raised for errors in dropping a table."""

    def __init__(self, message: str):
        super().__init__(f"Table Drop Error: {message}")


class MetadataRetrievalError(IcebergManagerError):
    """Exception raised for errors in retrieving metadata."""

    def __init__(self, message: str):
        super().__init__(f"Metadata Retrieval Error: {message}")


class InvalidTablePropertyError(IcebergManagerError):
    """Exception raised for invalid table properties."""

    def __init__(self, message: str):
        super().__init__(f"Invalid Table Property: {message}")
