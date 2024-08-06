from abc import abstractmethod

"""
This module defines an abstract base class for connectors.

Connectors are objects that handle the connection and querying of data sources,
such as databases, APIs, or file systems. This abstract base class defines the
interface for connectors, ensuring that all subclasses implement the `connect`
and `query` methods.

Classes:
    BaseConnector: The abstract base class for connectors.
"""


class BaseConnector:
    """
    Abstract base class for connectors.

    This class defines the interface for connectors, which should provide methods to
    establish connections and execute queries. Subclasses must implement the `connect`
    and `query` methods.

    Methods:
        connect: Establishes a connection. Must be implemented by subclasses.
        query: Executes a query. Must be implemented by subclasses.
    """

    @abstractmethod
    def connect(self):
        """
        Establishes a connection.

        This method must be implemented by subclasses to establish a connection to a data source.
        """
        pass

    @abstractmethod
    def query(self, query: str):
        """
        Executes a query.

        This method must be implemented by subclasses to execute a query on the connected data source.

        Args:
            query (str): The query to be executed.
        """
        pass
