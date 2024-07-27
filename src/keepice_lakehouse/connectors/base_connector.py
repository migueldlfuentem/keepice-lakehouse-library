from abc import abstractmethod


class BaseConnector:
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def query(self):
        pass
