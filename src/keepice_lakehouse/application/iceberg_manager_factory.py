from pathlib import Path

import yaml
from pydantic import ValidationError

from ..containers.containers import ConnectorsContainer
from ..containers.wiring import create_iceberg_manager
from ..models.models import ConfigModel
from ..utils.enums import ConnectorType
from ..utils.utils import find_config_folder


class IcebergManagerFactory:
    """
    A factory class for creating Iceberg managers.

    This class sets up a container with configuration loaded from a YAML file
    and provides a method to retrieve an Iceberg manager based on the connector name.

    Attributes:
        container (ConnectorsContainer): The container used for managing dependencies and configurations.
    """

    def __init__(self):
        """
        Initializes the IcebergManagerFactory instance.

        Sets up the container and loads configuration from the default YAML file.
        """
        self.container = ConnectorsContainer()
        self._setup_container()

    def _setup_container(self):
        """
        Configures the container with settings loaded from a YAML configuration file.

        The method finds the configuration folder, loads the configuration from the
        "connectors_config.yaml" file, and initializes the container with these settings.

        Raises:
            ValueError: If the configuration is invalid or cannot be loaded.
        """
        config_path = find_config_folder() / "connectors_config.yaml"
        with Path.open(config_path) as file:
            config_data = yaml.safe_load(file)

        try:
            config = ConfigModel(**config_data)
            self.container.config.from_dict(config.model_dump(mode="json"))
        except ValidationError as e:
            raise ValueError(f"Invalid configuration: {e}") from e

        self.container.wire(modules=["..containers.wiring"])

    def get_manager(self, connector_name: str):
        """
        Retrieves an Iceberg manager based on the provided connector name.

        Args:
            connector_name (str): The name of the connector for which the manager is to be created.

        Returns:
            IcebergManager: An instance of the Iceberg manager corresponding to the specified connector type.

        Raises:
            ValueError: If the connector name is unknown or not recognized.
        """
        try:
            connector_type = ConnectorType[connector_name.upper()]
            return create_iceberg_manager(connector_type, container=self.container)
        except KeyError as e:
            raise ValueError(f"Unknown connector type: {connector_name}") from e
