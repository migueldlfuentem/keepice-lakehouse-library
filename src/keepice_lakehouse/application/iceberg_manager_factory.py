from pathlib import Path

import yaml
from pydantic import ValidationError

from ..containers.containers import ConnectorsContainer
from ..containers.wiring import create_iceberg_manager
from ..models.models import ConfigModel
from ..utils.enums import ConnectorType
from ..utils.utils import find_config_folder


class IcebergManagerFactory:
    def __init__(self):
        self.container = ConnectorsContainer()
        self._setup_container()

    def _setup_container(self):
        # Find the config folder dynamically
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
        try:
            connector_type = ConnectorType[connector_name.upper()]
            return create_iceberg_manager(connector_type, container=self.container)
        except KeyError as e:
            raise ValueError(f"Unknown connector type: {connector_name}") from e
