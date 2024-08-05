from pathlib import Path


def find_config_folder(start_path=None):
    if start_path is None:
        start_path = Path(__file__).resolve().parent
    print(start_path)
    for parent in start_path.parents:
        config_path = parent / "config"
        print(config_path)
        if config_path.is_dir():
            return config_path

    raise FileNotFoundError("Config folder not found")
