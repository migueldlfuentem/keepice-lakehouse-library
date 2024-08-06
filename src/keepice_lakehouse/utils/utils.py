from pathlib import Path


def find_config_folder(start_path=None):
    """
    Finds the 'config' folder by traversing the parent directories starting from a given path.

    This function starts from the specified `start_path` (or the directory of the current script if not specified)
    and traverses upwards through the parent directories to find a folder named 'config'. If such a folder is found,
    its path is returned. If the 'config' folder is not found, a FileNotFoundError is raised.

    Args:
        start_path (Path or str, optional): The starting path for the search. If not provided, the directory of the current
                                            script (__file__) is used.

    Returns:
        Path: The path to the 'config' folder if found.

    Raises:
        FileNotFoundError: If the 'config' folder is not found in any of the parent directories.
    """
    if start_path is None:
        start_path = Path(__file__).resolve().parent
    else:
        start_path = Path(start_path).resolve()

    print(start_path)

    for parent in start_path.parents:
        config_path = parent / "config"
        print(config_path)
        if config_path.is_dir():
            return config_path

    raise FileNotFoundError("Config folder not found")
