from pathlib import Path

import pytest

from keepice_lakehouse.utils.utils import find_config_folder


def test_default_behavior(tmp_path):
    # Create the directory structure
    script_dir = tmp_path / "some" / "path" / "to"
    script_dir.mkdir(parents=True)

    config_folder = tmp_path / "some" / "config"
    config_folder.mkdir(parents=True)

    # Use a temporary path as __file__ for testing
    original_resolve = Path.resolve
    Path.resolve = lambda path: script_dir

    try:
        # Run the function to test
        result = find_config_folder()

        # Assert the result
        assert result == config_folder
    finally:
        # Restore original resolve method
        Path.resolve = original_resolve


def test_specific_start_path(tmp_path):
    # Create a directory structure
    config_folder = tmp_path / "other" / "config"
    config_folder.mkdir(parents=True)
    start_path = tmp_path / "other" / "path" / "start"

    # Run the function to test
    result = find_config_folder(start_path)

    # Assert the result
    assert result == config_folder


def test_config_not_found(mocker):
    start_path = Path("/no/config/here")
    mock_parents = mocker.patch("pathlib.Path.parents")
    mock_parents.__iter__.return_value = iter([Path("/no/config"), Path("/no"), Path("/")])

    mock_is_dir = mocker.patch("pathlib.Path.is_dir")
    mock_is_dir.return_value = False

    with pytest.raises(FileNotFoundError):
        find_config_folder(start_path)
