import os
import glob
import configparser


def _read_config():
    """A private helper to read and parse the config.ini file, avoiding repeated code."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config.read(config_path)
    return config


def get_output_path_from_config():
    """
    Reads the base output path from the config.ini file.
    This is now the single, central place to get this configuration.
    """
    config = _read_config()
    try:
        return config['file_paths']['base_output_path']
    except KeyError:
        print("Error: 'base_output_path' not found in config.ini")
        return None


def get_config_value(section, key, fallback=None, type_cast=None):
    """
    Reads a specific value from the config.ini file.
    Can optionally cast the value to a specific type (e.g., int).
    """
    config = _read_config()
    try:
        # Attempt to get the value, which could be of any type
        value = config.get(section, key)
        return type_cast(value) if type_cast else value
    except (configparser.NoSectionError, configparser.NoOptionError):
        if fallback is not None:
            print(f"'{key}' not found in config, using fallback: {fallback}")
        return fallback
    except Exception as e:
        print(f"Error reading config value '{key}': {e}")
        return fallback


def get_asset_class_paths(asset_class):
    """
    Gets the folder and filename prefix for a given asset class.
    Centralizes the naming logic (e.g., 'currency' maps to 'forex' folder).
    """
    if asset_class == 'currency':
        return 'forex', 'currency'
    return asset_class, asset_class

def find_latest_file(search_pattern):
    """
    Finds the most recently created file in a directory matching a pattern.

    :param search_pattern: The full path and file pattern (e.g., 'Y:/data/stocks-*-list.csv').
    :return: The path to the latest file, or None if not found.
    """
    try:
        list_of_files = glob.glob(search_pattern)
        if not list_of_files:
            print(f"Error: No files found matching pattern: {search_pattern}")
            return None

        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Found latest file: {os.path.basename(latest_file)}")
        return latest_file
    except Exception as e:
        print(f"Error finding latest file: {e}")
        return None


def get_unique_filepath(filepath):
    """
    Checks if a file exists. If so, appends a numeric suffix to make it unique.
    Example: 'data.csv' -> 'data(001).csv'

    :param filepath: The desired full path to the file.
    :return: A unique file path.
    """
    if not os.path.exists(filepath):
        return filepath

    directory, filename = os.path.split(filepath)
    name, ext = os.path.splitext(filename)

    counter = 1
    while True:
        # Format the new filename with a padded number (e.g., 001, 002)
        new_filename = f"{name}({counter:03d}){ext}"
        new_filepath = os.path.join(directory, new_filename)

        if not os.path.exists(new_filepath):
            print(f"Original filename existed. Using new unique name: {new_filename}")
            return new_filepath
        counter += 1
