
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv


def load_config() -> dict:
    """
    Loads configuration from config.yml and .env files.

    The .env file variables can override the YAML configuration
    if the keys are the same.

    Returns:
        A dictionary containing the merged configuration.
    """
    # Find the project root by looking for the .git directory
    project_root = Path(__file__).parent.parent.parent

    # Load the .env file from the project root
    env_path = project_root / '.env'
    load_dotenv(dotenv_path=env_path)

    # Load the main YAML config file
    config_path = project_root / 'config' / 'config.yml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # You can add logic here to allow environment variables to
    # override YAML values if needed, which is a common pattern.
    # For now, we'll just return the YAML config.

    return config


# Load the configuration once when the module is imported
# This acts as a singleton, ensuring the config is read only once.
config = load_config()