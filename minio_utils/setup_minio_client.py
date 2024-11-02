import json
import socket

from pathlib import Path
from typing import Dict, Optional, List

import boto3
from botocore.client import Config, BaseClient
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError

import pkg_resources


class MinioConfig:
    DEFAULT_CONFIG_FILE_PATH: str = pkg_resources.resource_filename(__name__, "minio_configs.json")
    MAC_HOSTNAME: str = "Antons-MacBook-Pro-3.local"

    def __init__(self, config_file: str = DEFAULT_CONFIG_FILE_PATH) -> None:
        self.config_file_path: Path = Path(config_file)
        self.configs: Optional[Dict[str, Dict[str, str]]] = None
        self.endpoint_url: Optional[str] = None
        self.aws_access_key_id: Optional[str] = None
        self.aws_secret_access_key: Optional[str] = None
        self.region_name: Optional[str] = None
        self.signature_version: Optional[str] = None

    def load_config(self) -> None:
        """Load and validate the configuration file."""
        self.config_file_path = self.get_valid_config_path()
        self.configs = self.read_config(self.config_file_path)

    def get_valid_config_path(self) -> Path:
        """Return a valid config path or prompt to select one."""
        if self.config_file_path.exists():
            return self.config_file_path
        return self.prompt_for_config_file()

    def prompt_for_config_file(self) -> Path:
        """Prompt user to select a configuration file if default is missing."""
        available_files = list(self.config_file_path.parent.glob('*.json'))
        if not available_files:
            raise FileNotFoundError(f"No JSON configuration files found in {self.config_file_path.parent}")

        print("Default configuration file not found. Available files:")
        for idx, file in enumerate(available_files, 1):
            print(f"{idx}: {file}")
        file_choice = int(input("Select a file by entering the corresponding number: ")) - 1

        if 0 <= file_choice < len(available_files):
            return available_files[file_choice]
        else:
            raise ValueError("Invalid selection. Please select a valid number from the list.")

    @staticmethod
    def read_config(path: Path) -> Dict[str, Dict[str, str]]:
        """Read and return configuration file content."""
        try:
            with open(path, 'r') as file:
                print(f"Loaded configuration from: {path}")
                return json.load(file)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in '{path}': {e}")

    def list_machines(self) -> List[str]:
        """List available machine configurations."""
        self.ensure_configs_loaded()
        return list(self.configs.keys())

    def select_machine(self, machine_name: str) -> None:
        """Select a machine and set the connection attributes."""
        self.ensure_configs_loaded()
        selected_config = self.get_machine_config(machine_name)
        self.set_connection_attributes(selected_config)
        self.adjust_endpoint_ip()

    def get_machine_config(self, machine_name: str) -> Dict[str, str]:
        """Retrieve configuration for the specified machine."""
        config = self.configs.get(machine_name)
        if not config:
            raise ValueError(f"Machine '{machine_name}' not found in configuration.")
        return config

    def set_connection_attributes(self, config: Dict[str, str]) -> None:
        """Set connection attributes from configuration."""
        self.endpoint_url = config.get('endpoint_url')
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.region_name = config.get('region_name', 'us-east-1')
        self.signature_version = config.get('signature_version', 's3v4')
        self.validate_attributes()

    def validate_attributes(self) -> None:
        """Ensure essential connection attributes are set."""
        if not all([self.endpoint_url, self.aws_access_key_id, self.aws_secret_access_key]):
            raise ValueError("Configuration is missing required attributes.")

    def ensure_configs_loaded(self) -> None:
        """Ensure configuration file is loaded."""
        if not self.configs:
            raise ValueError("Configuration file has not been loaded. Call 'load_config' first.")

    def get_mac_ip(self) -> Optional[str]:
        """Retrieve the Mac's IP address based on hostname."""
        try:
            return socket.gethostbyname(self.MAC_HOSTNAME)
        except socket.gaierror:
            print("Unable to resolve the Mac's IP. Ensure the hostname is correct and reachable.")
            return None

    def adjust_endpoint_ip(self) -> None:
        """Adjust endpoint URL if the current IP differs from the Mac's IP."""
        mac_ip = self.get_mac_ip()
        current_ip = socket.gethostbyname(socket.gethostname())

        if mac_ip and current_ip != mac_ip and self.endpoint_url:
            self.endpoint_url = self.endpoint_url.replace(mac_ip, current_ip)
            print(f"Endpoint URL updated to current IP ({current_ip}): {self.endpoint_url}")

    def get_minio_client(self) -> BaseClient:
        """Instantiate and return a MinIO client."""
        self.validate_attributes()
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=Config(signature_version=self.signature_version),
            region_name=self.region_name
        )

    def setup_default_client(self, machine_name: str = 'mac') -> BaseClient:
        """Setup and return MinIO client with default machine configuration."""
        self.load_config()
        self.select_machine(machine_name)
        client = self.get_minio_client()
        self.test_connection(client)
        return client

    @staticmethod
    def test_connection(client: BaseClient) -> None:
        """Test connection by attempting to list buckets."""
        try:
            response = client.list_buckets()
            if response['Buckets']:
                print("Connection successful! Buckets found:")
                for bucket in response['Buckets']:
                    print(f" - {bucket['Name']}")
            else:
                print("Connection successful, but no buckets found.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Credentials error: {e}")
        except EndpointConnectionError:
            print("Unable to connect to the endpoint. Check your connection or endpoint URL.")
        except Exception as e:
            print(f"An error occurred: {e}")