import json
from typing import Any, Dict
from CommonServices.Logger import Logger

logger=Logger()
class Config:
    _instance = None


    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._config_data = {}
        return cls._instance

    def load_config(self, file_path: str) -> None:
        try:
            logger.info(f"Loading server configuration from: {file_path}")
            print(f"[INFO] Loading server configuration from: {file_path}")
            with open(file_path, 'r') as f:
                self._config_data = json.load(f)
                
        except FileNotFoundError:
            logger.error(f"[ERROR] Configuration file not found: {file_path}")
            print(f"[ERROR] Configuration file not found: {file_path}")
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from the file: {file_path}")
            print(f"[ERROR] Error decoding JSON from the file: {file_path}")
        except Exception as e:
            logger.error(f"Unexpected error while loading config: {file_path}")
            print(f"[ERROR] Unexpected error while loading config: {file_path} - {e}")

    def get(self, key: str, default: Any = None) -> Any:
        return self._config_data.get(key, default)
