from pathlib import Path

import yaml


def read_config():
    with Path("config.yaml").open() as file:
        return yaml.safe_load(file)


class Config:
    def __init__(self, section_name):
        file = read_config()
        self.config = None
        self.read_config_section(file, section_name)

    def read_config_section(self, file, sect):
        self.config = file.get(sect, {})

    def __getitem__(self, key):
        return self.config[key]

    def __getattr__(self, key):
        try:
            return self.config[key]
        except KeyError as e:
            raise AttributeError(f"'Config' object has no attribute '{key}'") from e

    def as_dict(self):
        return self.config
