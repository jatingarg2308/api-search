import yaml
from easydict import EasyDict

def get_metadata():
    with open('table_metadata.yaml', 'r') as f:
        return EasyDict(yaml.safe_load(f.read()))