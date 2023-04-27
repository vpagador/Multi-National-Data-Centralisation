import yaml
from yaml.loader import SafeLoader

def read_db_creds():
        with open('db_creds.yaml') as f:
            data = yaml.load(f, Loader=SafeLoader)
            print(data)


read_db_creds()