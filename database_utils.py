import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine    


class DatabaseConnector:

    def __init__(self, engine = None, creds = None):
        self.engine = engine
        self.creds = creds

    def read_db_creds(self, creds_filepath):
        with open(creds_filepath) as f:
            self.creds = yaml.load(f, Loader=SafeLoader)
            return self.creds

    def init_db_engine(self):
        self.engine = create_engine(f"postgresql://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}"
        f"@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}")
        return self.engine.connect()
    
    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exsists = ' replace')


if __name__ == '__main__':
    connect = DatabaseConnector()
    creds = connect.read_db_creds('db_creds.yaml')
    connect.init_db_engine()