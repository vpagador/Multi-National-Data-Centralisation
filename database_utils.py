import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine    


class DatabaseConnector:

    def read_db_creds(self, creds_filepath):
        with open(creds_filepath) as f:
            creds = yaml.load(f, Loader=SafeLoader)
            format_creds = (f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}"
            f"@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
            print(format_creds)
            return format_creds

    def init_db_engine(self, format_creds):
        self.engine = create_engine(format_creds)
        return self.engine.connect()
    
    def upload_to_db(self, df, table_name, my_db_creds ='my_db_creds.yaml'):
        my_creds = self.read_db_creds(my_db_creds)
        self.init_db_engine(my_creds)
        df.to_sql(table_name, self.engine, if_exists = 'replace')
        print('uploaded')


if __name__ == '__main__':
    connect = DatabaseConnector()
    creds = connect.read_db_creds('db_creds.yaml')
    connect.init_db_engine(creds)
    