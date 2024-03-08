import yaml
from yaml.loader import SafeLoader
import psycopg2
from sqlalchemy import create_engine    


class DatabaseConnector:

    def read_db_creds(self, creds_filepath):
        with open(creds_filepath) as f:
            creds = yaml.load(f, Loader=SafeLoader)
            format_creds = (f"postgresql://{creds['USER']}:{creds['PASSWORD']}"
            f"@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
            return format_creds

    def init_db_engine(self, format_creds):
        engine = create_engine(format_creds)
        return engine.connect()
    
    def upload_to_db(self, df, table_name, my_db_creds='creds/my_db_creds.yaml'):
        my_creds = self.read_db_creds(my_db_creds)
        engine = self.init_db_engine(my_creds)
        df.to_sql(table_name, engine, if_exists = 'replace')
        print('uploaded')


if __name__ == '__main__':
    database_connector = DatabaseConnector()
    creds_to_read = database_connector.read_db_creds('python_scripts/db_creds.yaml')
    database_connector.init_db_engine(creds_to_read)
    