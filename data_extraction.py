from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import inspect

class DataExtractor:
    
    def __init__(self):
        connector = DatabaseConnector()
        connector.read_db_creds('db_creds.yaml')
        self.run_engine = connector.init_db_engine()

    def list_db_tables(self):
        inspector = inspect(self.run_engine)
        table_list = inspector.get_table_names()
        return table_list
    
    def read_rds_table(self, table):
        df = pd.read_sql_table(table,self.run_engine)
        return df

    def print_all_tables(self, table_names):
        for table in table_names:  
            df = pd.read_sql_table(table, self.run_engine)
            print(df.head(5))
        

if __name__ == '__main__':
    data_extractor = DataExtractor()
    table_list = data_extractor.list_db_tables()
    user_table = table_list[1]
    df = data_extractor.read_rds_table(user_table)
    print(df)