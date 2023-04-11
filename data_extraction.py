from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import inspect
import tabula

class DataExtractor:
    
    def __init__(self, db_connector):
        creds = db_connector.read_db_creds('db_creds.yaml')
        self.run_engine = db_connector.init_db_engine(creds)

    def list_db_tables(self):
        inspector = inspect(self.run_engine)
        table_list = inspector.get_table_names()
        return table_list
    
    def read_rds_table(self, table):
        df = pd.read_sql_table(table,self.run_engine)
        return df

    def retrieve_pdf_data(self,link):
        df = tabula.read_pdf(link,pages='all')
        return df
        

if __name__ == '__main__':
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    table_list = data_extractor.list_db_tables()
    user_table = table_list[1]
    df = data_extractor.read_rds_table(user_table)
    print(df)