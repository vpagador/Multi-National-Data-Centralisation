from python_scripts.database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import text, inspect
import tabula
import requests
import json
import boto3
import numpy as np

class DataExtractor:

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        table_list = inspector.get_table_names()
        return table_list
    
    def read_rds_table(self, engine,table):
        try:
            query = f"SELECT * from {table};"
            with engine as con:
                df = pd.read_sql_query(query, con=con)
            return df

        except Exception as err:
            print(f'Failed to read data')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    def retrieve_pdf_data(self,link):
        credit_card_df = tabula.read_pdf(link,pages='all')
        credit_card_df = pd.concat(credit_card_df)
        return credit_card_df
    
    def list_number_of_stores(self,number_of_stores_endpoint=
                          'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
                              header_dict={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):
        number_of_stores = requests.get(number_of_stores_endpoint, headers=header_dict).content
        number_of_stores = json.loads(number_of_stores) # returns dictionary
        return number_of_stores['number_stores']

    def retrieve_stores_data(self,store_endpoint=
                             'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/',
                             header_dict={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):
        list_of_stores = []
        store_number = self.list_number_of_stores()
        for number in range(0,store_number):
            store = requests.get(f'{store_endpoint}{number}', headers=header_dict).json()
            print(f"Getting: {number}")
            list_of_stores.append(pd.DataFrame(store,index=[np.NaN]))
        stores_df = pd.concat(list_of_stores)
        return stores_df
    
    def _format_s3_address(self,BUCKET_NAME,OBJECT_NAME,filename):
            s3 = boto3.client(service_name ='s3')
            s3.download_file(BUCKET_NAME,OBJECT_NAME,filename)
        
    def _read_file_type(self,filename):
        if filename[-3:] == 'csv':
            df = pd.read_csv(filename)
        elif filename[-4:] == 'json':
            df = pd.read_json(filename)
        return df

    def extract_from_s3(self, s3_address, filename):
        try:
            BUCKET_NAME, OBJECT_NAME = s3_address.split('/')[-2], s3_address.split('/')[-1]
        except:
            pass
        try:
            BUCKET_NAME, OBJECT_NAME = s3_address.split('.s3')[-2].split('/')[2], s3_address.split('/')[-1]
        except:
            pass
        self._format_s3_address(BUCKET_NAME,OBJECT_NAME,filename)
        df = self._read_file_type(filename)
        return df

if __name__ == '__main__':
    database_connector = DatabaseConnector()
    creds_to_read = database_connector.read_db_creds('python_scripts/db_creds.yaml')
    engine = database_connector.init_db_engine(creds_to_read)
    data_extractor = DataExtractor()
    data_extractor.list_db_tables(engine)