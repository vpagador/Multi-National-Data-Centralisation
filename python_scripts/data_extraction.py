from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import inspect
import tabula
import requests
import json
import boto3

class DataExtractor:
    
    def __init__(self, db_connector = None):
        if db_connector is not None:
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
            store = requests.get(f'{store_endpoint}{number}', headers=header_dict).content
            list_of_stores.append(json.loads(store))
            stores_df = pd.DataFrame(list_of_stores)
        print(len(list_of_stores))
        return stores_df
    
    def extract_from_s3(self,address ='s3://data-handling-public/products.csv',filename='products.csv'):
        def format_s3_address():
            s3 = boto3.client(service_name ='s3')
            s3.download_file(BUCKET_NAME,OBJECT_NAME,filename)
        
        def read_file_type():
            if filename[-3:] == 'csv':
                df = pd.read_csv(filename)
            elif filename[-4:] == 'json':
                df = pd.read_json(filename)
            return df
        try:
            BUCKET_NAME, OBJECT_NAME = address.split('/')[-2], address.split('/')[-1]
            format_s3_address()
            df = read_file_type()
            return df
        except:
            pass
        try:
            BUCKET_NAME, OBJECT_NAME = address.split('.s3')[-2].split('/')[2], address.split('/')[-1]
            format_s3_address()
            df = read_file_type()
            return df
        except:
            pass
        
    

if __name__ == '__main__':
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    data_extractor.list_number_of_stores()
    data_extractor.retrieve_stores_data()