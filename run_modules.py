from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import numpy as np

db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)
'''table_list = data_extractor.list_db_tables()
data_cleaner = DataCleaning(data_extractor, table_list)
clean_data = data_cleaner.clean_user_data()
db_connector.upload_to_db(df=clean_data, table_name='dim_users')'''
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
card_data = data_extractor.retrieve_pdf_data(link)
card_data = np.array(card_data,dtype=object)
card_data = pd.DataFrame(card_data).style.hide(axis='index')
card_data.to_string('card_data.txt')
