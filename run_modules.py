from python_scripts.data_extraction import DataExtractor
from python_scripts.database_utils import DatabaseConnector
from python_scripts.data_cleaning import DataCleaning
import pandas as pd

db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()

# load user data
table_list = data_extractor.list_db_tables()
table = table_list[1]
user_df = data_extractor.read_rds_table(table)
# clean user_data
clean_user_df = data_cleaner.clean_user_data(user_df)
db_connector.upload_to_db(df=clean_user_df, table_name='dim_users')

# load credit card data
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
credit_card_df = data_extractor.retrieve_pdf_data(link)
# clean card data
credit_card_df = data_cleaner.clean_card_data(credit_card_df)
credit_card_df.to_string('clean_credit_card_data.txt')
# Send to data_base
db_connector.upload_to_db(credit_card_df,table_name='dim_card_details')

# print store data
stores_df = data_extractor.retrieve_stores_data()
stores_df = pd.read_csv('unclean_stores_data.csv')
# clean store data and upload to db
clean_stores_df = data_cleaner.clean_store_data(stores_df)
# Send to db
db_connector.upload_to_db(clean_stores_df,table_name='dim_store_details')

# load products data
products_df = data_extractor.extract_from_s3()
# clean weights column and products data
clean_weights_data = data_cleaner.convert_product_weights(products_df)
clean_products_data = data_cleaner.clean_products_data(clean_weights_data)
# send to db
db_connector.upload_to_db(clean_products_data,table_name='dim_products')

# load orders data
list_ = data_extractor.list_db_tables()
orders_df = data_extractor.read_rds_table(list_[2])
# clean orders_data
clean_orders_df = data_cleaner.clean_orders_data(orders_df)
# send to db
db_connector.upload_to_db(clean_orders_df,table_name='orders_table')

# load events data
events_df = data_extractor.extract_from_s3(address='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json',
                                   filename='date_events.json')
# clean events data
clean_events_data = data_cleaner.clean_events_data(events_df)
# send to db
db_connector.upload_to_db(clean_events_data,table_name='dim_date_times')