from python_scripts.data_extraction import DataExtractor
from python_scripts.database_utils import DatabaseConnector
from python_scripts.data_cleaning import DataCleaning
import pandas as pd

db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()

creds_to_read = db_connector.read_db_creds('creds/db_creds.yaml')
engine = db_connector.init_db_engine(creds_to_read)

'''# load user data
table_list = data_extractor.list_db_tables(engine)
table = table_list[1]
user_df = data_extractor.read_rds_table(engine,table)
user_df.to_csv('notclean_user_data.csv')
# clean user_data
clean_user_df = data_cleaner.clean_user_data(user_df)
clean_user_df.to_csv('clean_user_data.csv')
#db_connector.upload_to_db(df=clean_user_df, table_name='dim_users')

# load credit card data
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
credit_card_df = data_extractor.retrieve_pdf_data(link)
credit_card_df.to_csv('notclean_credit_card_data.csv')
# clean card data
clean_credit_card_df = data_cleaner.clean_card_data(credit_card_df)
clean_credit_card_df.to_csv('clean_credit_card_data.csv')
# Send to data_base
#db_connector.upload_to_db(clean_credit_card_df,table_name='dim_card_details')

# print store data
data_extractor.list_number_of_stores()
stores_df = data_extractor.retrieve_stores_data()
stores_df.to_csv("notclean_stores_data.csv")
# clean store data and upload to db
clean_stores_df = data_cleaner.clean_store_data(stores_df)
clean_stores_df.to_csv("clean_stores_data.csv")
# Send to db
#db_connector.upload_to_db(clean_stores_df,table_name='dim_store_details')

# load products data
products_df = data_extractor.extract_from_s3()
products_df.to_csv("notclean_products_data.csv")
# clean weights column and products data
clean_weights_df = data_cleaner.convert_product_weights(products_df)
clean_products_df = data_cleaner.clean_products_data(clean_weights_df)
clean_products_df.to_csv("clean_products_data.csv")
# send to db
#db_connector.upload_to_db(clean_products_df,table_name='dim_products')'''

# load orders data
list_ = data_extractor.list_db_tables(engine)
orders_df = data_extractor.read_rds_table(engine,list_[2])
orders_df.to_csv('notclean_orders_data.csv')
# clean orders_data
clean_orders_df = data_cleaner.clean_orders_data(orders_df)
clean_orders_df.to_csv('clean_orders_data.csv')
# send to db
#db_connector.upload_to_db(clean_orders_df,table_name='orders_table')

# load events data
events_df = data_extractor.extract_from_s3(address='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json',
                                   filename='date_events.json')
events_df.to_csv('notclean_events_data.csv')
# clean events data
clean_events_df = data_cleaner.clean_events_data(events_df)
clean_events_df.to_csv('clean_events_data.csv')
# send to db
#db_connector.upload_to_db(clean_events_data,table_name='dim_date_times')
