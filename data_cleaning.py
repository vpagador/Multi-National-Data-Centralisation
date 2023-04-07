from data_extraction import DataExtractor
import pandas as pd


class DataCleaning:
    
    def __init__(self):
        self.data_extractor = DataExtractor()
        self.table_list = self.data_extractor.list_db_tables()

    def clean_user_data(self):
        users_table = self.table_list[1]
        users_df = self.data_extractor.read_rds_table(users_table)
        users_df = users_df[users_df.first_name != 'NULL']    # Get rid of NULL values

        users_df = users_df.loc[users_df['country_code'].isin(['GB','US','DE'])]  # Get rid of rows with wrong values 

        date_cols = ['date_of_birth','join_date']
        for date_col in date_cols:
            users_df.loc[:,date_col] = users_df.loc[:,date_col].apply(pd.to_datetime, # Fix Dates
                                            infer_datetime_format=True, 
                                            errors='coerce')
        
        users_df.loc[:,'address'] = users_df.loc[:,'address'].apply(lambda x:x.replace('\n', ','))  # Get rid of \n
        return users_df



if __name__=='__main__':
    data_cleaner = DataCleaning()
    clean_data = data_cleaner.clean_user_data()
    clean_data.to_string('clean_data.csv')