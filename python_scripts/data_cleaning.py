from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import re


class DataCleaning:

    def clean_user_data(self,users_df):
        # Get rid of duplicates
        users_df = users_df.drop_duplicates()
        # Get rid of NULL values
        users_df = users_df[users_df.first_name != 'NULL']    
        users_df = users_df.dropna()
        # Get rid of rows with wrong values
        users_df.loc[:,'country_code'] = users_df.loc[:,'country_code'].astype('string').apply(lambda x : x.replace('GGB','GB'))
        users_df = users_df.loc[users_df['country_code'].isin(['GB','US','DE'])]   
        # Fix date formatting
        date_cols = ['date_of_birth','join_date']
        for date_col in date_cols:
            users_df.loc[:,date_col] = users_df.loc[:,date_col].apply(pd.to_datetime, 
                                            infer_datetime_format=True, 
                                            errors='coerce')
        # Remove escape characters
        users_df.loc[:,'address'] = users_df.loc[:,'address'].apply(lambda x:x.replace('\n', ','))
        return users_df
    
    def clean_card_data(self, credit_card_df):
        # Reset indexing
        index = [row for row in range(0,len(credit_card_df))] 
        credit_card_df['index'] = index                         # new column
        credit_card_df = credit_card_df.set_index(['index'])    # replace index
        # Get rid of duplicates
        credit_card_df = credit_card_df.drop_duplicates()
        # Drop NULL values
        credit_card_df = credit_card_df[credit_card_df.card_number != 'NULL']
        credit_card_df = credit_card_df.dropna()
        # Infer datatypes
        credit_card_df['card_number'] = credit_card_df['card_number'].astype('str')     
        credit_card_df['card_provider'] = credit_card_df['card_provider'].astype('str')
        # Drop wrong values
        credit_card_df.loc[:,'card_number'] = credit_card_df.loc[:,'card_number'].astype('str').apply(lambda x:x.replace('?',''))
        credit_card_df = credit_card_df[credit_card_df['card_number'].str.isnumeric()]                                                                                       ###
        # Fix date formatting
        credit_card_df.loc[:,'expiry_date'] = \
        credit_card_df.loc[:,'expiry_date'].apply(pd.to_datetime, format='%m/%y')
        credit_card_df.loc[:,'date_payment_confirmed'] = \
        credit_card_df.loc[:,'date_payment_confirmed'].apply(pd.to_datetime,
                                                                infer_datetime_format=True,
                                                                errors='coerce')
        return credit_card_df

    def clean_store_data(self,stores_df):
        # Delete duplicate indexer column 
        stores_df = stores_df.iloc[:,1:]
        # Get rid of duplicates
        stores_df = stores_df.drop_duplicates()
         # Get rid of rows with wrong values
        stores_df = stores_df.loc[stores_df['country_code'].isin(['GB','US','DE'])]
        # Remove escape characters
        stores_df.loc[:,'address'] =  stores_df.loc[:,'address'].astype('str').apply(lambda x:x.replace('\n', ','))
        # Fix values mixing letters with numbers
        stores_df.loc[:,'staff_numbers'] = stores_df['staff_numbers'].astype('str').apply(lambda x : re.sub('\D','',x))
        # Fix date formatting
        stores_df[['opening_date']] = \
        stores_df[['opening_date']].apply(pd.to_datetime,
                                            infer_datetime_format=True,
                                            errors='coerce')
        # Fix continent misspellings
        stores_df[['continent']] = stores_df[['continent']] \
                                    .apply(lambda x:x.replace('eeEurope','Europe')) \
                                    .apply(lambda x:x.replace('eeAmerica','America'))
        return  stores_df
    
    def convert_product_weights(self, products_df):
        # ensure correct format e.g. '0.77g .'
        def correct_format(value):
            if value[-1].isalnum() is False:
                wrong_char = value[-1]
                value= value.replace(wrong_char,'').strip()
            return value
        products_df.loc[:,'weight'] =products_df.loc[:,'weight'].astype('str').apply(lambda x:correct_format(x))
        # convert oz
        def convert_oz(value):
            if 'oz' in value:
                value = value.replace('oz','')
                value = float(value) * 28.3495
            return value
        products_df.loc[:,'weight'] = products_df.loc[:,'weight'].astype('str').apply(lambda x:convert_oz(x))
        # convert grams to kg
        def convert_grams(value):
            if value[-1] == 'g' and value[-2].isdigit() and value[:-2].isdigit()or value[-2:] == 'ml':
                value = value.replace('g','').replace('ml','')
                value = int(value) /1000
            return value
        products_df.loc[:,'weight'] = products_df.loc[:,'weight'].astype('str').apply(lambda x:convert_grams(x))
        # remove kg sign
        products_df.loc[:,'weight'] = products_df.loc[:,'weight'].astype('str').apply(lambda x:re.sub('[kg]','',x))
        # multiply values if contains 'num x num'
        def multiply_weight_values(value):
            if 'x' in value:
                value = value.replace(' x ',' ')
                num1, num2 = value.split(' ')[0], value.split(' ')[1]
                new_value = int(num1)*int(num2) / 1000
                return new_value
            else:
                return value
        products_df.loc[:,'weight'] = products_df.loc[:,'weight'].apply(lambda x: multiply_weight_values(x)) 
        # drop non numerical values with weight is digit
        products_df.loc[:,'weight'] = products_df[products_df.loc[:,'weight'].astype('str').apply(lambda x:x.replace('.','').isdigit())] # works
        # convert all weight values to float and round up 2.d.p
        products_df.loc[:,'weight'] = products_df.loc[:,'weight'].astype('float').apply(lambda x: round(x,2))
        return products_df

    
    def clean_products_data(self,products_df):
        # drop one of the indexing columns and reindex 
        products_df = products_df.drop(columns='Unnamed: 0').reindex()
        # Drop NULL values
        products_df = products_df[products_df.weight != 'nan']
        products_df = products_df.dropna()
        # Drop duplicates
        products_df = products_df.drop_duplicates()
        # Format dates
        products_df.loc[:,'date_added'] = (
        products_df['date_added'].apply(pd.to_datetime, 
                                        infer_datetime_format=True, 
                                        errors='coerce'))
        return products_df

    def clean_orders_data(self,orders_df):
        orders_df = orders_df.drop(columns=['first_name','last_name','1','level_0','index']).reindex()
        return orders_df
    
    def clean_events_data(self,events_df):
        # drop duplicates
        events_df = events_df.drop_duplicates()
        # drop null and wrong values with month is digit
        events_df = events_df[events_df.loc[:,'month'].astype('str').apply(lambda x:x.isdigit())]
        return events_df
    

if __name__=='__main__':
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    table_list = data_extractor.list_db_tables()
    table = table_list[1]
    data_cleaner = DataCleaning()
