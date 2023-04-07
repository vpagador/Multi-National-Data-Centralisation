from data_extraction import DataExtractor
from data_utils import DataConnector
from data_cleaning import DataCleaning

data_cleaner = DataCleaning()
clean_data = data_cleaner.clean_user_data()
clean_data.to_string('clean_data.csv')

