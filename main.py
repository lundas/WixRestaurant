#!/usr/bin/env python3
from src import api_calls
from src import google_API
from src import data_reformat
import yaml

# Initialize classes
api = api_calls.Wix_Restaurant_API()
data_reformat = data_reformat.Data_Reformat()
gAPI = google_API.GoogleAPI()

# Config
conf_file = '/PATH/TO/CONFIG.yaml'
with open(conf_file, 'r') as stream:
    config = yaml.safe_load(stream)

# print(type(config))

# Parameters
restaurant_id = config['rest_id']
headers = config['headers'][0]
spreadsheetID = config['ss_id']
PATH = config['PATH']

json = api.orders_api_call(rest_id=restaurant_id, headers=headers)
items_df = api.menu_api_call(rest_id=restaurant_id, headers=headers)
df = api.format_orders_api_call(json, items_df)

data = data_reformat.format_df(df)
data['Execution Date'] = data_reformat.oakland_delivery_dates(data)
orders = data_reformat.order_processing(data)
workwave = data_reformat.workwave(orders)

data.to_csv('./data.csv')
orders.to_csv('./orders.csv') 
workwave.to_csv('./workwave.csv')
items_df.to_csv('./menu_items.csv')

gAPI.import_data(PATH=PATH, filename='data.csv', sheet_name='data', 
            spreadsheet_id=spreadsheetID)
gAPI.import_data(PATH=PATH, filename='orders.csv', sheet_name='Order Processing', 
            spreadsheet_id=spreadsheetID)
gAPI.import_data(PATH=PATH, filename='menu_items.csv', sheet_name='Menu Items', 
            spreadsheet_id=spreadsheetID)
gAPI.import_data(PATH=PATH, filename='workwave.csv', sheet_name='Workwave', 
            spreadsheet_id=spreadsheetID)