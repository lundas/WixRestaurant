#!/usr/bin/env python3
import numpy as np
import pandas as pd
import requests

class Wix_Restaurant_API:
    """class containing functions for making calls to the Wix Restaurant API
       functions return pandas dataframes"""

    def orders_api_call(self, rest_id, headers, payload={'viewMode':'restaurant'}):
        ''' Makes call to wix restaurant orders api
            returns json response
            rest_id == restaurant id, must be obtained through dev tools
            headers == Auth Token
            payload == url params'''
        
        url = 'https://api.wixrestaurants.com/v2/organizations/{}/orders'.format(rest_id)

        # Make requests call
        r = requests.get(url, params=payload, headers=headers)

        return r.json()

    def format_orders_api_call(self, json, items_df):
        ''' Formats data retrieved from orders api call
            takes a r.json() from orders_api_call and items_df from menu_api_call
            returns reformatted dataframe'''

        # Create dataframe from results
        df = pd.DataFrame(json['results'])

        #Clean up data
        # Add seperate columns from delivery column
        for i in df['delivery'].apply(pd.Series).columns:
            df[i] = df['delivery'].apply(pd.Series)[i]

        # Add seperate columns from contact column
        for i in df['contact'].apply(pd.Series).columns:
            df[i] = df['contact'].apply(pd.Series)[i]

        # Add tip column from orderCharges amount
        df['tip'] = df['orderCharges'].apply(lambda x: x[0]['amount'] if type(x) == list else np.nan)
        # Change negative tips to 0 – remnant from coupon which added discount as negative
        # tip for those customers who didn't tip (unclear why)
        df.loc[df['tip']<0, 'tip'] = 0

        # Expand Address dict into relevant columns
        address = df['address'].apply(pd.Series)[['formatted', 'apt', 'entrance',
                                                'floor','onArrival', 'comment']]
        for i in address.columns:
            df['address.{}'.format(i)] = address[i]

        # Add cols to df based on item
        # create mask in prep for adding cols based on item
        mask = pd.io.json.json_normalize(json['results'], 'orderItems', ['id'])
        mask['item'] = mask.apply(lambda x: items_df.loc[x['itemId'], 'title.en_US'], axis=1)
        # group by id, item to account for customers who add the same item multiple times
        grouped = mask.groupby(['id', 'item']).sum()
        # Create col for each item in Dataframe items_df and add counts from mask
        for i in items_df['title.en_US'].unique():
            df[i] = np.nan
        for index, row in grouped.iterrows():
            df.loc[df['id'] == index[0], index[1]] = row['count']

        return df


    def menu_api_call(self, rest_id, headers, payload={'viewMode':'restaurant'}):
        ''' Makes call to wix restaurant menu api
            returns pandas dataframe built from json response
            only returns items, itemIds, and price formatted to USD
            rest_id == restaurant id, must be obtained through dev tools
            headers == Auth Token
            payload == url params'''

        url = 'https://api.wixrestaurants.com/v2/organizations/{}/menu'.format(rest_id)

        # Make requests call
        r = requests.get(url, params=payload, headers=headers)

        # put items, itemIDs, and price into dataframe
        items = pd.io.json.json_normalize(r.json()['items'])[['id', 'title.en_US', 'price']]
        # set index to id
        items = items.set_index('id')
        # Convert price to $USD
        items['price'] = items['price']/100

        sections = {}
        for i in pd.io.json.json_normalize(r.json()['sections'])['children']:
            for j in i:
                if 'itemIds' in j.keys():
                    key=j['title']['en_US']
                    value=j['itemIds']
                    sections[key] = value
                else:
                    pass

        # Create format column, add section name to relevant items
        items['format'] = np.nan
        for i in sections.keys():
            for j in sections[i]:
                items.loc[j, 'format'] = i

        # Fill format nans with archived
        items['format'] = items['format'].fillna('archived')
        # Replace cans with 4-packs
        items.loc[items['format'].str.contains('Cans'), 'format'] = '4-Pack'
        # Deal with single cans
        items.loc[(items['format'].str.contains('4-Pack')) &\
                (items['price'] < 12), 'format'] = 'Single Can'

        return items





