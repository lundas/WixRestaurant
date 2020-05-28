#!/usr/bin/env python3
import numpy as np
import pandas as pd

class Data_Reformat():

    def delivery_date(self, date):
        '''Takes date and determines the associated delivery date
        Takes into account the addition of Tuesday deliveries on 4/21/2020
        Designed to be used with the df[created] column'''

        # cut off for first tuesday delivery
        first_tues_dtc = pd.Timestamp(tz='US/Pacific', year=2020, month=4, day=17, hour=18)
        if date <= first_tues_dtc:
            if date.weekday() == 0: # If created day on Mon -> following Wed
                del_date = date + pd.Timedelta(days=2)
            elif date.weekday() in [1,2,3] and date.hour >= 18: # TWTh after 6pm -> 2 days later
                del_date = date + pd.Timedelta(days=2)
            elif date.weekday() == 4 and date.hour >= 18: # F after 6pm -> following Wed
                del_date = date + pd.Timedelta(days=5)
            elif date.weekday() in [5,6]: # SSu -> following Wed
                del_date = date + pd.Timedelta(days=(9-date.weekday()))
            else: # otherwise next day
                del_date = date + pd.Timedelta(days=1)
        else:
            if date.weekday() in [0,1,2,3] and date.hour >= 18: # MTWTh after 6pm -> 2 days later
                del_date = date + pd.Timedelta(days=2)
            elif date.weekday() == 4 and date.hour >= 18: # F after 6pm -> following Tues
                del_date = date + pd.Timedelta(days=4)
            elif date.weekday() in [5,6]: # SSu -> following Tues
                del_date = date + pd.Timedelta(days=(8-date.weekday()))
            else: # otherwise next day
                del_date = date + pd.Timedelta(days=1)
        
        return del_date

    def format_df(self, df):
        ''' Takes df produced from format_orders_api_call() and formats it
        in prep for import into Google Sheets
        '''

        # Drop irrelevant columns
        cols = 'externalIds address timeGuarantee distributorId restaurantId locale orderItems currency delivery contact payments received modified user developer source platform log gatewayReturnUrl orderCharges properties'.split()
        df = df.drop(columns=cols)

        # Change created and time to datetime – column in microseconds
        # Make tz aware and convert tz from UTC to 'US/Pacific'
        df['created'] = pd.to_datetime(df['created'], unit='ms', utc=True).dt.tz_convert('US/Pacific')
        df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.tz_convert('US/Pacific')

        # rename time, type, and charge columns with delivery prefix
        cols = {'time':'delivery.time', 
                'type':'delivery.type', 
                'charge':'delivery.charge'}
        df = df.rename(columns=cols)

        # Fill nans for item cols with 0
        first_item_col = list(df.columns).index('address.comment')+1
        for i in df.columns[first_item_col:]:
            df[i] = df[i].fillna(0)

        # Fill nans in delivery charge and tip with 0
        df['delivery.charge'] = df['delivery.charge'].fillna(0)
        df['tip'] = df['tip'].fillna(0)

        # Divide monetary columns by 100 to convert to $USD
        for i in ['price', 'delivery.charge', 'tip']:
            df[i] = df[i]/100

        # Drop rows with status == canceled
        df = df[df['status'] != 'canceled']

        # Drop item columns where sum == 0
        for i in df.columns[first_item_col:]:
            if df[i].sum() == 0:
                df.drop(columns=[i], inplace=True)

        # Add execution date col
        df['Execution Date'] = df.apply(lambda x: self.delivery_date(x['created']).date() if x['delivery.type'] == 'delivery' else x['created'].date(), axis=1)

        return df

    def order_processing(self, df):
        '''Takes formatted df and produces df for order processing tab'''

        # Drop rows that aren't delivery
        df = df[df['delivery.type'] == 'delivery'].copy(deep=True)
        # Rename execution date column as delivery date
        cols = {'Execution Date':'Delivery Date'}
        df = df.rename(columns=cols)

        # Get orders with delivery date == today
        today_date = pd.Timestamp.today(tz='US/Pacific').date()
        df = df[df['Delivery Date'] == today_date]

        # Drop item columns in orders_df where sum == 0
        first_item_col = list(df.columns).index('address.comment')+1
        for i in df.columns[first_item_col:-1]:
            if df[i].sum() == 0:
                df.drop(columns=[i], inplace=True)

        return df

    def workwave(self, df):
        '''Takes df and formats it for import into workwave
        Intended for use with df preprocessed with order_processing()
        '''

        # Create copy of df to avoid chain indexing
        # Unsure if this is necessary but better safe than sorry
        df = df.copy(deep=True)

        # Create numItems col
        df['numItems'] = df[df.columns[list(df.columns).index('address.comment')+1:-1]].apply(sum, axis=1)
        # Create name column
        df['name'] = df.apply(lambda x: '{} {}'.format(x['firstName'], x['lastName']), axis=1)
        # Drop irrelevant columns
        cols = 'name phone email address.formatted address.entrance address.floor address.apt address.comment address.onArrival numItems'.split()
        df = df[cols]

        # Create needed columns
        today_date = pd.Timestamp.today(tz='US/Pacific').date()
        if today_date.weekday() == 5:
            df['startTime'] = pd.Timestamp.today('US/Pacific').replace(hour=12, minute=0, second=0, microsecond=0).time()
        else:
            df['startTime'] = pd.Timestamp.today('US/Pacific').replace(hour=14, minute=0, second=0, microsecond=0).time()
        # endTime
        df['endTime'] = df['startTime'].apply(lambda x: x.replace(x.hour+4))
        # serviceTime
        df['serviceTime'] = 10

        return df










