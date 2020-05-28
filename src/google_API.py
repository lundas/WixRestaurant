#!/usr/bin/env python3
import csv
import os
import httplib2
from googleapiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from datetime import datetime, date

class GoogleAPI:
    # argparse only relevant for command line - throws error in jupyter
    try:
        import argparse
        flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
    except ImportError:
        flags = None

    # If modifying these scopes, delete your previous saved credentials at
    # ~/.credentials/sheets.googleapis.com-python-gAPItest.json
    scopes = 'https://www.googleapis.com/auth/spreadsheets'
    #uses Oauth ID rather than service account
    client_secret_file = '/PATH/TO/CREDENTIALS'
    application_Name = 'Allocations'

    def get_credentials(self):
        """get valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid, 
        the Oauth2 flow is completed to obtain new credentials

        Returns:
            Credentials, the obtained credentials.
        """

        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir, 
            'sheets.googleapis.com-python-gAPItest.json')#change for each project for different credentials


        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(self.client_secret_file, self.scopes)
            flow.user_agent = self.application_Name
            if self.flags:
                credentials = tools.run_flow(flow, store, self.flags)
            else:
                tools.run(flow, store)
            print('Storing credentials to ' + credential_path)
        return credentials
    
    def import_data(self, PATH, filename, sheet_name, 
                    spreadsheet_id, cell_range='A1:DZ5000'):
        print("Interacting w/ Google Sheets API")
        # Populates Google Sheets. Dependent on Pandas
        credentials = GoogleAPI().get_credentials()
        http = credentials.authorize(httplib2.Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?', 
            'version=v4')
        service = discovery.build('sheets', 'v4', http=http)
        spreadsheetID = spreadsheet_id #taken from url; change for each project

        # Read csv files to be imported
        file = open(PATH + filename)
        reader = csv.reader(file)
        data = list(reader)

        # Populate Google Sheets with csv data
        # Master Data - sheet is named data
        body = {u'range': u'{}!{}'.format(sheet_name, cell_range), 
                     u'values': data, u'majorDimension': u'ROWS'}
        # clear sheet first to ensure no extra rows from last import
        result = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, 
                                                       range=body['range'], 
                                                       body={}).execute()
        # import csv data to Google Sheet - Master Data
        result = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, 
                                                        range=body['range'],
                                                        valueInputOption='USER_ENTERED', 
                                                        body=body).execute()

        return