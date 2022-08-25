"""
Template Component main class.

"""
import csv
import logging
from datetime import date, timedelta
import pandas as pd
import requests
import msal
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_CLIENT_ID = '#client_id'
KEY_PASSWORD = '#password'
KEY_USERNAME = '#username'
KEY_INCREMENTAL = 'incremental'
KEY_AUTHORITY_URL = 'authority_url'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_PASSWORD, KEY_USERNAME, KEY_INCREMENTAL]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.access_token = None
        # self.get_api_token()
        self.incremental = self.get_incremental()
        self.activityDate = date.today() - timedelta(days=1)
        self.activityDate = self.activityDate.strftime("%Y-%m-%d")

    def get_incremental(self):
        params = self.configuration.parameters
        return params.get(KEY_INCREMENTAL)

    def get_api_token(self):
        params = self.configuration.parameters

        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_PASSWORD)
        authority_url = params.get(KEY_AUTHORITY_URL)
        scope = ["https://analysis.windows.net/powerbi/api/.default"]

        # Use MSAL to grab token
        app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
        result = app.acquire_token_for_client(scopes=scope)

        self.access_token = result['access_token']

    def run(self):
        """
        Main execution code
        """
        key = [
            'Id',
            'RecordType',
            'CreationTime',
            'Operation',
            'OrganizationId',
            'UserType',
            'UserKey',
            'Workload',
            'UserId',
            'ClientIP',
            'UserAgent',
            'Activity',
            'ItemName',
            'WorkSpaceName',
            'DatasetName',
            'ReportName',
            'WorkspaceId',
            'ObjectId',
            'DatasetId',
            'ReportId',
            'DataConnectivityMode',
            'IsSuccess',
            'ReportType',
            'RequestId',
            'ActivityId',
            'DistributionMethod',
            'ConsumptionMethod',
            'DashboardName',
            'DashboardId',
            'Datasets',
            'ModelsSnapshots',
            'IsTenantAdminApi',
            'GatewayClusters',
            'LastRefreshTime',
            'ImportId',
            'ImportSource',
            'ImportType',
            'ImportDisplayName',
        ]

        table = self.create_out_table_definition('pbi_event_logs.csv', incremental=self.incremental,
                                                 columns=key, primary_key=['Id'])

        out_table_path = table.full_path
        logging.info(out_table_path)

        url = 'https://api.powerbi.com/v1.0/myorg/admin/activityevents'
        parameters = {
            "startDateTime": f"'{self.activityDate}T00:00:00'",
            "endDateTime": f"'{self.activityDate}T23:59:59'",
        }

        headers = {
            f"Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvYTczZjhiODYtZDBiNS00ZmQ0LTllYzctNGE0ZTkyOGM3NjQ4LyIsImlhdCI6MTY2MTQwODY3NCwibmJmIjoxNjYxNDA4Njc0LCJleHAiOjE2NjE0MTMyMjgsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVFFBeS84VEFBQUFTMDV6T3JDMi9ucTN3bGJqU0ZsNjIyZkczWnM5QkZnTWU0VDJhY0xiSUpaYXVHc0NnUjg0SEZJZmc4dmlhdHRVIiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6IjE4ZmJjYTE2LTIyMjQtNDVmNi04NWIwLWY3YmYyYjM5YjNmMyIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiQURNSU4iLCJnaXZlbl9uYW1lIjoiQkkiLCJpcGFkZHIiOiIxODUuMTguMzAuMTYxIiwibmFtZSI6IkJJIEFETUlOIiwib2lkIjoiNzMyMDRlMzAtYzM5MS00YTViLTgyN2ItODEwMGQ1MDYwZmNlIiwicHVpZCI6IjEwMDMwMDAwQTIyOTg4NTYiLCJyaCI6IjAuQVJFQWhvc19wN1hRMUUtZXgwcE9rb3gyU0FrQUFBQUFBQUFBd0FBQUFBQUFBQUFSQUd3LiIsInNjcCI6IkFwcC5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkV3JpdGUuQWxsIENvbnRlbnQuQ3JlYXRlIERhc2hib2FyZC5SZWFkLkFsbCBEYXNoYm9hcmQuUmVhZFdyaXRlLkFsbCBEYXRhZmxvdy5SZWFkLkFsbCBEYXRhZmxvdy5SZWFkV3JpdGUuQWxsIERhdGFzZXQuUmVhZC5BbGwgRGF0YXNldC5SZWFkV3JpdGUuQWxsIEdhdGV3YXkuUmVhZC5BbGwgR2F0ZXdheS5SZWFkV3JpdGUuQWxsIFBpcGVsaW5lLkRlcGxveSBQaXBlbGluZS5SZWFkLkFsbCBQaXBlbGluZS5SZWFkV3JpdGUuQWxsIFJlcG9ydC5SZWFkLkFsbCBSZXBvcnQuUmVhZFdyaXRlLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkV3JpdGUuQWxsIFRlbmFudC5SZWFkLkFsbCBUZW5hbnQuUmVhZFdyaXRlLkFsbCBVc2VyU3RhdGUuUmVhZFdyaXRlLkFsbCBXb3Jrc3BhY2UuUmVhZC5BbGwgV29ya3NwYWNlLlJlYWRXcml0ZS5BbGwiLCJzaWduaW5fc3RhdGUiOlsia21zaSJdLCJzdWIiOiJFWkE2aDNySlM3SlRybnh2Z0Izb09qcFAwR09pS2lhT3pvMnRTc3pjN1V3IiwidGlkIjoiYTczZjhiODYtZDBiNS00ZmQ0LTllYzctNGE0ZTkyOGM3NjQ4IiwidW5pcXVlX25hbWUiOiJiaS1hZG1pbkBnb3BheS5jeiIsInVwbiI6ImJpLWFkbWluQGdvcGF5LmN6IiwidXRpIjoiR2dlSUV1NjY0VTJ0YlJuTVpiNVNBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiNjJlOTAzOTQtNjlmNS00MjM3LTkxOTAtMDEyMTc3MTQ1ZTEwIiwiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il19.oZdcw2oC494AFPBSFwDM7gFA5nFz1tUp_A8Ji2TrZLDqvzunBie_cvVPXn7g-8Ad6qn_nXxHsSrUqj5OGDhncG6HWtbZgrJ_kO5yrTEJpklnr1r7C7VLqa-n0Yb3212eMRm4xFxLNaouVjIcURHIiGKIjWxQI1iPLeYMBABERb8IK2M0MJ58qA1gKVXcAHQPswACCb4qjvaFXH_yeFrCJRGEPzUcF03lmmlpUkUG5JNzvUVmVr6tR_OaMK-keqdw4PQwy7WW1eNbNsgYxGIG3MDSjvx9b3dKjBYoltifS4TVdOrpfiDvd9WcmvbAXrZPZvaO1Iz-mpcsdQ3HvFDodg"
        }

        df = pd.DataFrame(columns=key)

        api_call = requests.get(url=url, params=parameters, headers=headers)
        api_call.raise_for_status()

        # Set continuation URL
        contUrl = api_call.json()['continuationUri']

        # Get all Activities for first hour, save to dataframe (df1) and append to empty created df
        result = api_call.json()['activityEventEntities']
        df1 = pd.DataFrame(result)
        if not df1.empty:
            df1 = df1[df1.Activity != 'ExportActivityEvents']
        pd.concat([df, df1])

        # Call Continuation URL as long as results get one back to get all activities through the day
        while contUrl is not None:
            api_call_cont = requests.get(url=contUrl, headers=headers)
            contUrl = api_call_cont.json()['continuationUri']
            result = api_call_cont.json()['activityEventEntities']
            df2 = pd.DataFrame(result)
            if not df2.empty:
                df2 = df2[df2.Activity != 'ExportActivityEvents']
            df = pd.concat([df, df2])

        # Set ID as Index of df
        df = df.set_index('Id')

        # Save df as CSV
        df.to_csv(out_table_path)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
