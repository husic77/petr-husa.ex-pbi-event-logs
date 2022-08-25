"""
Template Component main class.

"""
import logging
from datetime import date, timedelta
import pandas as pd
import requests
# import msal
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_CLIENT_ID = '#client_id'
KEY_PASSWORD = '#password'
KEY_USERNAME = '#username'
KEY_INCREMENTAL = 'incremental'
KEY_AUTHORITY_URL = '#authority_url'
KEY_ACCESS_TOKEN = 'access_token'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_PASSWORD, KEY_USERNAME, KEY_INCREMENTAL, KEY_AUTHORITY_URL, KEY_ACCESS_TOKEN]
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
        self.get_api_token()
        self.incremental = self.get_incremental()
        self.activityDate = date.today() - timedelta(days=1)
        self.activityDate = self.activityDate.strftime("%Y-%m-%d")

    def get_incremental(self):
        params = self.configuration.parameters
        return params.get(KEY_INCREMENTAL)

    def get_api_token(self):
        params = self.configuration.parameters

        # client_id = params.get(KEY_CLIENT_ID)
        # client_secret = params.get(KEY_PASSWORD)
        # authority_url = params.get(KEY_AUTHORITY_URL)
        # scope = ["https://analysis.windows.net/powerbi/api/.default"]

        # Use MSAL to grab token
        # app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
        # result = app.acquire_token_for_client(scopes=scope)

        # self.access_token = result['access_token']
        self.access_token = params.get(KEY_ACCESS_TOKEN)

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
            "Authorization": f"Bearer {self.access_token}"
        }

        df = pd.DataFrame(columns=key)

        api_call = requests.get(url=url, params=parameters, headers=headers)
        api_call.raise_for_status()

        # Set continuation URL
        cont_url = api_call.json()['continuationUri']

        # Get all Activities for first hour, save to dataframe (df1) and append to empty created df
        result = api_call.json()['activityEventEntities']
        df1 = pd.DataFrame(result)
        if not df1.empty:
            df1 = df1[df1.Activity != 'ExportActivityEvents']
        pd.concat([df, df1])

        # Call Continuation URL as long as results get one back to get all activities through the day
        while cont_url is not None:
            api_call_cont = requests.get(url=cont_url, headers=headers)
            cont_url = api_call_cont.json()['continuationUri']
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
