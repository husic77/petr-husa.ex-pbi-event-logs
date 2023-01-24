"""
Template Component main class.

"""
import logging
from datetime import date, timedelta

import pandas as pd
import requests
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

        url = "https://login.microsoftonline.com/common/oauth2/token"
        body = {
            "Content-Type": "application/x-www-form-urlencoded",
            "client_id": params.get(KEY_CLIENT_ID),
            "scope": "openid",
            "resource": "https://analysis.windows.net/powerbi/api",
            "grant_type": "password",
            "password": params.get(KEY_PASSWORD),
            "username": params.get(KEY_USERNAME)
        }

        response = requests.post(url, data=body).json()
        self.access_token = response['access_token']
        # print(self.access_token)

    def run(self):
        """
        Main execution code
        """
        key = [
            'Id',
            'CreationTime',
            'Operation',
            'UserKey',
            'Workload',
            'UserId',
            'ClientIP',
            'Activity',
            'ItemName',
            'WorkSpaceName',
            'DatasetName',
            'ReportName',
            'WorkspaceId',
            'ObjectId',
            'DatasetId',
            'ReportId',
            'ArtifactId',
            'ArtifactName',
            'ArtifactKind',
            'IsSuccess',
            'ActivityId',
            'DistributionMethod',
            'ConsumptionMethod',
        ]

        table = self.create_out_table_definition('pbi_event_logs.csv', incremental=self.incremental,
                                                 columns=key, primary_key=['Id'])

        out_table_path = table.full_path
        logging.info(out_table_path)
        df = pd.DataFrame(columns=key, dtype='string')
        df.to_csv(out_table_path, mode='w', header=True, index=False)

        for x in range(1, 8):
            self.activityDate = date.today() - timedelta(days=x)
            self.activityDate = self.activityDate.strftime("%Y-%m-%d")
            # self.activityDate = '2023-01-17'
            url = 'https://api.powerbi.com/v1.0/myorg/admin/activityevents'
            parameters = {
                "startDateTime": f"'{self.activityDate}T00:00:00'",
                "endDateTime": f"'{self.activityDate}T23:59:59'",
            }

            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }

            api_call = requests.get(url=url, params=parameters, headers=headers)
            api_call.raise_for_status()

            # Set continuation URL
            cont_url = api_call.json()['continuationUri']

            # Get all Activities for first hour, save to dataframe (df1) and append to empty created df
            result = api_call.json()['activityEventEntities']
            df1 = pd.DataFrame(result, dtype='string')
            if not df1.empty:
                df1 = df1[df1.Activity != 'ExportActivityEvents']
            pd.concat([df, df1])

            # Call Continuation URL as long as results get one back to get all activities through the day
            while cont_url is not None:
                api_call_cont = requests.get(url=cont_url, headers=headers)
                cont_url = api_call_cont.json()['continuationUri']
                result = api_call_cont.json()['activityEventEntities']
                df2 = pd.DataFrame(result, dtype='string')
                if not df2.empty:
                    df2 = df2[df2.Activity != 'ExportActivityEvents']
                df = pd.concat([df, df2])

            final_data = df[key].copy()
            final_data = final_data.astype(dtype="string")
            # Set ID as Index of df
            final_data = final_data.set_index('Id')

            # Save df as CSV
            final_data.to_csv(out_table_path, mode='a', header=False)


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
