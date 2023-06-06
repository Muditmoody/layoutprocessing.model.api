import requests
import pandas as pd
from http import HTTPStatus as req_status
import json
from json import JSONEncoder
import datetime


class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


class DataExtractProcessor:
    """
    A class that handles data extraction from APIs and saving results.

    Methods:
        get_task_codes(): Retrieves task codes from the API.
        get_group_codes(): Retrieves group codes from the API.
        get_engine_programs(): Retrieves engine programs from the API.
        get_data_from_api(request_url, params=None): Retrieves data from the specified API endpoint.
        save_results(request_url, result, params=None): Saves results to the specified API endpoint.
        get_training_data(request_url, params=None): Retrieves training data from the specified API endpoint.
        get_inference_data(request_url, params=None): Retrieves inference data from the specified API endpoint.

    """
    
    def __init__(self):
        """
        Initializes a DataExtractProcessor instance.

        """
        pass
    
    @staticmethod
    def get_task_codes():
        """
        Retrieves task codes from the API.

        Returns:
            pd.DataFrame: A DataFrame containing the task codes.

        """
        
        response = requests.get(
            f'http://localhost:6001/api/etl/TaskCode/GetTaskCodes',
            verify=False)

        data = ""

        if response.status_code == req_status.OK:
            content_type = response.headers['content-type'].split(';')[0]
            if content_type == "application/json":
                data = response.json()

        # print(json.dumps(obj=data, indent=2))
        return pd.DataFrame.from_dict(data).drop(columns=['GroupCodeId', 'TaskCodeText'])

    @staticmethod
    def get_group_codes():
        """
        Retrieves group codes from the API.

        Returns:
            pd.DataFrame: A DataFrame containing the group codes.

        """
        response = requests.get(
            f'http://localhost:6001/api/etl/CodeGroup/GetGroupCodes',
            verify=False)

        data = ""

        if response.status_code == req_status.OK:
            content_type = response.headers['content-type'].split(';')[0]
            if content_type == "application/json":
                data = response.json()

        # print(json.dumps(obj=data, indent=2))
        return pd.DataFrame.from_dict(data)

    @staticmethod
    def get_engine_programs():
        """
        Retrieves engine programs from the API.

        Returns:
            pd.DataFrame: A DataFrame containing the engine programs.

        """
        response = requests.get(
            f'http://localhost:6001/api/etl/EngineProgram/GetEnginePrograms',
            verify=False)

        data = ""

        if response.status_code == req_status.OK:
            content_type = response.headers['content-type'].split(';')[0]
            if content_type == "application/json":
                data = response.json()

        # print(json.dumps(obj=data, indent=2))
        return pd.DataFrame.from_dict(data).drop(columns=['CodingCodeId', 'CodingCodeName'])

    @staticmethod
    def get_data_from_api(request_url: str, params: dict = None):
        """
        Retrieves data from the specified API endpoint.

        Args:
            request_url (str): The URL of the API endpoint.
            params (dict, optional): Additional parameters for the API request. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the retrieved data.

        """
        if len(request_url) > 0:
            try:
                response = requests.get(request_url, params=params, verify=False)
                data = ""

                if response.status_code == req_status.OK:
                    content_type = response.headers['content-type'].split(';')[0]
                    if content_type == "application/json":
                        data = response.json()
                return pd.DataFrame.from_dict(data)
            except RuntimeError as ex:
                print(ex)
        else:
            raise ValueError("Url is invalid or empty")

    @staticmethod
    def save_results(request_url: str, result, params: dict = None):
        """
        Saves results to the specified API endpoint.

        Args:
            request_url (str): The URL of the API endpoint.
            result (pd.DataFrame or dict): The results to be saved. It can be either a DataFrame or a dictionary.
            params (dict, optional): Additional parameters for the API request. Defaults to None.

        """
        if isinstance(result, pd.DataFrame):
            result = json.loads(result.to_json(orient="records"))

        response = requests.post(url=request_url, params=params, json=result, verify=False)

        if response.status_code == req_status.OK:
            print("ok")

    @staticmethod
    def get_training_data(request_url: str, params: dict = None):
        """
        Retrieves training data from the specified API endpoint.

        Args:
            request_url (str): The URL of the API endpoint.
            params (dict, optional): Additional parameters for the API request. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the training data.

        """

        return DataExtractProcessor.get_data_from_api(request_url, params= params)

    @staticmethod
    def get_inference_data(request_url: str, params: dict = None):
        """
        Retrieves inference data from the specified API endpoint.

        Args:
            request_url (str): The URL of the API endpoint.
            params (dict, optional): Additional parameters for the API request. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the inference data.

        """
        return DataExtractProcessor.get_data_from_api(request_url, params= params)
