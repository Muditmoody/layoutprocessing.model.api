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
    def __init__(self):
        print("")

    @staticmethod
    def get_task_codes():
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
        if isinstance(result, pd.DataFrame):
            result = json.loads(result.to_json(orient="records"))

        response = requests.post(url=request_url, params=params, json=result, verify=False)

        if response.status_code == req_status.OK:
            print("ok")

    @staticmethod
    def get_training_data(request_url: str, params: dict = None):
        return DataExtractProcessor.get_data_from_api(request_url, params= params)

    @staticmethod
    def get_inference_data(request_url: str, params: dict = None):
        return DataExtractProcessor.get_data_from_api(request_url, params= params)
