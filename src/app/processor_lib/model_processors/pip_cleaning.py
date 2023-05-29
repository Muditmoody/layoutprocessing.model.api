import pandas as pd
import numpy as np
from datetime import datetime
from app.processor_lib.model_processors.pip_constants import Constants
from app.processor_lib.model_processors.data_extract_processor import DataExtractProcessor as dcp
import math

import os

'''
Data Tables
GroupCode.csv info:
Id            int64
GroupCode    object
GroupText    object
dtype: object

CodingCode.csv info:
Id             int64
Coding        object
CodingText    object
dtype: object

SupplierVendor.csv info:
Id                int64
SupplierVendor    int64
dtype: object

DamageCode.csv info:
Id             int64
DamageCode     int64
DamageText    object
dtype: object

CauseCode.csv info:
Id            int64
CauseCode    object
CauseText    object
dtype: object

TaskOwner.csv info:
Id             int64
TaskOwner    float64
dtype: object

LayoutTask.csv info:
Id                  int64
Notification        int64
Item                int64
Task                int64
Material           object
Created_On         object
Completed_On       object
TaskText           object
GroupCode          object
TaskCode           object
Planned_Start      object
Planned_Finish     object
SupplierVendor      int64
TaskOwner         float64
TaskStatus         object
dtype: object

TaskStatus.csv info:
Id             int64
TaskStatus    object
dtype: object

EngineProgram.csv info:
Id               int64
Notification     int64
Coding          object
Description     object
dtype: object

Material.csv info:
Id              int64
Material       object
Description    object
dtype: object

LayoutType.csv info:
Id               int64
Notification     int64
Sort_Number      int64
Text            object
DamageCode       int64
CauseCode       object
dtype: object

TaskCode.csv info:
Id               int64
TaskCode        object
TaskCodeText    object
GroupCode       object
dtype: object
'''

def date2String(date, format):
    try:
        return date.strftime(format)
    except ValueError:
        return None

class Cleaning:
    def __init__(self):
        pass

    def __turnbackCheck__(self, text):
        flag = 0
        for keyword in ["turnback", "return"]:
            if keyword in str(text).lower():
                flag = 1
        return flag

        # Approved for life

    def __lifeCheck__(self, text):
        flag = 0
        if "life" in str(text).lower():
            flag = 1
        return flag

    def clean(self, dfs):
        # ADDING GENERALCODE
        groupMap = dfs["GeneralCode_df"]

        dfs['TaskCode_df'][["TaskCode", "GroupCode"]] = dfs['TaskCode_df'][["TaskCode", "GroupCode"]].apply(
            lambda x: x.astype(str).str.lower())
        dfs['TaskCode_df'] = pd.merge(dfs['TaskCode_df'], groupMap[["Code_Group", "Task_Code", "General_Code"]],
                                      how="left", left_on=["GroupCode", "TaskCode"],
                                      right_on=["Code_Group", "Task_Code"])
        dfs['TaskCode_df'] = dfs['TaskCode_df'].drop(columns=["Code_Group", "Task_Code"])
        dfs['TaskCode_df'][["TaskCode", "GroupCode"]] = dfs['TaskCode_df'][["TaskCode", "GroupCode"]].apply(
            lambda x: x.astype(str).str.upper())
        dfs['TaskCode_df'] = dfs['TaskCode_df'].rename(columns={'General_Code': 'GeneralCode'})

        # 'GroupCode' is one to many to taskcode
        dfs["LayoutProcessingTask_df"] = dfs["LayoutProcessingTask_df"][
            ['Notification', 'Item', 'LayoutTaskId', 'Task', 'Material', 'Created_On', 'Completed_On', 'TaskText',
             'TaskCodeId',
             'Planned_Start', 'Planned_Finish', 'SupplierVendorId', 'TaskOwnerId', 'TaskStatusId', 'DamageCodeId',
             'CauseCodeId', 'GroupCodeId', 'CategoryId']]

        task = pd.merge(dfs["LayoutProcessingTask_df"],
                        dfs["EngineProgram_df"][["Notification", "Coding", "Description"]], how="left",
                        left_on="Notification", right_on="Notification")
        task = task.rename(columns={"Description": "Engine"})
        task = pd.merge(task, dfs["SupplierVendor_df"], how="left", left_on=["SupplierVendorId"],
                        right_on=["SupplierVendorId"])
        task = pd.merge(task, dfs["TaskOwner_df"], how="left", left_on=["TaskOwnerId"], right_on=["TaskOwnerId"])
        task = pd.merge(task, dfs["TaskStatus_df"], how="left", left_on=["TaskStatusId"], right_on=["TaskStatusId"])
        task = pd.merge(task, dfs["GroupCode_df"][["GroupCodeId", "GroupCode"]], how="left", left_on=["GroupCodeId"],
                        right_on=["GroupCodeId"])
        task = pd.merge(task, dfs["CauseCode_df"][["CauseCodeId", "CauseCode"]], how="left", left_on=["CauseCodeId"],
                        right_on=["CauseCodeId"])
        task = pd.merge(task, dfs["DamageCode_df"][["DamageCodeId", "DamageCode"]], how="left",
                        left_on=["DamageCodeId"],
                        right_on=["DamageCodeId"])
        task = pd.merge(task, dfs["Category_df"], how="left", left_on=["CategoryId"], right_on=["CategoryId"])
        # task = pd.merge(task,dfs["LayoutType_df"][["Notification","Sort_Number","DamageCode","CauseCode"]],how="left",left_on=["Notification","Item"],right_on=["Notification","Sort_Number"]).drop(columns = ['Sort_Number'])
        # task = pd.merge(task,dfs["Material_df"][["Material","Description"]],how="left",left_on=["Material"],right_on=["Material"])
        # task = pd.merge(task,dfs["TaskCode_df"][["TaskCode","GroupCode","TaskCodeText", "GeneralCode"]],how="left",left_on=["TaskCode","GroupCode"],right_on=["TaskCode","GroupCode"])
        task = pd.merge(task, dfs["TaskCode_df"][["TaskCodeId", "TaskCode", "TaskCodeText", "GeneralCode"]], how="left",
                        left_on=["TaskCodeId"], right_on=["TaskCodeId"])

        # Index-related Columns to Integer
        for i in ["Notification", "Item", "Task"]:
            task[i] = task[i].map(lambda x: int(x))

        # Comment-related Columns to String
        task["TaskText"] = task["TaskText"].map(lambda x: str(x))

        # Time-related Columns to Datetime
        for i in ["Created_On", "Completed_On", "Planned_Start", "Planned_Finish"]:
            task[i] = pd.to_datetime(task[i], format="%Y/%m/%d", errors="coerce")

        # Lowercase all engine names
        task["Engine"] = task["Engine"].str.lower()

        # tags
        task["IsCompleted"] = task["Completed_On"].notna()
        Completed = task[["Notification", "Item", "IsCompleted"]].groupby(["Notification", "Item"]).min().rename(
            columns={"IsCompleted": "IsItemCompleted"})
        task = pd.merge(task, Completed, how="left", left_on=["Notification", "Item"],
                        right_on=["Notification", "Item"]).rename(columns={"IsCompleted": "IsTaskCompleted"})

        task["IsTurnback1"] = task["TaskCodeText"].map(lambda x: self.__turnbackCheck__(x))
        task["IsTurnback2"] = task["TaskText"].map(lambda x: self.__turnbackCheck__(x))
        task["IsTurnback"] = task[["IsTurnback1", "IsTurnback2"]].max(axis=1)
        task = task.drop(["IsTurnback1", "IsTurnback2"], axis=1)

        task["IsLife"] = task["TaskText"].map(lambda x: self.__lifeCheck__(x))

        planning_codes = ["tkat", "lpa", "vlml", "vlps", "psub"]
        flayout_planning_codes = ["0010", "0040", "0070", "0100", "0030", "0060", "0090", "0120"]
        IsPlanning = task['TaskCode'].isin(planning_codes) | (
                task['GroupCode'].isin(["flayout"]) & task['TaskCode'].isin(flayout_planning_codes))
        task['IsPlanning'] = IsPlanning
        # remove once inac part is added
        # task['GeneralCode'] = task['GeneralCode'].dropna()

        dropList = []
        dropNum = 0
        correctNum = 0
        for index, loc in task.iterrows():
            # Drop inactive/deleted/no demand tasks
            if ("delete" in loc["TaskCodeText"].lower()) or ("inactive" in loc["TaskCodeText"].lower()) or (
                    "no demand" in loc["TaskText"].lower()):
                dropList.append(index)
                dropNum += 1
            # Correct the "Completed On" for expiry/buffer/life approval-related tasks
            elif ("expiry" in str(loc["TaskCodeText"]).lower()) or \
                    ("expiry" in str(loc["TaskText"]).lower()) or \
                    ("expire" in str(loc["TaskText"]).lower()) or \
                    ("buffer" in str(loc["TaskText"]).lower()) or \
                    ("life" in str(loc["TaskText"]).lower()) or \
                    (loc["Planned_Start"] > datetime.strptime("2500/01/01", "%Y/%m/%d")) or \
                    (loc["Planned_Finish"] > datetime.strptime("2500/01/01", "%Y/%m/%d")):
                task.loc[index, "Completed_On"] = task.loc[index, "Created_On"]
                correctNum += 1
            elif (loc["Created_On"] > loc["Completed_On"]):
                task.loc[index, "Created_On"] = task.loc[index, "Completed_On"]
                correctNum += 1
        task['Available'] = np.where(task.index.isin(dropList), 0, 1)

        return task

    @staticmethod
    def process_cleaning(save_result=True):
        use_api = True
        filePath = "../data/processed/"
        save_path = "../../data/"

        dfs = {}

        if use_api:
            for key in Constants.CleaningSourceMap.keys():
                print(f"Processing for  {key}")
                request_url = Constants.CleaningSourceMap[key]['request_url']
                columnMap = Constants.CleaningSourceMap[key]['columnMap']
                drop_col = Constants.CleaningSourceMap[key]['drop_columns']
                is_active = Constants.CleaningSourceMap[key]['is_active']

                if is_active:
                    df_data = dcp.get_data_from_api(request_url=request_url)

                    if columnMap is not None:
                        df_data = df_data.rename(columns=columnMap)

                    if drop_col is not None and len(drop_col) > 0:
                        df_data = df_data.drop(columns=drop_col)
                    dfs[key + "_df"] = df_data

        else:
            for filename in os.listdir(filePath):
                if filename.endswith(".csv"):
                    filepath = os.path.join(filePath, filename)
                    print("Reading file", filename, "...")
                    df = pd.read_csv(filepath, sep=";")
                    rename_map = filename[:-4] + "Map"
                    dfs[filename[:-4] + "_df"] = df.rename(Constants.rename_map)

        cleaning_processor = Cleaning()
        cleaned_df = cleaning_processor.clean(dfs)
        cleaned_df = cleaned_df.rename(columns=Constants.cleanModelInputColMap)
        dateFormat = "%Y-%m-%dT%H:%M:%S"
        runDate = datetime.now().strftime(dateFormat)

        dateFormat = "%Y-%m-%d"
        cleaned_df = cleaned_df.pipe(lambda df: df.assign(plannedStart=lambda df: df['plannedStart'].apply(lambda x: date2String(x,dateFormat)))) \
            .pipe(lambda df: df.assign(plannedFinish=lambda df: df['plannedFinish'].apply(lambda x: date2String(x,dateFormat)))) \
            .pipe(lambda df: df.assign(createdDate=lambda df: df['createdDate'].apply(lambda x: date2String(x,dateFormat)))) \
            .pipe(lambda df: df.assign(completedDate=lambda df: df['completedDate'].apply(lambda x: date2String(x,dateFormat))))

        if save_result:
            if use_api:
                url = f"https://localhost:5001/api/model/ModelDataProcessed/SaveCleanModelInput?deleteExisting=False&runDate={runDate}"
                if len(cleaned_df) > 1000:
                    start = 0
                    end = 0
                    batch_size = 1000
                    for batch_num in range(0, int(math.ceil(len(cleaned_df) / batch_size))):
                        start = end
                        end = start + batch_size
                        data = cleaned_df.iloc[start:end]
                        dcp.save_results(request_url=url, result=data)
                else:
                    dcp.save_results(request_url=url, result=cleaned_df)

            else:
                cleaned_df.to_csv(save_path + "pipe_cleaned.csv", index=False)
                print("Cleaned Data Saved to", save_path + "pipe_cleaned.csv")

        return cleaned_df
