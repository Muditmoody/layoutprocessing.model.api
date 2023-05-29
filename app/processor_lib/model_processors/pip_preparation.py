import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

from sklearn.ensemble import IsolationForest, RandomForestRegressor, GradientBoostingRegressor
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# Set SEED for random states
SEED = 53


def quarter(month):
    return (month - 1) // 3 + 1


def intNA(x):
    if str(type(x)) == "<class 'str'>":
        return x
    elif math.isnan(x):
        return "NA"
    else:
        return int(x)


# Process engine names
def generalEngine(x):
    if x.startswith("pw") or x.startswith("pt"):
        return x[:3]
    else:
        return x


# Bin major engine group
def binEngine(x):
    if x in ["pt6", "pw1", "pw3"]:
        return x
    else:
        return "other"


# Bin major cause code
def binCause_Code(x):
    if x in ["l055", "l070", "l020", "nan"]:
        return x
    else:
        return "other"


# Bin major damage code
def binDamage_Code(x):
    if x in ["10", "20", "30"]:
        return x
    else:
        return "other"


# Bin major supplier
def binSupplier_Vendor(x):
    if x in ["7965", "1375", "9483", "2849", "1510"]:
        return x
    else:
        return "other"


def freq(freqMap, word):
    if "na" in word.lower():
        return 0
    else:
        return freqMap[word]


def adjust_feature_names(f):
    newF = []
    for i in list(f):
        newF.append(i.split("__")[1])
    return newF


class Preparation:
    selected_features = ['Item', 'TaskOwnerExperience', 'DaysFromItemStart', 'DaysPerTask',
                         'Task', 'SupplierVendor_2849', 'Seasonality', 'DamageCode_30',
                         'CauseCode_l055', 'TaskCreatedMonth_7', 'TaskCreatedMonth_2',
                         'GeneralCode_ecpcrev', 'TaskCreatedMonth_9', 'GeneralCode_flayout_vend',
                         'TaskCreatedMonth_4', 'TaskCreatedMonth_10', 'DamageCode_10',
                         'Category_housing', 'SupplierVendor_7965', 'CauseCode_other',
                         'Category_vane', 'GeneralCode_flayout', 'GeneralCode_flayout_0020',
                         'Category_gearbox', 'Coding_z010', 'Coding_z020', 'DamageCode_20',
                         'TaskCreatedQuarter_3', 'TaskCreatedMonth_3',
                         'CauseCode_l070', 'TaskCreatedMonth_6', 'Category_blade', 'Category_case',
                         'TaskCreatedMonth_5', 'IsEval_False', 'GeneralCode_flayout_0110',
                         'GeneralCode_epsto', 'GeneralCode_ecmechan_0020', 'GeneralCode_fpqcr_0060',
                         'GeneralCode_ecturbin_0050', 'GeneralCode_flayout_0080',
                         'GeneralCode_eholdcod'
                         #, 'CauseCode_nan'
                         ]

    def __init__(self):
        pass

    @staticmethod
    def ItemCreatedOn_df(df):
        return df[['Notification', 'Item', 'Created_On']].groupby(['Notification', 'Item']). \
            agg({'Created_On': 'min'}).reset_index(). \
            rename(columns={"Created_On": "Item_Created_On"})

    @staticmethod
    def feature_manipulation(task):
        for i in ["Created_On", "Completed_On", "Planned_Start", "Planned_Finish"]:
            task[i] = pd.to_datetime(task[i], format="%Y/%m/%d", errors="coerce")
        task["TaskCreatedYear"] = task["Created_On"].map(lambda x: x.year)
        task["TaskCreatedMonth"] = task["Created_On"].map(lambda x: x.month)
        task["TaskCreatedQuarter"] = task["TaskCreatedMonth"].map(lambda x: quarter(x))

        # Keep only tasks after 2015
        # Keep about 2/3 of data
        #task = task[task["Created_On"].map(lambda x: x.year) >= 2015]

        task = pd.merge(task, Preparation.ItemCreatedOn_df(task),
                        how="left", left_on=["Notification", "Item"], right_on=["Notification", "Item"])
        task = task.rename(columns={"Created_On": "Task_Created_On"})

        task["DaysFromItemStart"] = task["Task_Created_On"] - task["Item_Created_On"]
        task["DaysFromItemStart"] = task["DaysFromItemStart"].map(lambda x: x + timedelta(days=1))
        task["DaysFromItemStart"] = task["DaysFromItemStart"].dt.days

        dtype = task.dtypes.to_frame().rename(columns={0: "Original Data Type"})

        # To string (from int and float)
        for col in ["Notification", "SupplierVendor", "DamageCode", "TaskCreatedMonth", "TaskCreatedQuarter",
                    "TaskOwner", "Engine"]:
            task[col] = task[col].apply(lambda x: intNA(x)).astype(str)

        # To bool (from int)
        for col in ["IsTurnback", "IsLife"]:
            task[col] = task[col].astype(bool)

        dtype["Adjusted Data Type"] = task.dtypes

        for col in list(dtype[dtype["Adjusted Data Type"] == "object"].index):
            task[col] = task[col].apply(lambda x: str(x).lower())

        taskAvailable = task.copy()

        taskAvailable["Engine"] = taskAvailable["Engine"].apply(lambda x: binEngine(x))
        taskAvailable["CauseCode"] = taskAvailable["CauseCode"].apply(lambda x: binCause_Code(x))
        taskAvailable["DamageCode"] = taskAvailable["DamageCode"].apply(lambda x: binDamage_Code(x))
        taskAvailable["SupplierVendor"] = taskAvailable["SupplierVendor"].apply(
            lambda x: binSupplier_Vendor(x))
        taskAvailable["IsExpiry"] = taskAvailable["TaskCodeText"].apply(lambda x: "expir" in x.lower()).astype(bool)
        taskAvailable["IsPCE"] = taskAvailable["TaskCodeText"].apply(lambda x: "pce" in x.lower()).astype(bool)
        taskAvailable["IsEval"] = taskAvailable["TaskCodeText"].apply(lambda x: "eval" in x.lower()).astype(bool)
        taskAvailable["IsReceive"] = taskAvailable["TaskCodeText"].apply(lambda x: "receive" in x.lower()).astype(bool)

        taskAvailable.drop(columns=["TaskCodeText"], inplace=True)

        # counting frequencies
        ownerList = taskAvailable["TaskOwner"].value_counts().to_dict()
        taskAvailable["TaskOwner"] = taskAvailable["TaskOwner"].apply(lambda x: freq(ownerList, x))
        taskAvailable = taskAvailable.rename(columns={"TaskOwner": "TaskOwnerExperience"})

        taskAvailable["DaysPerTask"] = taskAvailable["DaysFromItemStart"] / taskAvailable["Task"]
        taskAvailable["Seasonality"] = taskAvailable["TaskCreatedMonth"].apply(
            lambda x: np.cos(2 * np.pi * float(x) / 12))

        return taskAvailable

    @staticmethod
    def get_target(task_df):
        # add target column for training dataset
        task_df['TaskDuration'] = task_df["Completed_On"] - task_df["Task_Created_On"]
        task_df['TaskDuration'] = task_df['TaskDuration'].dt.days

        return task_df

    @staticmethod
    def drop_outliers(taskAvailable):
        # drop planning tasks
        # taskAvailable = taskAvailable[~taskAvailable['TaskCode'].isin(["tkat", "lpa", "vlml", "vlps", "psub"])]
        taskAvailable = taskAvailable[taskAvailable['IsPlanning'] == False]
        taskAvailable = taskAvailable[taskAvailable["Task_Created_On"].map(lambda x: x.year) >= 2015]

        # outliers
        isolationForest = IsolationForest(random_state=SEED,
                                          # contamination = 0.05,
                                          n_jobs=-1,
                                          verbose=1)
        outlierPred = isolationForest.fit_predict(taskAvailable[["Item", "Task", "TaskDuration", "DaysFromItemStart"]])
        taskAvailable = taskAvailable.iloc[outlierPred == 1]
        # taskSave = taskSave.iloc[outlierPred == 1]
        return taskAvailable

    @staticmethod
    def get_x_y(task):
        unavailable = ["Completed_On",
                       "Planned_Start",
                       "Planned_Finish",
                       "TaskStatus",
                       "IsTaskCompleted",
                       "IsItemCompleted"]
        taskAvailable = task.loc[:, ~task.columns.isin(unavailable)]
        dropList = ["Id",
                    "Notification",
                    # "Description",
                    "Task_Created_On",
                    "TaskText",
                    "Item_Created_On",
                    "Material"]
        taskAvailable.drop(columns=dropList, inplace=True)

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']

        num_col = taskAvailable.loc[:, ~taskAvailable.columns.isin(["TaskDuration"])].select_dtypes(
            include=numerics).columns
        cat_col = taskAvailable.select_dtypes(exclude=numerics).columns

        num_pipeline = Pipeline([
            ("impute", SimpleImputer(strategy="median")),
        ])

        cat_pipeline = Pipeline([
            ("impute", SimpleImputer(strategy="most_frequent")),
            ("encode", OneHotEncoder(sparse=False, dtype=int, handle_unknown="ignore"))
        ])

        pipe_tree = ColumnTransformer([("num", num_pipeline, num_col),
                                       ("cat", cat_pipeline, cat_col)])

        X = taskAvailable.drop(columns=['TaskDuration'])
        y = taskAvailable["TaskDuration"]

        X_tree = pipe_tree.fit_transform(X)
        X_tree = pd.DataFrame(X_tree, columns=adjust_feature_names(pipe_tree.get_feature_names_out()), index = X.index)

        X_tree['TaskDuration'] = taskAvailable['TaskDuration']
        return X_tree, y

    @staticmethod
    def get_x(task):
        task = task[task['IsTaskCompleted'] == False]

        unavailable = ["Completed_On",
                       "Planned_Start",
                       "Planned_Finish",
                       "TaskStatus",
                       "IsTaskCompleted",
                       "IsItemCompleted"]
        taskAvailable = task.loc[:, ~task.columns.isin(unavailable)]
        dropList = ["Id",
                    "Notification",
                    #"Description",
                    "Task_Created_On",
                    "TaskText",
                    "Item_Created_On",
                    "Material"]
        taskAvailable.drop(columns=dropList, inplace=True)

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']

        num_col = taskAvailable.select_dtypes(include=numerics).columns
        cat_col = taskAvailable.select_dtypes(exclude=numerics).columns

        num_pipeline = Pipeline([
            ("impute", SimpleImputer(strategy="median")),
        ])

        cat_pipeline = Pipeline([
            ("impute", SimpleImputer(strategy="most_frequent")),
            ("encode", OneHotEncoder(sparse=False, dtype=int, handle_unknown="ignore"))
        ])
        # pipe_lin = ColumnTransformer([("num_norm", num_norm_pipeline, num_col),
        #                            ("cat", cat_pipeline, cat_col)])

        pipe_tree = ColumnTransformer([("num", num_pipeline, num_col),
                                       ("cat", cat_pipeline, cat_col)])

        X_tree = pipe_tree.fit_transform(taskAvailable)
        X_tree = pd.DataFrame(X_tree, columns= adjust_feature_names(pipe_tree.get_feature_names_out()))

        return X_tree
