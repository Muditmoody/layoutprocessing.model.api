import pandas as pd
from app.utils import db_handler as db
import os

from app.Enum.EnumFileType import EnumFileType


class RawDataProcessor:
    columnNameMap = {
        "CauseCode": "CauseCode",
        "CauseText": "CauseText"
    }

    __layoutType_ColumnMap__ = {
        'Damage Code': "DamageCode",
        'Prob. code text': 'DamageText',
        'Cause code': 'CauseCode',
        'Cause code text': 'CauseText',
        'Sort_Number': 'Sort_Number',
        'Sort number': 'Sort_Number',
    }

    __engineProgram_ColumnMap__ = {
        'Coding': "Coding",
        'Coding code txt': 'CodingText'
    }

    __categories_ColumnMap__ = {
        'Description': "Description",
        'Category': "Category"
    }

    __sap_raw_extract_ColumnMap__ = {
        'Created On': "Created_On",
        'Completed On': "Completed_On",
        'Task text': "TaskText",
        'Code group': "GroupCode",
        'Task group text': "GroupText",
        'Task code': "TaskCode",
        'Task code text': "TaskCodeText",
        'Planned start': "Planned_Start",
        'Planned finish': "Planned_Finish",
        'Supplier Vendor': "SupplierVendor",
        'Task Owner': "TaskOwner",
        'Task Status': "TaskStatus",
        'CATEGORY': "Category",
    }

    def __init__(self):
        print('Raw File Processor Initialized')

    @staticmethod
    def get_from_file(file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")
        print("Reading raw data from file")

        data = pd.DataFrame()
        try:
            fileExt = os.path.splitext(file_path)[-1].lower()
            match fileExt:
                case '.csv':
                    data = pd.read_csv(file_path, dtype='string')
                case '.xls' | '.xlsx' | '.xlsm':
                    data = pd.read_excel(file_path, sheet_name=None, dtype='string')
        except Exception as ex:
            raise RuntimeError("Unable to retrieve Cause Code" + ex.__str__())

        return data

    def process(self, raw_data_df, file_type, root_dir):
        root_dir = os.path.join(root_dir, "processed")
        if not os.path.exists(root_dir):
            os.mkdir(root_dir)

        match file_type:
            case EnumFileType.LAYOUT_TYPE:
                print('Processing Layout process')
                self.__process_layout_type__(raw_data_df, root_dir)
            case EnumFileType.ENGINE_PROGRAM:
                print('Processing Engine program')
                self.__process_engine_program__(raw_data_df, root_dir)
            case EnumFileType.SAP_RAW:
                print('Processing sap raw extract')
                self.__process_sap_raw__(raw_data_df, root_dir)
            case EnumFileType.CATEGORIES:
                print('Processing categories')
                self.__process_categories__(raw_data_df, root_dir)
            case _:
                print('None')

    def __process_layout_type__(self, raw_data: pd.DataFrame, root_dir: str) -> None:
        raw_data_df = raw_data.rename(columns=self.__layoutType_ColumnMap__)

        if {'DamageCode', 'DamageText'}.issubset(raw_data_df.columns):
            damageCode_df = raw_data_df[['DamageCode', 'DamageText']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='DamageCode')
            if not damageCode_df.empty:
                damageCode_df.index = pd.RangeIndex(start=1, stop=len(damageCode_df) + 1, step=1)
                damageCode_df.reset_index(inplace=True)
                damageCode_df.rename(columns={"index": "Id"}, inplace=True)
                damageCode_df.to_csv(os.path.join(root_dir, "DamageCode.csv"), index=False, sep=';')

        if {'CauseCode', 'CauseText'}.issubset(raw_data_df.columns):
            causeCode_df = raw_data_df[['CauseCode', 'CauseText']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='CauseCode')
            if not causeCode_df.empty:
                causeCode_df.index = pd.RangeIndex(start=1, stop=len(causeCode_df) + 1, step=1)
                causeCode_df.reset_index(inplace=True)
                causeCode_df.rename(columns={"index": "Id"}, inplace=True)
                causeCode_df.to_csv(os.path.join(root_dir, "CauseCode.csv"), index=False, sep=';')

        if {'Notification', 'Sort_Number', 'Text', 'DamageCode', 'CauseCode'}.issubset(raw_data_df.columns):
            layoutType_df = raw_data_df[['Notification', 'Sort_Number', 'Text', 'DamageCode', 'CauseCode']] \
                .sort_values(by=['Notification', 'Sort_Number'])
            if not layoutType_df.empty:
                layoutType_df.index = pd.RangeIndex(start=1, stop=len(layoutType_df) + 1, step=1)
                layoutType_df.reset_index(inplace=True)
                layoutType_df.rename(columns={"index": "Id"}, inplace=True)
                layoutType_df.to_csv(os.path.join(root_dir, "LayoutType.csv"), index=False, sep=';')

    def __process_engine_program__(self, raw_data: pd.DataFrame, root_dir: str) -> None:
        raw_data_df = raw_data.rename(columns=self.__engineProgram_ColumnMap__)

        if {'Coding', 'CodingText'}.issubset(raw_data_df.columns):
            codingCode_df = raw_data_df[['Coding', 'CodingText']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='Coding')
            if not codingCode_df.empty:
                codingCode_df.index = pd.RangeIndex(start=1, stop=len(codingCode_df) + 1, step=1)
                codingCode_df.reset_index(inplace=True)
                codingCode_df.rename(columns={"index": "Id"}, inplace=True)
                codingCode_df.to_csv(os.path.join(root_dir, "CodingCode.csv"), index=False, sep=';')

        if {'Notification', 'Coding', 'Description'}.issubset(raw_data_df.columns):
            engineProgram_df = raw_data_df[['Notification', 'Coding', 'Description']] \
                .sort_values(by=['Notification'])
            if not engineProgram_df.empty:
                engineProgram_df.index = pd.RangeIndex(start=1, stop=len(engineProgram_df) + 1, step=1)
                engineProgram_df.reset_index(inplace=True)
                engineProgram_df.rename(columns={"index": "Id"}, inplace=True)
                engineProgram_df.to_csv(os.path.join(root_dir, "EngineProgram.csv"), index=False, sep=';')

    def __process_sap_raw__(self, raw_data: pd.DataFrame, root_dir: str) -> None:
        raw_data_df = raw_data.rename(columns=self.__sap_raw_extract_ColumnMap__)
        raw_data_df.fillna('', inplace=True)

        if {'GroupCode', 'GroupText'}.issubset(raw_data_df.columns):
            groupCode_df = raw_data_df[['GroupCode', 'GroupText']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='GroupCode')
            if not groupCode_df.empty:
                groupCode_df.index = pd.RangeIndex(start=1, stop=len(groupCode_df) + 1, step=1)
                groupCode_df.reset_index(inplace=True)
                groupCode_df.rename(columns={"index": "Id"}, inplace=True)
                groupCode_df.to_csv(os.path.join(root_dir, "GroupCode.csv"), index=False, sep=';')

        if {'TaskCode', 'TaskCodeText', 'GroupCode'}.issubset(raw_data_df.columns):
            taskCode_df = raw_data_df[['TaskCode', 'TaskCodeText', 'GroupCode']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='TaskCode')
            if not taskCode_df.empty:
                taskCode_df.index = pd.RangeIndex(start=1, stop=len(taskCode_df) + 1, step=1)
                taskCode_df.reset_index(inplace=True)
                taskCode_df.rename(columns={"index": "Id"}, inplace=True)
                taskCode_df.to_csv(os.path.join(root_dir, "TaskCode.csv"), index=False, sep=';')

        if {'SupplierVendor'}.issubset(raw_data_df.columns):
            supplierVendor_df = raw_data_df[['SupplierVendor']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='SupplierVendor')
            if not supplierVendor_df.empty:
                supplierVendor_df.index = pd.RangeIndex(start=1, stop=len(supplierVendor_df) + 1, step=1)
                supplierVendor_df.reset_index(inplace=True)
                supplierVendor_df.rename(columns={"index": "Id"}, inplace=True)
                supplierVendor_df.to_csv(os.path.join(root_dir, "SupplierVendor.csv"), index=False, sep=';')

        if {'TaskOwner'}.issubset(raw_data_df.columns):
            taskOwner_df = raw_data_df[['TaskOwner']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='TaskOwner')
            if not taskOwner_df.empty:
                taskOwner_df.index = pd.RangeIndex(start=1, stop=len(taskOwner_df) + 1, step=1)
                taskOwner_df.reset_index(inplace=True)
                taskOwner_df.rename(columns={"index": "Id"}, inplace=True)
                taskOwner_df.to_csv(os.path.join(root_dir, "TaskOwner.csv"), index=False, sep=';')

        if {'TaskStatus'}.issubset(raw_data_df.columns):
            taskStatus_df = raw_data_df[['TaskStatus']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='TaskStatus')
            if not taskStatus_df.empty:
                taskStatus_df.index = pd.RangeIndex(start=1, stop=len(taskStatus_df) + 1, step=1)
                taskStatus_df.reset_index(inplace=True)
                taskStatus_df.rename(columns={"index": "Id"}, inplace=True)
                taskStatus_df.to_csv(os.path.join(root_dir, "TaskStatus.csv"), index=False, sep=';')

        if {'Material', 'Description', 'Category'}.issubset(raw_data_df.columns):
            material_df = raw_data_df[['Material', 'Description', 'Category']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='Material')
            if not material_df.empty:
                material_df.index = pd.RangeIndex(start=1, stop=len(material_df) + 1, step=1)
                material_df.reset_index(inplace=True)
                material_df.rename(columns={"index": "Id"}, inplace=True)
                material_df.to_csv(os.path.join(root_dir, "Material.csv"), index=False, sep=';')

        if {'Notification', 'Item', 'Task', 'Material', 'Created_On', 'Completed_On', 'TaskText', 'GroupCode',
            'TaskCode', 'Planned_Start', 'Planned_Finish', 'SupplierVendor', 'TaskOwner', 'TaskStatus'} \
                .issubset(raw_data_df.columns):

            layout_task_df = raw_data_df[
                ['Notification', 'Item', 'Task', 'Material', 'Created_On', 'Completed_On', 'TaskText', 'GroupCode',
                 'TaskCode', 'Planned_Start', 'Planned_Finish', 'SupplierVendor', 'TaskOwner', 'TaskStatus']] \
                .drop_duplicates(keep='first') \
                .sort_values(by=['Notification', 'Item', 'Task'])
            if not layout_task_df.empty:
                layout_task_df.index = pd.RangeIndex(start=1, stop=len(layout_task_df) + 1, step=1)
                layout_task_df.reset_index(inplace=True)
                layout_task_df.rename(columns={"index": "Id"}, inplace=True)
                layout_task_df.to_csv(os.path.join(root_dir, "LayoutTask.csv"), index=False, sep=';')

    def __process_categories__(self, raw_data: pd.DataFrame, root_dir: str) -> None:
        raw_data_df = raw_data.rename(columns=self.__categories_ColumnMap__)

        if {'Category'}.issubset(raw_data_df.columns):
            category_df = raw_data_df[['Category']] \
                .drop_duplicates(keep='first') \
                .sort_values(by='Category')
            if not category_df.empty:
                category_df.index = pd.RangeIndex(start=1, stop=len(category_df) + 1, step=1)
                category_df.reset_index(inplace=True)
                category_df.rename(columns={"index": "Id"}, inplace=True)
                category_df.to_csv(os.path.join(root_dir, "Category.csv"), index=False, sep=';')
