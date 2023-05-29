#!pip install openpyxl

from mysql.connector import CMySQLConnection, MySQLConnection
import pyodbc as odbc
from app.utils import db_handler as db
from app.Enum.EnumDatabase import EnumDatabase
import os
from typing import Union
from app.processor_lib.file_processors import coding_code_processor as cdc_proc, cause_code_processor as cc_proc, \
    damage_code_processor as dc_proc, engine_program_processor as ep_proc, layout_type_processor as lt_proc, \
    group_code_processor as gc_proc, material_processor as m_proc, task_owner_processor as to_proc, \
    task_code_processor as tc_proc, suppliervendor_processor as sv_proc, task_status_processor as ts_proc, \
    layout_task_processor as l_tsk_proc, category_processor as c_proc


class DataLoader:
    __db_handler__ = None
    __base_path__ = None
    __processed_dir__ = None
    __failed_dir__ = None
    __dbType__ = EnumDatabase.SQL_SERVER

    def __init__(self, db_handler: db.db_handler, db_type: EnumDatabase, base_path: str, failed_dir: str,
                 processed_dir: str):
        self.__dbType__ = EnumDatabase.SQL_SERVER if db_type is None else db_type
        self.__db_handler__ = db.db_handler(self.__dbType__) if db_handler is None else db_handler
        self.__base_path__ = ".." if (base_path is None or base_path == "") else base_path
        self.__processed_dir__ = "..\\processed" if (processed_dir is None or processed_dir == "") else processed_dir
        self.__failed_dir__ = "..\\failed" if (failed_dir is None or failed_dir == "") else failed_dir

    def connectToDb(self):
        conn = None
        match self.__dbType__:
            case EnumDatabase.SQL_SERVER:
                conn = self.__db_handler__.connect_db(server_name="LAPTOP-HMF8Q5ET\SQLEXPRESS",
                                                      db_name="PWC_layoutProcessing")
                # conn = handler.connect_db(server_name="LAPTOP-HMF8Q5ET\SQLEXPRESS", db_name="Test_db")
            case _:
                conn = self.__db_handler__.connect_db(server_name="localhost", userName="root", password="Mudit@1004")

        return conn

    def process_datafile(self ,conn: Union[CMySQLConnection | MySQLConnection | odbc.Connection]):
        feature_config = {
            "TaskOwner": {
                'file_name': 'TaskOwner.csv',
                'is_enabled': True,
                'processor': to_proc.TaskOwnerProcessor,
                'order': 0
            },
            "CauseCode": {
                'file_name': 'CauseCode.csv',
                'is_enabled': True,
                'processor': cc_proc.CauseCodeProcessor,
                'order': 0
            },
            "TaskStatus": {
                'file_name': 'TaskStatus.csv',
                'is_enabled': True,
                'processor': ts_proc.TaskStatusProcessor,
                'order': 0
            },
            "GroupCode": {
                'file_name': 'GroupCode.csv',
                'is_enabled': True,
                'processor': gc_proc.GroupCodeProcessor,
                'order': 0
            },
            "TaskCode": {
                'file_name': 'TaskCode.csv',
                'is_enabled': True,
                'processor': tc_proc.TaskCodeProcessor,
                'order': 1
            },
            "CodingCode": {
                'file_name': 'CodingCode.csv',
                'is_enabled': True,
                'processor': cdc_proc.CodingCodeProcessor,
                'order': 0
            },
            "DamageCode": {
                'file_name': 'DamageCode.csv',
                'is_enabled': True,
                'processor': dc_proc.DamageCodeProcessor,
                'order': 0
            },
            "Material": {
                'file_name': 'Material.csv',
                'is_enabled': True,
                'processor': m_proc.MaterialProcessor,
                'order': 1
            },
            "EngineProgram": {
                'file_name': 'EngineProgram.csv',
                'is_enabled': False,
                'processor': ep_proc.EngineProgramProcessor,
                'order': 2
            },
            "SupplierVendor": {
                'file_name': 'SupplierVendor.csv',
                'is_enabled': False,
                'processor': sv_proc.SupplierVendorProcessor,
                'order': 0
            },
            "LayoutType": {
                'file_name': 'LayoutType.csv',
                'is_enabled': True,
                'processor': lt_proc.LayoutTypeProcessor,
                'order': 3
            },
            "LayoutTask": {
                'file_name': 'LayoutTask.csv',
                'is_enabled': True,
                'processor': l_tsk_proc.LayoutTaskProcessor,
                'order': 4
            },
            "Category": {
                'file_name': 'Category.csv',
                'is_enabled': True,
                'processor': c_proc.CategoryProcessor,
                'order': 0
            },
        }

        keys = list(dict(sorted(feature_config.items(), key=lambda x: x[1]['order'])).keys())

        for item in keys:
            config = feature_config[item]
            if config['is_enabled']:
                processor_ref = config['processor']
                print(f'Processing - {item}')

                processor = processor_ref(self.__db_handler__)
                file_name = config['file_name']
                file_path = os.path.join(self.__processed_dir__, file_name)

                file_data_df = processor.get_from_file(file_path=file_path)
                affectedRows, failed = processor.process_file_import(source_df=file_data_df, conn=conn,
                                                                     catch_failed=True)
                print(f'For {item}, affected {affectedRows} rows')

                if not os.path.exists(self.__failed_dir__):
                    os.mkdir(self.__failed_dir__)
                if failed is not None:
                    failed.to_csv(os.path.join(self.__failed_dir__, f'Failed_{item}.csv'), index=False, sep=';')
