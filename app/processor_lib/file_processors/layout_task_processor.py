import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors import layout_type_processor as lt_proc, material_processor as m_proc, \
    task_owner_processor as to_proc, task_code_processor as tc_proc, suppliervendor_processor as sv_proc, \
    task_status_processor as ts_proc
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor
from app.Enum.EnumDatabase import EnumDatabase
from app.utils.date_util import date_util


class LayoutTaskProcessor(IEntityProcessor):
    __db_schema__ = 'etl'
    __db_table_name__ = 'LayoutProcessingTasks'
    __separator__ = ';'
    __db_handler__ = None
    __layoutTypeProcessor__ = None
    __materialProcessor__ = None
    __supplierProcessor__ = None
    __taskCodeProcessor__ = None
    __taskOwnerProcessor__ = None
    __taskStatusProcessor__ = None

    columnNameMap = {
        'Notification': "Notification_Id",
        'Item': "Item_Number",
        'Task': "Task_Id",
        'Material': "Material_Id",
        'Created_On': "Created_On",
        'Completed_On': "Completed_On",
        'TaskText': "Task_Text",
        'GroupCode': "GroupCode",
        'TaskCode': "TaskCode",
        'Planned_Start': "Planned_Start",
        'Planned_Finish': "Planned_Finish",
        'SupplierVendor': "SupplierVendor",
        'TaskOwner': "TaskOwner",
        'TaskStatus': "TaskStatus"
    }

    dTypeMap = {
        "Notification": "string",
        "Item": "string",
        "Task": "string",
        "Material": "string",
        'Created_On': "string",
        'Completed_On': "string",
        'TaskText': "string",
        'GroupCode': "string",
        'TaskCode': "string",
        'Planned_Start': "string",
        'Planned_Finish': "string",
        'SupplierVendor': "string",
        'TaskOwner': "string",
        'TaskStatus': "string"
    }

    def __init__(self, db_handler: db.db_handler):

        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler

        self.__layoutTypeProcessor__ = lt_proc.LayoutTypeProcessor(db_handler=db_handler)
        self.__materialProcessor__ = m_proc.MaterialProcessor(db_handler=db_handler)
        self.__supplierProcessor__ = sv_proc.SupplierVendorProcessor(db_handler=db_handler)
        self.__taskCodeProcessor__ = tc_proc.TaskCodeProcessor(db_handler=db_handler)
        self.__taskOwnerProcessor__ = to_proc.TaskOwnerProcessor(db_handler=db_handler)
        self.__taskStatusProcessor__ = ts_proc.TaskStatusProcessor(db_handler=db_handler)
        self.dateFormat = "%Y-%m-%d" if self.__db_handler__.databaseType == EnumDatabase.SQL_SERVER else "%Y-%m-%d"
        self.date_pattern = "[0-9]{1,2}[-][0-9]{1,2}[-][7-9]{4}"
        self.parser_format = "%Y-%m-%d %H:%M:%S"

        print('Layout Task Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        print("Reading LayoutType Task from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)

        if not res:
            raise RuntimeError("Unable to retrieve Layout Task from Database")
        else:
            data.set_index('Id', inplace=True, drop=False)
        return data

    def get_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")
        print("Reading Layout Task from file")
        try:
            data = pd.read_csv(file_path, dtype=self.dTypeMap, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve Layout Task" + ex.__str__())

        return data

    def process_file_import(self, source_df, conn, catch_failed=True):
        affected = 0
        errors = []
        failed = None
        print("Importing Layout Task from file")
        layoutTask_df = source_df.rename(self.columnNameMap, axis=1)
        layoutTask_df.fillna('', inplace=True)
        cols = list(self.columnNameMap.values())

        layoutType_df = self.__layoutTypeProcessor__.get_from_db(conn=conn, get_ref_value=True) \
            .pipe(lambda df: df.rename(columns={"Id": "lt_Id", 'Notification': "Notification_Id"})) \
            .pipe(lambda df: df[['lt_Id', 'Item_Number', 'Notification_Id']]) \
            .pipe(lambda df: df.astype(dtype={'lt_Id': int}))

        material_df = self.__materialProcessor__.get_from_db(conn=conn, get_ref_value=True) \
            .pipe(lambda df: df.rename(columns={"Id": "m_Id"})) \
            .pipe(lambda df: df[['m_Id', 'Material_Id']])

        taskCode_df = self.__taskCodeProcessor__.get_from_db(conn=conn, get_ref_value=True) \
            .pipe(lambda df: df.rename(columns={"Id": "tsk_Id"})) \
            .pipe(lambda df: df[['tsk_Id', 'TaskCode', 'GroupCode']])

        supplierVendor_df = self.__supplierProcessor__.get_from_db(conn=conn, get_ref_value=True) \
            .pipe(lambda df: df.rename(columns={"Id": "sv_Id", 'SupplierVendor_Id': "SupplierVendor"})) \
            .pipe(lambda df: df[['sv_Id', 'SupplierVendor']])

        taskOwner_df = self.__taskOwnerProcessor__.get_from_db(conn=conn, get_ref_value=True) \
            .pipe(lambda df: df.rename(columns={"Id": "to_Id", 'TaskOwner_Id': "TaskOwner"})) \
            .pipe(lambda df: df[['to_Id', 'TaskOwner']])

        taskStatus_df = self.__taskStatusProcessor__.get_from_db(conn=conn, get_ref_value=True) \
            .pipe(lambda df: df.rename(columns={"Id": "ts_Id"})) \
            .pipe(lambda df: df[['ts_Id', 'TaskStatus']])

        layoutTask_df_inner = layoutTask_df.pipe(lambda df: pd.merge(df, layoutType_df,
                                                                     left_on=['Notification_Id', 'Item_Number'],
                                                                     right_on=['Notification_Id', 'Item_Number'],
                                                                     how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['Notification_Id', 'Item_Number'])) \
            .pipe(lambda df: df.rename(columns={"lt_Id": "Layout_Id"})) \
            .pipe(lambda df: df[df['_merge'] == 'both']) \
            .pipe(lambda df: df.astype({'Layout_Id': int})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(lambda df: pd.merge(df, material_df, left_on=['Material_Id'], right_on=['Material_Id'],
                                      how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['Material_Id'])) \
            .pipe(lambda df: df.rename(columns={"m_Id": "Material_Id"})) \
            .pipe(lambda df: df.astype({'Material_Id': int})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(lambda df: pd.merge(df, taskOwner_df, left_on=['TaskOwner'], right_on=['TaskOwner'],
                                      how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['TaskOwner'])) \
            .pipe(lambda df: df.rename(columns={"to_Id": "Task_Owner_Id"})) \
            .pipe(lambda df: df.astype({'Task_Owner_Id': int})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(lambda df: pd.merge(df, supplierVendor_df, left_on=['SupplierVendor'], right_on=['SupplierVendor'],
                                      how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['SupplierVendor'])) \
            .pipe(lambda df: df.rename(columns={"sv_Id": "SupplierVendor_Id"})) \
            .pipe(lambda df: df.astype({'SupplierVendor_Id': int})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(lambda df: pd.merge(df, taskStatus_df, left_on=['TaskStatus'], right_on=['TaskStatus'],
                                      how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['TaskStatus'])) \
            .pipe(lambda df: df.rename(columns={"ts_Id": "Task_Status_Id"})) \
            .pipe(lambda df: df.astype({'Task_Status_Id': int})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(
            lambda df: pd.merge(df, taskCode_df, left_on=['TaskCode', 'GroupCode'], right_on=['TaskCode', 'GroupCode'],
                                how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['TaskCode', 'GroupCode'])) \
            .pipe(lambda df: df.rename(columns={"tsk_Id": "Task_Code_Id"})) \
            .pipe(lambda df: df.astype({'Task_Code_Id': int})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(lambda df: df.assign(Planned_Start=
                                       date_util.format_lifeline_or_missing_date(df['Planned_Start'], self.date_pattern,
                                                                                 self.dateFormat, self.parser_format))) \
            .pipe(lambda df: df.assign(Planned_Finish=
                                       date_util.format_lifeline_or_missing_date(df['Planned_Finish'],
                                                                                 self.date_pattern,
                                                                                 self.dateFormat, self.parser_format))) \
            .pipe(lambda df: df.assign(Created_On=
                                       date_util.format_lifeline_or_missing_date(df['Created_On'], self.date_pattern,
                                                                                 self.dateFormat, self.parser_format))) \
            .pipe(lambda df: df.assign(Completed_On=
                                       date_util.format_lifeline_or_missing_date(df['Completed_On'], self.date_pattern,
                                                                                 self.dateFormat, self.parser_format)))

        layoutTask_database_df = self.get_from_db(conn)

        layoutTask_database_df_new = pd.merge(layoutTask_df_inner, layoutTask_database_df,
                                              on=['Layout_Id', 'Task_Id'],
                                              how='left', indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        cols_y = [col for col in layoutTask_database_df_new.columns if '_y' in col]

        layoutTask_database_df_new.drop(columns=cols_y, inplace=True)
        for col in layoutTask_database_df_new.columns:
            if '_x' in col:
                layoutTask_database_df_new.rename(columns={col: col.replace('_x', '')}, inplace=True)

        if not layoutTask_database_df_new.empty:
            print(f'Identified {len(layoutTask_database_df_new)} new records for layoutType')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=layoutTask_database_df_new,
                                                                       catch_failed=catch_failed
                                                                       )

        layoutTask_df_outer = layoutTask_df.pipe(lambda df: pd.merge(df, layoutType_df,
                                                                     left_on=['Notification_Id', 'Item_Number'],
                                                                     right_on=['Notification_Id', 'Item_Number'],
                                                                     how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['Notification_Id', 'Item_Number'])) \
            .pipe(lambda df: df.rename(columns={"lt_Id": "Layout_Id"})) \
            .pipe(lambda df: df[df['_merge'] != 'both'])

        layoutTask_df_outer.to_csv(os.path.join('../..', "Failed.csv"), index=False, sep=';')

        return affected, failed
