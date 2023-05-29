import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor
from app.processor_lib.file_processors import group_code_processor as gc_proc


class TaskCodeProcessor(IEntityProcessor):
    __db_schema__ = 'etl'
    __db_table_name__ = 'TaskCode'
    __separator__ = ';'
    __db_handler__ = None
    __groupCodeProcessor__ = None

    columnNameMap = {
        "TaskCode": "TaskCode",
        "TaskCodeText": "TaskCodeText",
        "GroupCode": "GroupCode"

    }

    def __init__(self, db_handler: db.db_handler):
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        self.__groupCodeProcessor__ = gc_proc.GroupCodeProcessor(db_handler=db_handler)
        print('TaskCode Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        print("Reading TaskCode from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)
        if res and get_ref_value:
            groupCode_df = self.__groupCodeProcessor__.get_from_db(conn) \
                .pipe(lambda df: df.rename(columns={"Id": "gc_Id"}))

            data = data.pipe(lambda df: df.rename(columns={"GroupCode": "d_GroupCode"})) \
                .pipe(lambda df: pd.merge(df, groupCode_df, left_on='d_GroupCode', right_on='gc_Id',
                                          how='left', indicator=True)) \
                .pipe(lambda df: df.drop(columns=["gc_Id", "d_GroupCode"])) \
                .pipe(lambda df: df.drop(columns=['_merge']))

        if not res:
            raise RuntimeError("Unable to retrieve TaskCode from Database")
        else:
            data.set_index('Id', inplace=True, drop=False)
        return data

    def get_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")
        print("Reading TaskCode from file")
        try:
            data = pd.read_csv(file_path, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve GroupCode" + ex.__str__())

        return data

    def process_file_import(self, source_df, conn, catch_failed=True):
        affected = 0
        errors = []
        failed = None
        print("Importing TaskCode from file")
        taskCode_df = source_df.rename(self.columnNameMap, axis=1)
        taskCode_df.fillna('', inplace=True)

        groupCode_df = self.__groupCodeProcessor__.get_from_db(conn)

        taskCode_df = taskCode_df.pipe(lambda df: pd.merge(df, groupCode_df, on=['GroupCode'],
                                                           how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=["GroupCode", "GroupText"])) \
            .pipe(lambda df: df.rename(columns={"Id": "GroupCode"})) \
            .pipe(lambda df: df.drop(columns=['_merge']))

        taskCode_database_df = self.get_from_db(conn)

        taskCode_df_new = pd.merge(taskCode_df, taskCode_database_df, on=['TaskCode', 'TaskCodeText', 'GroupCode'],
                                   how='outer',
                                   indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        if not taskCode_df_new.empty:
            print(f'Identified {len(taskCode_df_new)} new records for Group Code')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=taskCode_df_new,
                                                                       catch_failed=catch_failed)

        return affected, failed
