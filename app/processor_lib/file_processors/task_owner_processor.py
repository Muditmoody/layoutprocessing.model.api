import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor


class TaskOwnerProcessor(IEntityProcessor):
    __db_schema__ = 'etl'
    __db_table_name__ = 'TaskOwner'
    __separator__ = ';'
    __db_handler__ = None

    columnNameMap = {
        "TaskOwner": "TaskOwner_Id",
    }

    dTypeMap = {
        "TaskOwner_Id": 'string',
    }
    dTypeMap_1 = {
        "TaskOwner": 'string',
    }

    def __init__(self, db_handler: db.db_handler):
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        print('Task Owner Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        print("Reading Task owners from DB")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)
        if not res:
            raise RuntimeError("Unable to retrieve Task Owner from Database")
        else:
            data.set_index('Id', inplace=True, drop=False)
            data = data.astype(self.dTypeMap)
        return data

    def get_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")

        print("Reading Task owners from file")
        try:
            data = pd.read_csv(file_path, sep=self.__separator__, dtype=self.dTypeMap_1)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
            data.rename(self.columnNameMap, axis=1, inplace=True)
            data = data.astype(self.dTypeMap)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve Task Owner" + ex.__str__())

        return data

    def process_file_import(self, source_df, conn, catch_failed=True):
        affected = 0
        errors = []
        failed = None
        print("Importing Task owners from file")
        taskOwner_df = source_df.rename(self.columnNameMap, axis=1)
        taskOwner_df.fillna('', inplace=True)

        database_df = self.get_from_db(conn)
        taskOwner_df_new = pd.merge(taskOwner_df, database_df, on=['TaskOwner_Id'], how='outer',
                                    indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])


        if not taskOwner_df_new.empty:
            print(f'Identified {len(taskOwner_df_new)} new records for task owners')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=taskOwner_df_new,
                                                                       catch_failed=catch_failed)

        return affected, failed
