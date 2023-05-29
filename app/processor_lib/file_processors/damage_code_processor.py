import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor


class DamageCodeProcessor(IEntityProcessor):
    __db_schema__ = 'etl'
    __db_table_name__ = 'DamageCode'
    __separator__ = ';'
    __db_handler__ = None

    columnNameMap = {
        "DamageCode": "DamageCode",
        "DamageText": "DamageText"
    }

    dTypeMap = {
        "DamageCode": 'string',
    }

    def __init__(self, db_handler: db.db_handler):
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        print('Damage Code Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        print("Reading Damage codes from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)
        if not res:
            raise RuntimeError("Unable to retrieve Damage Code from Database")
        else:
            data.set_index('Id', inplace=True, drop=False)
            data = data.astype(self.dTypeMap)
        return data

    def get_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")
        print("Reading Damage codes from file")
        try:
            data = pd.read_csv(file_path, dtype=self.dTypeMap, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve Damage Code" + ex.__str__())

        return data

    def process_file_import(self, source_df, conn, catch_failed=True):
        affected = 0
        errors = []
        failed = None
        print("Importing Damage codes from file")
        damageCode_df = source_df.rename(self.columnNameMap, axis=1)
        damageCode_df.fillna('', inplace=True)

        database_df = self.get_from_db(conn)
        damageCode_df_new = pd.merge(damageCode_df, database_df, on=['DamageCode', 'DamageText'], how='outer',
                                     indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        if not damageCode_df_new.empty:
            print(f'Identified {len(damageCode_df_new)} new records for Cause codes')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=damageCode_df_new,
                                                                       catch_failed=catch_failed
                                                                       )

        return affected, failed
