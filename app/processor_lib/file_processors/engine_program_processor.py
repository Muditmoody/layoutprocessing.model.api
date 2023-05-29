import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors import coding_code_processor as cc_proc
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor


class EngineProgramProcessor(IEntityProcessor):
    __db_schema__ = 'etl'
    __db_table_name__ = 'EngineProgram'
    __separator__ = ';'
    __db_handler__ = None
    __codingCodeProcessor__ = None

    columnNameMap = {
        "Notification": "Notification_Id",
        "Coding": "CodingCode",
        "Description": "Description"
    }

    dTypeMap = {
        "Notification": "string",
        "Coding": "string",
        "Description": "string"
    }

    def __init__(self, db_handler: db.db_handler):
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        self.__codingCodeProcessor__ = cc_proc.CodingCodeProcessor(db_handler=db_handler)
        print('engineProgram Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        print("Reading EngineProgram from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)

        if res and get_ref_value:
            codingCode_df = self.__codingCodeProcessor__.get_from_db(conn) \
                .pipe(lambda df: df.rename(columns={"Id": "cc_Id", "Coding": "CodingCode"}))

            data = data.pipe(lambda df: df.rename(columns={"CodingCode": "d_codingCode"})) \
                .pipe(lambda df: pd.merge(df, codingCode_df, left_on="d_codingCode", right_on='cc_Id',
                                          how='left', indicator=True)) \
                .pipe(lambda df: df.drop(columns=["cc_Id", "d_codingCode"])) \
                .pipe(lambda df: df.drop(columns=['_merge']))

        if not res:
            raise RuntimeError("Unable to retrieve EngineProgram from Database")
        else:
            data.set_index('Id', inplace=True, drop=False)
        return data

    def get_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")
        print("Reading EngineProgram from file")
        try:
            data = pd.read_csv(file_path, dtype=self.dTypeMap, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve EngineProgram" + ex.__str__())

        return data

    def process_file_import(self, source_df, conn, catch_failed=True):
        affected = 0
        errors = []
        failed = None
        print("Importing TaskCode from file")
        engineProgram_df = source_df.rename(self.columnNameMap, axis=1)
        engineProgram_df.fillna('', inplace=True)

        codingCode_df = self.__codingCodeProcessor__.get_from_db(conn) \
            .pipe(lambda df: df.rename(columns={"Coding": "CodingCode"}))

        engineProgram_df = pd.merge(engineProgram_df, codingCode_df, on=['CodingCode'], how='outer', indicator=True) \
            .pipe(lambda df: df.drop(columns=["CodingCode", "CodingText"])) \
            .pipe(lambda df: df.rename(columns={"Id": "CodingCode"})) \
            .pipe(lambda df: df.drop(columns=['_merge']))

        engineProgram_database_df = self.get_from_db(conn)

        engineProgram_df_new = pd.merge(engineProgram_df, engineProgram_database_df,
                                        on=['Notification_Id', 'Description', 'CodingCode'], how='outer',
                                        indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        if not engineProgram_df_new.empty:
            print(f'Identified {len(engineProgram_df_new)} new records for EngineProgram')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=engineProgram_df_new,
                                                                       catch_failed=catch_failed
                                                                       )

        return affected, failed
