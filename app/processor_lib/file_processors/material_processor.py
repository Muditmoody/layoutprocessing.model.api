import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor
from app.processor_lib.file_processors.category_processor import CategoryProcessor as cp_proc


class MaterialProcessor(IEntityProcessor):
    __db_schema__ = 'etl'
    __db_table_name__ = 'Material'
    __separator__ = ';'
    __db_handler__ = None
    __categoryProcessor__ = None

    columnNameMap = {
        "Material": "Material_Id",
        "Description": "Description",
        "Category": "CategoryName"
    }

    dTypeMap = {
        "Material": 'string'
    }

    def __init__(self, db_handler: db.db_handler):
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        self.__categoryProcessor__ = cp_proc (db_handler=db_handler)
        print('Material Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        print("Reading Material from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)

        if res and get_ref_value:
            category_df = self.__categoryProcessor__.get_from_db(conn) \
                .pipe(lambda df: df.rename(columns={"Id": "c_Id"}))

            data = data.pipe(lambda df: df.rename(columns={"Category_Id": "d_Category"})) \
                .pipe(lambda df: pd.merge(df, category_df, left_on='d_Category', right_on='c_Id',
                                          how='left', indicator=True)) \
                .pipe(lambda df: df.drop(columns=["c_Id", "d_Category"])) \
                .pipe(lambda df: df.drop(columns=['_merge']))

        if not res:
            raise RuntimeError("Unable to retrieve Material from Database")
        else:
            data.set_index('Id', inplace=True, drop=False)
        return data

    def get_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")
        print("Reading Material from file")
        try:
            data = pd.read_csv(file_path, dtype=self.dTypeMap, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve Material" + ex.__str__())

        return data

    def process_file_import(self, source_df, conn, catch_failed=True):
        affected = 0
        errors = []
        failed = None
        print("Importing Material from file")
        material_df = source_df.rename(self.columnNameMap, axis=1)
        material_df.fillna('', inplace=True)

        category_df = self.__categoryProcessor__.get_from_db(conn)

        material_df = material_df.pipe(lambda df: pd.merge(df, category_df, on=['CategoryName'],
                                                           how='outer', indicator=True)) \
            .pipe(lambda df: df.drop(columns=["CategoryName"])) \
            .pipe(lambda df: df.rename(columns={"Id": "Category_Id"})) \
            .pipe(lambda df: df.drop(columns=['_merge']))


        database_df = self.get_from_db(conn)
        material_df_new = pd.merge(material_df, database_df, on=['Material_Id', "Description", "Category_Id"], how='outer',
                                   indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        if not material_df_new.empty:
            print(f'Identified {len(material_df_new)} new records for Material')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=material_df_new,
                                                                       catch_failed=catch_failed)

        return affected, failed
