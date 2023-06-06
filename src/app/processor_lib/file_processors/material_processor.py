import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor
from app.processor_lib.file_processors.category_processor import CategoryProcessor as cp_proc


class MaterialProcessor(IEntityProcessor):
    """
    A class that processes material types.

    Attributes:
        __db_schema__ (str): The database schema name.
        __db_table_name__ (str): The database table name.
        __separator__ (str): The separator used in the file.
        __db_handler__ (db.db_handler): The database handler object.
        __categoryProcessor__ (__categoryProcessor__.CategoryProcessor): The category code processor object.
        
    Methods:
        __init__(db_handler): Initializes the MaterialProcessor instance.
        get_from_db(conn, get_ref_value): Retrieves material from the database.
        get_from_file(file_path): Retrieves materials from a file.
        process_file_import(source_df, conn, catch_failed): Processes the file import for materials.

    """
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
        """
        Initializes the MaterialProcessor instance.

        Args:
            db_handler (db.db_handler): Database handler object.

        Raises:
            IOError: Raised if the DB handler is not initialized.

        """
        
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        self.__categoryProcessor__ = cp_proc (db_handler=db_handler)
        print('Material Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        """
        Retrieves data from the database.

        Args:
            conn: Database connection object.
            get_ref_value (bool, optional): Flag to get reference values. Defaults to False.

        Returns:
            pandas.DataFrame: Retrieved entity data.

        Raises:
            RuntimeError: Raised if unable to retrieve data from the database.

        """
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
        """
        Retrieves data from a file.

        Args:
            file_path (str): Path to the file.

        Returns:
            pandas.DataFrame: Retrieved entity data.

        Raises:
            FileNotFoundError: Raised if the file is not found.
            RuntimeError: Raised if unable to retrieve data from the file.

        """
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
        """
        Processes the file import for source data.

        Args:
            source_df (pandas.DataFrame): Source data to import.
            conn: Database connection object.
            catch_failed (bool, optional): Flag to catch failed records. Defaults to True.

        Returns:
            tuple: A tuple containing the number of affected records, a list of errors, and failed records.

        """
        
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
