import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors import coding_code_processor as cc_proc
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor


class EngineProgramProcessor(IEntityProcessor):
    """
    Engine program Processor class for handling cause code data.

    Attributes:
        __db_schema__ (str): Database schema name.
        __db_table_name__ (str): Database table name.
        __separator__ (str): Separator used in the file.
        __db_handler__ (db.db_handler): Database handler object.
        columnNameMap (dict): Mapping of column names.

    Methods:
        __init__(db_handler): Initializes the EngineProgramProcessor instance.
        get_from_db(conn, get_ref_value): Retrieves engine program from the database.
        get_from_file(file_path): Retrieves engine program from a file.
        process_file_import(source_df, conn, catch_failed): Processes the file import for engine program.

    """
    
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
        """
        Initializes the EngineProgramProcessor instance.

        Args:
            db_handler (db.db_handler): Database handler object.

        Raises:
            IOError: Raised if the DB handler is not initialized.

        """
        
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        self.__codingCodeProcessor__ = cc_proc.CodingCodeProcessor(db_handler=db_handler)
        print('engineProgram Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        """
        Retrieves engine programs from the database.

        Args:
            conn: Database connection object.
            get_ref_value (bool, optional): Flag to get reference values. Defaults to False.

        Returns:
            pandas.DataFrame: Retrieved engine program data.

        Raises:
            RuntimeError: Raised if unable to retrieve engine programs from the database.

        """
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
        """
        Retrieves engine programs from a file.

        Args:
            file_path (str): Path to the file.

        Returns:
            pandas.DataFrame: Retrieved engine programs data.

        Raises:
            FileNotFoundError: Raised if the file is not found.
            RuntimeError: Raised if unable to retrieve engine prorgams from the file.

        """
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
