import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor


class TaskStatusProcessor(IEntityProcessor):
    """
    Task status Processor class for handling Task status data.

    Attributes:
        __db_schema__ (str): Database schema name.
        __db_table_name__ (str): Database table name.
        __separator__ (str): Separator used in the file.
        __db_handler__ (db.db_handler): Database handler object.
        columnNameMap (dict): Mapping of column names.

    Methods:
        __init__(db_handler): Initializes the TaskStatusProcessor instance.
        get_from_db(conn, get_ref_value): Retrieves Task status from the database.
        get_from_file(file_path): Retrieves Task status from a file.
        process_file_import(source_df, conn, catch_failed): Processes the file import for Task status.

    """

    __db_schema__ = 'etl'
    __db_table_name__ = 'TaskStatus'
    __separator__ = ';'
    __db_handler__ = None

    columnNameMap = {
        "TaskStatus": "TaskStatus",
    }

    def __init__(self, db_handler: db.db_handler):
        """
        Initializes the TaskStatusProcessor instance.

        Args:
            db_handler (db.db_handler): Database handler object.

        Raises:
            IOError: Raised if the DB handler is not initialized.

        """
        
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        print('Task Status Processor Initialized')

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
        print("Reading Task status from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)
        if not res:
            raise RuntimeError("Unable to retrieve Task status from Database")
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
        print("Reading Task status from file")
        try:
            data = pd.read_csv(file_path, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve Task status" + ex.__str__())

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
        print("Importing Task status from file")
        taskStatus_df = source_df.rename(self.columnNameMap, axis=1)
        taskStatus_df.fillna('', inplace=True)

        database_df = self.get_from_db(conn)
        taskStatus_df_new = pd.merge(taskStatus_df, database_df, on=['TaskStatus'], how='outer',
                                     indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        if not taskStatus_df_new.empty:
            print(f'Identified {len(taskStatus_df_new)} new records for Task status')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=taskStatus_df_new,
                                                                       catch_failed=catch_failed)

        return affected, failed
