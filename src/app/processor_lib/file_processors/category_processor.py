import pandas as pd
import app.utils.db_handler as db
import os
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor

class CategoryProcessor(IEntityProcessor):
    """
    A class for processing category data.

    Inherits:
        IEntityProcessor: An interface for entity processors.

    Attributes:
        __db_schema__ (str): The database schema name.
        __db_table_name__ (str): The database table name.
        __separator__ (str): The separator used in the file.
        __db_handler__ (db.db_handler): The database handler.
        columnNameMap (dict): A mapping of column names.

    """

    def __init__(self, db_handler: db.db_handler):
        """
        Initializes a CategoryProcessor instance.

        Args:
            db_handler (db.db_handler): The database handler.

        Raises:
            IOError: If the DB handler is not initialized.

        """
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        print('Category Processor Initialized')

    def get_from_db(self, conn, get_ref_value=False):
        """
        Retrieves categories from the database.

        Args:
            conn: The database connection object.
            get_ref_value (bool): Whether to get reference values or not.

        Returns:
            pd.DataFrame: The retrieved category data.

        Raises:
            RuntimeError: If unable to retrieve category from the database.

        """
        print("Reading categories from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)
        if not res:
            raise RuntimeError("Unable to retrieve category from Database")
        else:
            data.set_index('Id', inplace=True, drop=False)
        return data

    def get_from_file(self, file_path):
        """
        Retrieves category data from a file.

        Args:
            file_path (str): The path to the file.

        Returns:
            pd.DataFrame: The retrieved category data.

        Raises:
            FileNotFoundError: If the file is not found.
            RuntimeError: If unable to retrieve category from the file.

        """
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")
        print("Reading category from file")
        try:
            data = pd.read_csv(file_path, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve category" + ex.__str__())

        return data

    def process_file_import(self, source_df, conn, catch_failed=True):
        """
        Processes the import of category data from a file.

        Args:
            source_df (pd.DataFrame): The source data to import.
            conn: The database connection object.
            catch_failed (bool): Whether to catch failed records or not.

        Returns:
            tuple: A tuple containing the number of affected records and any failed records.

        """
        affected = 0
        errors = []
        failed = None
        print("Importing cause codes from file")
        category_df = source_df.rename(self.columnNameMap, axis=1)
        category_df.fillna('', inplace=True)

        database_df = self.get_from_db(conn)
        category_df_new = pd.merge(category_df, database_df, on=['CategoryName'], how='outer',
                                    indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        if not category_df_new.empty:
            print(f'Identified {len(category_df_new)} new records for category')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=category_df_new,
                                                                       catch_failed=catch_failed)

        return affected, failed
