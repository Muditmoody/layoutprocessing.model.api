import pandas as pd
import app.utils.db_handler as db
import os

from app.processor_lib.file_processors import cause_code_processor as cc_proc, damage_code_processor as dc_proc, \
    engine_program_processor as ep_proc
from app.processor_lib.file_processors.IEntityProcessor import IEntityProcessor


class LayoutTypeProcessor(IEntityProcessor):
    """
    A class that processes layout types.

    Attributes:
        __db_schema__ (str): The database schema name.
        __db_table_name__ (str): The database table name.
        __separator__ (str): The separator used in the file.
        __db_handler__ (db.db_handler): The database handler object.
        __damageCodeProcessor__ (damage_code_processor.DamageCodeProcessor): The damage code processor object.
        __causeCodeProcessor__ (cause_code_processor.CauseCodeProcessor): The cause code processor object.
        __engineProgramProcessor__ (engine_program_processor.EngineProgramProcessor): The engine program processor object.

    Methods:
        __init__(db_handler): Initializes the LayoutTypeProcessor instance.
        get_from_db(conn, get_ref_value): Retrieves layout type from the database.
        get_from_file(file_path): Retrieves layout type from a file.
        process_file_import(source_df, conn, catch_failed): Processes the file import for layout types.

    """
    __db_schema__ = 'etl'
    __db_table_name__ = 'LayoutType'
    __separator__ = ';'
    __db_handler__ = None
    __damageCodeProcessor__ = None
    __causeCodeProcessor__ = None
    __engineProgramProcessor__ = None

    columnNameMap = {
        "Notification": "Notification",
        "Sort_Number": "Item_Number",
        "Sort Number": "Item_Number",
        "Text": "Layout_Text",
        "Damage_Code": "DamageCode",
        "Cause_Code": "CauseCode"
    }

    dTypeMap = {
        "Notification": "string",
        "Sort_Number": "string",
        "Text": "string",
        "DamageCode": "string",
        "CauseCode": "string"
    }

    def __init__(self, db_handler: db.db_handler):
        """
        Initializes the LayoutTypeProcessor instance.

        Args:
            db_handler (db.db_handler): Database handler object.

        Raises:
            IOError: Raised if the DB handler is not initialized.

        """
        
        if db_handler is None:
            raise IOError("DB handler not initialized")
        self.__db_handler__ = db_handler
        self.__damageCodeProcessor__ = dc_proc.DamageCodeProcessor(db_handler=db_handler)
        self.__causeCodeProcessor__ = cc_proc.CauseCodeProcessor(db_handler=db_handler)
        self.__engineProgramProcessor__ = ep_proc.EngineProgramProcessor(db_handler=db_handler)
        print('LayoutType Processor Initialized')

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
        print("Reading LayoutType from Db")
        (res, data) = self.__db_handler__.read_from_db(conn=conn, schema=self.__db_schema__,
                                                       table_name=self.__db_table_name__)
        if res and get_ref_value:
            causeCode_df = self.__causeCodeProcessor__.get_from_db(conn, get_ref_value) \
                .pipe(lambda df: df.rename(columns={"Id": "cc_Id"}))

            damageCode_df = self.__damageCodeProcessor__.get_from_db(conn, get_ref_value) \
                .pipe(lambda df: df.rename(columns={"Id": "dc_Id"}))

            engineProgram_df = self.__engineProgramProcessor__.get_from_db(conn, get_ref_value) \
                .pipe(lambda df: df.rename(columns={"Id": "ep_Id", "Notification_Id": "Notification"})) \
                .pipe(lambda df: df.astype(dtype={"Notification": "string"}))

            data = data.pipe(lambda df: df.rename(columns={
                "Notification_Id": "d_Notification",
                "CauseCode": "d_CauseCode",
                'DamageCode': 'd_DamageCode'})) \
                .pipe(lambda df: df.astype(dtype={'Item_Number': "string"})) \
                .pipe(lambda df: pd.merge(df, engineProgram_df, left_on='d_Notification', right_on='ep_Id',
                                          how='left', indicator=True)) \
                .pipe(lambda df: df.drop(columns=['ep_Id', 'd_Notification'])) \
                .pipe(lambda df: df.drop(columns=['_merge'])) \
                .pipe(lambda df: pd.merge(df, causeCode_df, left_on='d_CauseCode', right_on='cc_Id',
                                          how='left', indicator=True)) \
                .pipe(lambda df: df.drop(columns=['cc_Id', 'd_CauseCode'])) \
                .pipe(lambda df: df.drop(columns=['_merge'])) \
                .pipe(lambda df: pd.merge(df, damageCode_df, left_on='d_DamageCode', right_on='dc_Id',
                                          how='left', indicator=True)) \
                .pipe(lambda df: df.drop(columns=['dc_Id', 'd_DamageCode'])) \
                .pipe(lambda df: df.drop(columns=['_merge']))

        if not res:
            raise RuntimeError("Unable to retrieve LayoutType from Database")
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
        print("Reading LayoutType from file")
        try:
            data = pd.read_csv(file_path, dtype=self.dTypeMap, sep=self.__separator__)
            if {'Id'}.issubset(data.columns):
                data.set_index('Id', inplace=True)
        except Exception as ex:
            raise RuntimeError("Unable to retrieve LayoutType" + ex.__str__())

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
        print("Importing LayoutType from file")
        layoutType_df = source_df.rename(self.columnNameMap, axis=1)
        layoutType_df.fillna('', inplace=True)
        cols = list(self.columnNameMap.values())

        layoutType_df = layoutType_df.loc[:, layoutType_df.columns.isin(cols)]

        causeCode_df = self.__causeCodeProcessor__.get_from_db(conn) \
            .pipe(lambda df: df.rename(columns={"Id": "cc_Id"})) \
            .pipe(lambda df: df.drop(columns=["CauseText"]))

        damageCode_df = self.__damageCodeProcessor__.get_from_db(conn) \
            .pipe(lambda df: df.rename(columns={"Id": "dc_Id"})) \
            .pipe(lambda df: df.drop(columns=["DamageText"]))

        engineProgram_df = self.__engineProgramProcessor__.get_from_db(conn) \
            .pipe(lambda df: df.rename(columns={"Id": "ep_Id", "Notification_Id": "Notification"})) \
            .pipe(lambda df: df.drop(columns=["Description", 'CodingCode']))

        layoutType_df = layoutType_df.pipe(lambda df: pd.merge(df, engineProgram_df, on=['Notification'],
                                                               how='left', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['Notification'])) \
            .pipe(lambda df: df.rename(columns={"ep_Id": "Notification_Id"})) \
            .pipe(lambda df: df.astype({'Notification_Id': int})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(lambda df: pd.merge(df, causeCode_df, on=['CauseCode'],
                                      how='left', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['CauseCode'])) \
            .pipe(lambda df: df.rename(columns={"cc_Id": "CauseCode"})) \
            .pipe(lambda df: df.drop(columns=['_merge'])) \
            .pipe(lambda df: pd.merge(df, damageCode_df, on=['DamageCode'],
                                      how='left', indicator=True)) \
            .pipe(lambda df: df.drop(columns=['DamageCode'])) \
            .pipe(lambda df: df.rename(columns={"dc_Id": "DamageCode"})) \
            .pipe(lambda df: df.drop(columns=['_merge']))

        layoutType_database_df = self.get_from_db(conn)

        layoutType_df_new = pd.merge(layoutType_df, layoutType_database_df,
                                     on=['Notification_Id', 'Item_Number', 'Layout_Text', 'DamageCode', 'CauseCode'],
                                     how='left', indicator=True) \
            .query("_merge == 'left_only'") \
            .drop(columns=['_merge', 'Id'])

        if not layoutType_df_new.empty:
            print(f'Identified {len(layoutType_df_new)} new records for layoutType')
            affected, errors, failed = self.__db_handler__.write_to_db(conn=conn, schema=self.__db_schema__,
                                                                       table_name=self.__db_table_name__,
                                                                       dataFrame=layoutType_df_new,
                                                                       catch_failed=catch_failed
                                                                       )

        return affected, failed
