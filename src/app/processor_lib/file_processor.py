import sys
import getopt
import app.processor_lib.file_processors.dataloader as dl
import app.processor_lib.raw_file_loader as rfl
from app.utils import db_handler as db


def process_raw_file():
    """
    Process the raw file.

    Returns:
        None
    """
    raw_file_loader = rfl.RawFileLoader()

    base_path = "..\\data"
    process_dir = "..\\data\\processed"
    failed_dir = "..\\data\\failed"
    file_name = 'RawData_McGillMMA_20221014.xlsm'
    raw_file_loader.process(base_path, file_name)


def process_file_import():
    """
    Process file import.

    Returns:
        None
    """
    base_path = "..\\data"
    process_dir = "..\\data\\processed"
    failed_dir = "..\\data\\failed"

    dbType = db.EnumDatabase.SQL_SERVER
    handler = db.db_handler(dbType)

    data_loader = dl.DataLoader(db_handler=handler, db_type=dbType, base_path=base_path, processed_dir=process_dir,
                                failed_dir=failed_dir)
    conn = data_loader.connectToDb()
    data_loader.process_datafile(conn)


def process_files(raw_loader=False, file_import=False):
    """
    Process files based on the specified options.

    Args:
        raw_loader (bool): Flag indicating whether to perform raw file loading.
        file_import (bool): Flag indicating whether to perform file import.

    Returns:
        None
    """
    argumentList = sys.argv[1:]
    options = "ri:"

    long_options = ["raw_loader=", "file_import="]
    raw_loader_feature = raw_loader
    file_import_feature = file_import

    if raw_loader_feature:
        process_raw_file()
    if file_import_feature:
        process_file_import()

