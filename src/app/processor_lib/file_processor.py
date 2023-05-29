import sys
import getopt
import app.processor_lib.file_processors.dataloader as dl
import app.processor_lib.raw_file_loader as rfl
from app.utils import db_handler as db


def process_raw_file():
    raw_file_loader = rfl.RawFileLoader()

    base_path = "..\\data"
    process_dir = "..\\data\\processed"
    failed_dir = "..\\data\\failed"
    file_name = 'RawData_McGillMMA_20221014.xlsm'
    raw_file_loader.process(base_path, file_name)


def process_file_import():
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
    argumentList = sys.argv[1:]
    options = "ri:"

    long_options = ["raw_loader=", "file_import="]
    raw_loader_feature = raw_loader
    file_import_feature = file_import

    '''
    try:
        # Parsing argument
        arguments, values = getopt.getopt(argumentList, options, long_options)

        # checking each argument
        for currentArgument, currentValue in arguments:

            if currentArgument in ("-r", "--raw_loader"):
                raw_loader_feature = True

            elif currentArgument in ("-i", "--file_import"):
                file_import_feature = True
    except Exception as ex:
        print(ex)
    '''
    if raw_loader_feature:
        process_raw_file()
    if file_import_feature:
        process_file_import()

