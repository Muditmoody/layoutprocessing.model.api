#!pip install openpyxl
from app.Enum.EnumDatabase import EnumDatabase
from app.Enum.EnumFileType import EnumFileType
import os
from app.utils import db_handler as db

from app.processor_lib.file_processors import raw_processor as r_proc


class RawFileLoader:
    """
    Class for loading and processing raw files.

    Attributes:
        __base_path__ (str): Base path for file loading.
        __file_name__ (str): Name of the file to be processed.

    Methods:
        __init__(): Initializes the RawFileLoader object.
        process(base_path: str, file_name: str): Processes the raw file based on the specified base path and file name.
    """

    __base_path__ = ".."
    __file_name__ = 'RawData_McGillMMA_20221014.xlsm'

    def __init__(self):
        """
        Initializes the RawFileLoader object.

        Returns:
            None
        """
        print("File loader initalized")

    def process(self, base_path: str, file_name: str):
        """
        Processes the raw file based on the specified base path and file name.

        Args:
            base_path (str): Base path for file loading.
            file_name (str): Name of the file to be processed.

        Returns:
            None
        """
        self.__base_path__ = ".." if base_path is None or base_path == "" else base_path
        self.__file_name__ = 'RawData_McGillMMA_20221014.xlsm' if file_name is None or file_name == "" else file_name

        raw_processor = r_proc.RawDataProcessor()
        #data = raw_processor.get_from_file(file_path=os.path.join(base_path, file_name))
        data = raw_processor.get_from_file(file_path=f"{base_path}/{file_name}")

        for sheet in data.keys():
            match sheet.lower():
                case 'layouttype':
                    fileType = EnumFileType.LAYOUT_TYPE
                    raw_processor.process(raw_data_df=data[sheet], file_type=fileType, root_dir=base_path)
                case 'pwc_engineprogram':
                    fileType = EnumFileType.ENGINE_PROGRAM
                    raw_processor.process(raw_data_df=data[sheet], file_type=fileType, root_dir=base_path)
                case 'sap_raw_extract':
                    fileType = EnumFileType.SAP_RAW
                    raw_processor.process(raw_data_df=data[sheet], file_type=fileType, root_dir=base_path)
                case 'categories':
                    fileType = EnumFileType.CATEGORIES
                    raw_processor.process(raw_data_df=data[sheet], file_type=fileType, root_dir=base_path)
