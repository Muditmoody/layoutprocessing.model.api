from abc import abstractmethod


class IEntityProcessor:
    """
    Interface for entity processors.

    Methods:
        get_from_db(conn, get_ref_value=False): Retrieves entity data from a database.
        get_from_file(file_path): Retrieves entity data from a file.
        process_file_import(source_df, conn, catch_failed=True): Processes entity data for file import.

    Attributes:
        None
    """
    @abstractmethod
    def get_from_db(self, conn, get_ref_value=False):
        """
        Retrieves entity data from a database.

        Args:
            conn: The database connection.
            get_ref_value (bool): Indicates whether to retrieve reference values or not.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented in a derived class.
        """
        pass

    @abstractmethod
    def get_from_file(self, file_path):
        """
        Retrieves entity data from a file.

        Args:
            file_path (str): Path to the file.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented in a derived class.
        """
        pass

    @abstractmethod
    def process_file_import(self, source_df, conn, catch_failed=True):
        """
        Processes entity data for file import.

        Args:
            source_df (pandas.DataFrame): The source data as a DataFrame.
            conn: The database connection.
            catch_failed (bool): Indicates whether to catch and handle import failures.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented in a derived class.
        """
        pass
