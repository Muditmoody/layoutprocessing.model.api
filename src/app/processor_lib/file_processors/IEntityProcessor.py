from abc import abstractmethod


class IEntityProcessor:
    @abstractmethod
    def get_from_db(self, conn, get_ref_value=False):
        pass

    @abstractmethod
    def get_from_file(self, file_path):
        pass

    @abstractmethod
    def process_file_import(self, source_df, conn, catch_failed=True):
        pass
