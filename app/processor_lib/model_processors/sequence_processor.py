import pandas as pd

from app.processor_lib.model_processors import data_extract_processor as data_proc, sequence_comparer_processor as scp
import logging
import json
from datetime import datetime


logger = logging.getLogger(__name__)

columnMap = {
    "db_layout_id_ref": "LayoutIdRef",
    "db_layout_id_test": "LayoutIdTest",
    "Seq1": "TaskSequenceRef",
    "Seq2": "TaskSequenceTest",
    "Align_Seq1": "AlignTaskSequenceRef",
    "Align_Seq2": "AlignTaskSequenceTest",
    "Score": "Score"
}


class SequenceProcessor:
    def __init__(self):
        return self

    @staticmethod
    def get_similarity_byRef(notif_ref):
        processor = scp.SequenceComparerProcessor()
        df_data, df_result = processor.evaluate(notif_ref)

        return df_result.sort_values(by=['Score', 'layout_ref'], ascending=False)

    @staticmethod
    def get_similarity():
        batch_size = 100

        data_processor = data_proc.DataExtractProcessor()
        groupCodes = data_processor.get_group_codes()

        engine_programs = data_processor.get_engine_programs()

        seq_processor = scp.SequenceComparerProcessor()

        df_summ = pd.DataFrame(
            columns=["db_layout_id_ref", "layout_ref", "db_layout_id_test", "layout_test", "Seq1", "Seq2", "Align_Seq1",
                     "Align_Seq2", "Score"])

        dateFormat = "%Y-%m-%dT%H:%M:%S"
        runDate = datetime.now().strftime(dateFormat)

        for row_num,notification in enumerate(engine_programs['NotificationCode']):
            df_result = SequenceProcessor.get_similarity_byRef(notification)

            df_summ = pd.concat([df_summ, df_result], ignore_index=True)
            if row_num % batch_size == 0:
                df_result_save = json.loads(df_summ.rename(columns= columnMap).to_json(orient= "records"))
                df_summ = df_summ.drop(df_summ.index)
                SequenceProcessor.save_results(df_result_save, runDate)
        return df_summ

    @staticmethod
    def save_results(results, runDate):
        seq_processor = scp.SequenceComparerProcessor()
        seq_processor.save_results(results, runDate)
