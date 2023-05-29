import pandas as pd
import requests
from http import HTTPStatus as sc
import numpy as np
from app.processor_lib.model_processors import data_extract_processor as data_proc
from app.processor_lib.business_lib import smith_waterman as sw, needle_wunsch as nw
from datetime import datetime


def extract_taskCode(row):
    return row['taskCodeName']


def extract_groupCode(row):
    return row['groupCode']['codeGroupName']


def extract_itemNumber(row):
    return row['itemNumber']


def calculate_date_diff(row):
    date_format = "%Y-%m-%dT%H:%M:%S"
    date_start = datetime.strptime(row['CreatedOn'], date_format)
    date_end = datetime.strptime(row['CompletedOn'], date_format)
    return (date_end-date_start).days + 1

def hybrid_alignment(seq_1, seq_2):
    global_alignment_1_2 = nw.NeedleWunsch.perform_alignment(seq_1, seq_2)
    local_alignment_1_2_x = sw.SmithWaterman.perform_alignment(global_alignment_1_2[0], global_alignment_1_2[1])
    return local_alignment_1_2_x


class SequenceComparerProcessor:
    __taskCodes__ = None
    __groupCodes__ = None

    def __init__(self):
        self.__taskCodes__ = self.__taskCodes__ if self.__taskCodes__ is not None else data_proc.DataExtractProcessor.get_task_codes()
        self.__groupCodes__ = self.__groupCodes__ if self.__groupCodes__ is not None else data_proc.DataExtractProcessor.get_group_codes()
        print("")

    def evaluate(self, notif_ref):
        response = requests.get(
            f'http://localhost:6001/api/etl/LayoutTask/GetLayoutTypeByNotification?notificationRef={notif_ref}',
            verify=False)

        data = ""

        if response.status_code == sc.OK:
            content_type = response.headers['content-type'].split(';')[0]
            if content_type == "application/json":
                data = response.json()

        # print(json.dumps(obj=data, indent=2))
        df_1 = pd.DataFrame.from_dict(data)
        df_summ = pd.DataFrame(
            columns=["db_layout_id_ref", "layout_ref", "db_layout_id_test", "layout_test", "Seq1", "Seq2", "Align_Seq1",
                     "Align_Seq2", "Score"])

        if len(df_1) > 0:
            self.__taskCodes__ = self.__taskCodes__ if self.__taskCodes__ is not None else data_proc.DataExtractProcessor.get_task_codes()
            self.__groupCodes__ = self.__groupCodes__ if self.__groupCodes__ is not None else data_proc.DataExtractProcessor.get_group_codes()

            df_taskCodes = self.__taskCodes__
            df_groupCodes = self.__groupCodes__

            df_1 = df_1.pipe(lambda df: pd.merge(df, df_taskCodes, left_on="TaskCodeId", right_on="TaskCodeId",
                                                 how="left", indicator=True)) \
                       .pipe(lambda df: df.drop(columns=['_merge']))

            df_1['ItemNumber_label'] = df_1['ItemNumber'] #df_1.apply(lambda row: extract_itemNumber(row['itemNumber']), axis=1)
            df_1['TaskCode_label'] = df_1['TaskCodeName'] #df_1.apply(lambda row: extract_taskCode(row['taskCodename']), axis=1)
            df_1['CodeGroup_label'] = df_1['GroupCodeName']  #df_1.apply(lambda row: extract_groupCode(row['taskCode']), axis=1)
            df_1['date_diff'] = df_1.apply(lambda row: calculate_date_diff(row), axis=1)


            task_dicts = {}
            layout_id_dict = {}
            for layout in np.unique(df_1.ItemNumber_label):  # np.unique(df_1.layoutId):
                task_dicts[layout] = df_1.query(f"ItemNumber_label ==  '{layout}'")['TaskCode_label']
                layout_id_dict[layout] = df_1.query(f"ItemNumber_label ==  '{layout}'")['LayoutId'].unique()[0]

            for i in range(0, len(task_dicts.keys()) - 1):
                seq_1 = [item for item in task_dicts[list(task_dicts.keys())[i]]]
                seq_2 = [item for item in task_dicts[list(task_dicts.keys())[len(task_dicts.keys()) - 1]]]

                align_seq1, align_seq2, score = hybrid_alignment(seq_1, seq_2)
                layout_id_ref = list(task_dicts.keys())[i]
                layout_id_test = list(task_dicts.keys())[len(task_dicts.keys()) - 1]

                db_layoutId_ref = layout_id_dict[layout_id_ref]
                db_layoutId_test = layout_id_dict[layout_id_test]

                print(f"LayoutId - {layout_id_ref}/{layout_id_test} \
                    \n seq1 - \n[{' => '.join(seq_1)}] \
                    \n seq2 - \n[{' => '.join(seq_2)}] \
                    \n Alignment_result - \n   {(align_seq1, align_seq2, score)}")
                print('_________________________________')
                df_summ = pd.concat(
                    [df_summ, pd.DataFrame([[db_layoutId_ref, layout_id_ref, db_layoutId_test, layout_id_test, seq_1, seq_2, align_seq1, align_seq2, score]],
                                           columns=df_summ.columns)], ignore_index=True)

        return df_1, df_summ

    @staticmethod
    def save_results(result, runDate):
        response = requests.post(url= f'http://localhost:6001/api/model/SequenceSimilarity/AddSequenceSimilarity?runDate={runDate}',
                                 json= result, verify=False)

        if response.status_code == sc.OK:
            print("ok")


# %%


# df_data,df_result = evaluate(51)
# df_result.sort_values(by= ['Score','layout_ref'], ascending=False)
