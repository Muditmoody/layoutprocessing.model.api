from app.processor_lib.model_processors.pip_preparation import Preparation as prep_pc
from app.processor_lib.model_processors.pip_constants import Constants
from app.processor_lib.model_processors.data_extract_processor import DataExtractProcessor as dcp
import pandas as pd
import joblib
import math
from datetime import datetime

SEED = 53


class Prediction:
    """
    Class for making predictions using trained models.
    """

    def __init__(self):
        """
        Initialize the Prediction object.
        """

        pass

    @staticmethod
    def process_prediction(save_result=True):
        """
        Process the prediction using trained models.

        Args:
            save_result (bool): Flag indicating whether to save the prediction result.

        Returns:
            pandas.DataFrame: The prediction result.
        """
        use_api = True
        filePath = "../data/processed/"
        save_path = "../../data/"
        classifier_model = joblib.load(save_path + "final_classifier.pkl")
        regressor_model = joblib.load(save_path + "final_regressor.pkl")

        inference_data_df = dcp.get_inference_data(Constants.inference_data_url)
        prepared_df = prep_pc.feature_manipulation(inference_data_df)

        # Try processing training data
        prepared_df = prepared_df[prepared_df.IsItemCompleted == False]
        # Category_housing is missing for now, add later
        selected_features = prep_pc.selected_features

        X = prep_pc.get_x(prepared_df)

        X_cls = X.loc[:, X.columns.isin(selected_features)]
        for c in selected_features:
            if c not in X_cls.columns.to_list():
                X_cls[c] = False

        ## pickled model has issues with dataframes
        # xgbcPred = classifier_model.predict(X_cls)
        xgbcPred = classifier_model.predict(X_cls.values)

        y_cls_result = pd.Series(xgbcPred)
        y_cls_result.index = X.index

        X_cls["predictionResult"] = y_cls_result
        result_df = X_cls.copy()
        result_df['predictionResult'] = result_df['predictionResult'].replace(1, -1)

        X_reg = X_cls[X_cls["predictionResult"] == 0]  # run regression for tasks that can be completed in 30 days
        X_reg.drop(["predictionResult"], axis=1, inplace=True)
        # xgbrPred = regressor_model.predict(X_reg)
        xgbrPred = regressor_model.predict(X_reg.values)

        # modify the predictionResult
        result_df.iloc[X_reg.index, -1] = xgbrPred
        result_df['predictionResult'] = result_df['predictionResult'].apply(lambda x: math.ceil(x))
        
        pred_output = inference_data_df[['Id']].rename(columns={"Id": "ModelDataInputId"})
        pred_output['predictionResult'] = result_df['predictionResult']

        dateFormat = "%Y-%m-%dT%H:%M:%S"
        runDate = datetime.now().strftime(dateFormat)

        # store the predicted result
        if save_result:
            if use_api:
                url = f"https://localhost:5001/api/model/ModelDataProcessed/SaveTaskDurationResult?deleteExisting=False&runDate={runDate}"
                dcp.save_results(request_url=url, result=pred_output)
            else:
                result_df.to_csv(save_path + "pip_prediction_result.csv")
                print("Prediction Result Saved to", save_path + "pip_prediction_result.csv")

        return result_df
