from app.processor_lib.model_processors.pip_preparation import Preparation as prep_pc
from app.processor_lib.model_processors.pip_constants import Constants
from app.processor_lib.model_processors.data_extract_processor import DataExtractProcessor as dcp
import pandas as pd
import xgboost as xgb
import joblib

SEED = 53


class Training:
    """
    Class for training models.
    """
    def __init__(self):
        """
        Initialize the Training object.
        """
        pass

    @staticmethod
    def process_training(save_result=True):
        """
        Process the training of models.

        Args:
            save_result (bool): Flag indicating whether to save the trained models.

        Returns:
            None
        """
        use_api = False
        filePath = "../data/processed/"
        save_path = "../../data/"

        train_data_df = dcp.get_training_data(Constants.train_data_url)

        prepared_df = prep_pc.feature_manipulation(train_data_df)
        prepared_df = prepared_df[prepared_df.IsItemCompleted == True]
        selected_features = prep_pc.selected_features
        prepared_df = prep_pc.get_target(prepared_df)
        prepared_df = prep_pc.drop_outliers(prepared_df)
        X, y = prep_pc.get_x_y(prepared_df)

        selected_features = list(set(selected_features).intersection(X.columns))
        X_cls = X[selected_features]

        y_cls = y.apply(lambda x: 0 if x <= 30 else 1)
        xgbc = xgb.XGBClassifier(n_jobs=-1, learning_rate=0.05, max_depth=6, scale_pos_weight=15, verbosity=1)
        classifier_model = xgbc.fit(X_cls, y_cls)

        xgbcPred = xgbc.predict(X_cls)
        y_cls_result = pd.Series(xgbcPred)
        y_cls_result.index = y.index

        X_cls["classResult"] = y_cls_result
        X_reg = X_cls[X_cls["classResult"] == 0][X["TaskDuration"] > 1]
        X_reg.drop(["classResult"], axis=1, inplace=True)
        y_reg = y[X_reg.index]

        xgbr = xgb.XGBRegressor(random_state=SEED,
                                n_estimators = 700,
                                n_jobs=-1,
                                learning_rate=0.001,
                                verbosity=1)
        regressor_model = xgbr.fit(X_reg, y_reg)

        if save_result:
            joblib.dump(classifier_model, save_path + "final_classifier.pkl")
            print("Classifier model Saved to", save_path)
            joblib.dump(regressor_model, save_path + "final_regressor.pkl")
            print("Regressor model Saved to", save_path)
