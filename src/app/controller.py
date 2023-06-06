from fastapi import APIRouter
from typing import Union
from pydantic import BaseModel
from app.processor_lib.model_processors import pip_prediction_model as predict_pc, pip_model_training as train_pc, \
    pip_cleaning as clean_pc, sequence_processor as seq_pc
import logging
import json
import app.processor_lib.file_processor as rf_proc

logger = logging.getLogger(__name__)

api_router = APIRouter()


class itemType(BaseModel):
    """
    BaseModel class representing an item type.

    Attributes:
        a (int): An integer attribute.
        b (str): A string attribute.
    """
    a: int
    b: str


@api_router.post("/similarity/processSequenceSimilarity", status_code=200, response_model=object)
def process_sequence_similarity():
    """
    Endpoint for processing sequence similarity.

    Returns:
        str: Response indicating the success of the operation.
    """
    results = json.loads(seq_pc.SequenceProcessor.get_similarity().to_json(orient="records"))
    return "Ok"
    # seq_pc.SequenceProcessor.save_results(results)


@api_router.get("/similarity/getSequenceSimilarity/NotificationRef", status_code=200, response_model=object)
def get_sequence_simrilarity(notif_ref):
    """
    Endpoint for getting sequence similarity based on a notification reference.

    Args:
        notif_ref: Notification reference.

    Returns:
        object: JSON object representing the sequence similarity results.
    """

    results = json.loads(seq_pc.SequenceProcessor.get_similarity_byRef(notif_ref).to_json(orient="records"))
    return results
    # seq_pc.SequenceProcessor.save_results(results)


@api_router.post("/dataPrep/CleanData", status_code=200, response_model=object)
def process_clean_data():
    """
    Endpoint for processing data cleaning.

    Returns:
        str: Response indicating the success of the operation.
    """
    clean_pc.Cleaning.process_cleaning()
    return "Ok"
    # seq_pc.SequenceProcessor.save_results(results)


@api_router.post("/modelling/TrainModel", status_code=200, response_model=object)
def process_train_model():
    """
    Endpoint for processing model training.

    Returns:
        str: Response indicating the success of the operation.
    """
    train_pc.Training().process_training()
    # results =  json.loads(seq_pc.SequenceProcessor.get_similarity_byRef(notif_ref).to_json(orient="records"))
    return "Ok"


@api_router.post("/modelling/ModelPredict", status_code=200, response_model=object)
def process_model_predict():
    """
    Endpoint for processing model prediction.

    Returns:
        str: Response indicating the success of the operation.
    """

    results = json.loads(predict_pc.Prediction.process_prediction().to_json(orient="records"))
    return "Ok"


@api_router.post("/etl/processRawFile", status_code=200, response_model=object)
def process_raw_file():
    """
    Endpoint for processing a raw file.

    Returns:
        str: Response indicating the success of the operation.
    """
    results = rf_proc.process_raw_file()
    return "Ok"


@api_router.post("/etl/processFileImport", status_code=200, response_model=object)
def process_file_import():
    """
    Endpoint for processing file import.

    Returns:
        str: Response indicating the success of the operation.
    """
    results = rf_proc.process_file_import()
    return "Ok"