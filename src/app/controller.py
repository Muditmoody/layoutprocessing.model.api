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
    a: int
    b: str


@api_router.post("/similarity/processSequenceSimilarity", status_code=200, response_model=object)
def process_sequence_similarity():
    results = json.loads(seq_pc.SequenceProcessor.get_similarity().to_json(orient="records"))
    return "Ok"
    # seq_pc.SequenceProcessor.save_results(results)


@api_router.get("/similarity/getSequenceSimilarity/NotificationRef", status_code=200, response_model=object)
def get_sequence_simrilarity(notif_ref):
    results = json.loads(seq_pc.SequenceProcessor.get_similarity_byRef(notif_ref).to_json(orient="records"))
    return results
    # seq_pc.SequenceProcessor.save_results(results)


@api_router.post("/dataPrep/CleanData", status_code=200, response_model=object)
def process_clean_data():
    clean_pc.Cleaning.process_cleaning()
    return "Ok"
    # seq_pc.SequenceProcessor.save_results(results)


@api_router.post("/modelling/TrainModel", status_code=200, response_model=object)
def process_train_model():
    train_pc.Training().process_training()
    # results =  json.loads(seq_pc.SequenceProcessor.get_similarity_byRef(notif_ref).to_json(orient="records"))
    return "Ok"


@api_router.post("/modelling/ModelPredict", status_code=200, response_model=object)
def process_model_predict():
    results = json.loads(predict_pc.Prediction.process_prediction().to_json(orient="records"))
    return "Ok"


@api_router.post("/etl/processRawFile", status_code=200, response_model=object)
def process_raw_file():
    results = rf_proc.process_raw_file()
    return "Ok"


@api_router.post("/etl/processFileImport", status_code=200, response_model=object)
def process_file_import():
    results = rf_proc.process_file_import()
    return "Ok"