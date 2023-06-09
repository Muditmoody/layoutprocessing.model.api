import uvicorn
from typing import Union
from fastapi import FastAPI, APIRouter, Query, Response
import processor_lib as pc
import controller
import requests
import pandas as pd
from http import HTTPStatus as sc
import logging


logger = logging.getLogger(__name__)
app = FastAPI() #openapi_url="/openapi.json"

api_router = APIRouter()
api_router.include_router(controller.api_router, prefix="/processor/controller", tags=[],include_in_schema=True)
app.include_router(api_router, prefix="/api")

@app.get("/")
async def root():
    """
    Root endpoint.

    Returns:
        dict: Response containing a message.
    """
    return {"message": "Hey"}


if __name__ == '__main__':
    """
    Main entry point of the application.
    """
    uvicorn.run(app, host="localhost", port="6060")
