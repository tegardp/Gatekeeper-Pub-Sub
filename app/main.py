from pydantic import BaseModel
from fastapi import FastAPI
from typing import List, Optional

import json
from fastapi.encoders import jsonable_encoder
import os, sys
sys.path.append(os.path.abspath('..'))
from pubsub.publisher import publish


app = FastAPI()


class OldValue(BaseModel):
    col_names: List[str]
    col_types: List[str]
    col_values: List


class Activity(BaseModel):
    operation: str
    table: str
    col_names: Optional[List[str]] = None
    col_types: Optional[List[str]] = None
    col_values: Optional[List] = None
    old_values: Optional[OldValue] = None


class Payload(BaseModel):
    activities: List[Activity]


@app.get("/")
def home():
    return {"status": 200}


@app.post("/api/activities")
async def api(payload: Payload):
    payload_json = jsonable_encoder(payload)
    payload_string = json.dumps(payload_json).encode('utf-8')
    publish(payload_string)

    return payload_json
