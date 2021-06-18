from pydantic import BaseModel
from fastapi import FastAPI, Response, status
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
    old_value: Optional[OldValue] = None


class Payload(BaseModel):
    activities: List[Activity]


@app.get("/")
def home():
    return {"msg": "OK"}


@app.post("/api/activities")
async def api(payload: Payload, response: Response):
    payload_json = jsonable_encoder(payload)

    if not payload_json["activities"]:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"msg": "Cannot process empty activities"}
    
    for activity in payload_json["activities"]:
        if not activity["operation"]:
            response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
            return {"msg": "Missing operation"}
        if not activity["table"]:
            response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
            return {"msg": "Missing table name"}
        if activity["operation"] == "delete" and activity["old_value"] == None:
            response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
            return {"msg": "Missing old value"}
        

    payload_string = json.dumps(payload_json).encode('utf-8')
    publish(payload_string)
    
    return payload_json
