from app.main import app
import os
import sys
from fastapi.testclient import TestClient

sys.path.append(os.path.abspath('..'))

client = TestClient(app)

OPERATION_INSERT = "insert"
OPERATION_DELETE = "delete"
TABLE_NAME = "table1"
COL_NAMES = ["a", "b", "c"]
COL_TYPES = ["INTEGER", "TEXT", "TEXT"]
COL_VALUES = [1, "Backup and Restore", "2018-03-27 11:58:28.988414"]


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "OK"}


def test_should_not_accept_empty_activities():
    response = client.post(
        "/api/activities",
        headers={"X-Token": "coneofsilence"},
        json={"activities": []},
    )
    assert response.status_code == 400
    assert response.json() == {"msg": "Cannot process empty activities"}


def test_should_not_accept_missing_operation():
    response = client.post(
        "/api/activities",
        headers={"X-Token": "coneofsilence"},
        json={"activities": [{
            "operation": "",
            "table": TABLE_NAME,
            "col_names": COL_NAMES,
            "col_types": COL_TYPES,
            "col_values": COL_VALUES
        }]},
    )
    assert response.status_code == 422
    assert response.json() == {"msg": "Missing operation"}


def test_should_not_accept_missing_table():
    response = client.post(
        "/api/activities",
        headers={"X-Token": "coneofsilence"},
        json={"activities": [{
            "operation": OPERATION_INSERT,
            "table": "",
            "col_names": COL_NAMES,
            "col_types": COL_TYPES,
            "col_values": COL_VALUES
        }]},
    )
    assert response.status_code == 422
    assert response.json() == {"msg": "Missing table name"}


def test_should_not_accept_missing_old_value_on_delete_operation():
    response = client.post(
        "/api/activities",
        headers={"X-Token": "coneofsilence"},
        json={"activities": [{
            "operation": OPERATION_DELETE,
            "table": TABLE_NAME,
        }]},
    )
    assert response.status_code == 422
    assert response.json() == {"msg": "Missing old value"}
