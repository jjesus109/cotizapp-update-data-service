from typing import Any

from app.config import Config
from app.errors import DBConnectionError, InsertionError, ElementNotFoundError

from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import (
    ConnectionFailure,
    ExecutionTimeout,
)

EMPTY_COUNT = 0
ELEMENT_FOUND = 1
conf = Config()

async def insert_data(connection: AsyncIOMotorDatabase, data: BaseModel, collection_name: str):
    data_dict = data.dict()
    data_dict["_id"] = str(data_dict["id"])
    data_dict.pop("id")
    try:
        await connection[collection_name].insert_one(data_dict)
    except (ConnectionFailure, ExecutionTimeout) as e:
        raise InsertionError("Could not insert element in DB")

async def update_data(connection: AsyncIOMotorDatabase, data_id: str, data: Any, collection_name: str):
    query = {"_id": data_id}
    data_dict = data.dict()
    data_dict.pop("id")
    values = {
        "$set": data_dict
    }
    try:
        await connection[collection_name].update_one(query, values)
    except (ConnectionFailure, ExecutionTimeout):
        raise InsertionError("Could not update element in DB")


async def query_data(connection: AsyncIOMotorDatabase, data_id: str, collection_name: str):
    query = {"_id": data_id}
    try:
        element = await connection[collection_name].find(
            query
        ).to_list(ELEMENT_FOUND)
    except (ConnectionFailure, ExecutionTimeout):
        raise DBConnectionError(
            "Element not found in DB"
        )
    if element.__len__() == EMPTY_COUNT:
        raise ElementNotFoundError(
            "Element not found in DB"
        )
    return element[0]

