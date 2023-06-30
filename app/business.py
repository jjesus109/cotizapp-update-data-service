import json
import logging

from app.errors import (
    MessageProcessingError,
    DBConnectionError,
    InsertionError,
    ElementNotFoundError,
)
from app.repository import (
    insert_data,
    update_data,
    query_data,

)
from app.connections import TYPED_MODEL

from motor.motor_asyncio import AsyncIOMotorDatabase


log = logging.getLogger(__name__)


async def process_message(nosql_conn: AsyncIOMotorDatabase, msg: bytes):
    try:
        decoded_data = json.loads(msg.value().decode('utf-8'))
    except Exception as e:
        log.error(f"Could not decode message: {msg.value()} due to: {e}")
        raise MessageProcessingError(
            f"Could not decode message: {msg.value()} due to: {e}"
        )
    type = decoded_data.get("type")
    data = TYPED_MODEL.get(type)
    if not data:
        log.error(f"Could not find something to process: {type}")
        raise MessageProcessingError(
            f"Could not find something to process: {type}"
        )
    model = data.get("model")
    collection_name = data.get("collection_name")
    data_got = model(**decoded_data.get("content"))
    try:
        await query_data(
            nosql_conn,
            data_got.id.__str__(),
            collection_name
        )
        log.info("Element found in DB")
    except (ElementNotFoundError, DBConnectionError):
        log.info("Could not find this element in DB")
        try:
            await insert_data(
                nosql_conn,
                data_got,
                collection_name
            )
            log.info("Element inserted!")
        except InsertionError:
            log.info("Could not insert this element in DB")
        except Exception as e:
            log.info(
                f"There was a problem inserting this element in DB: {e}"
            )
        return
    except Exception as e:
        log.error(f"Could not get data from DB due to: {e}")
        raise MessageProcessingError(
            f"Could not get data from DB due to: {e}"
        )
    try:
        await update_data(
            nosql_conn,
            data_got.id.__str__(),
            data_got,
            collection_name
        )
    except InsertionError:
        log.error(
            f"Could not update element id: {data_got.id} in DB"
        )
    log.info("Element updated!")
