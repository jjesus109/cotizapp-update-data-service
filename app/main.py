import json
import asyncio
import logging

from app.config import Config
from app.connections import (
    create_connection,
    create_consumer,
    set_logger,
    TYPED_MODEL
)
from app.repository import (
    insert_data,
    update_data,
    query_data,

)
from app.errors import (
    DBConnectionError,
    InsertionError,
    ElementNotFoundError,
)

conf = Config()


async def main():
    set_logger()
    log = logging.getLogger(__name__)
    log.info("Creating connections")
    consumer = create_consumer()
    nosql_conn = create_connection()
    consumer.subscribe([conf.kafka_topic])
    log.info("Ready to receive messages")

    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error() is not None:
            continue
        try:
            decoded_data = json.loads(msg.value().decode('utf-8'))
        except:
            log.error(f"Could not decode message: {msg.value()}")
            continue
        type = decoded_data.get("type")
        data = TYPED_MODEL.get(type)
        if not data:
            log.error(f"Could not find something to process: {type}")
            continue
        model = data.get("model")
        collection_name = data.get("collection_name")
        data_got = model.parse_obj(decoded_data.get("content"))
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
            continue
        except Exception as e:
            log.error(f"Could not get data from DB due to: {e}")
            continue
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


import time

def on_exit(sig, func=None):
    print("exit handler")
    time.sleep(10)  # so you can see the message before program exits

import signal
signal.signal(signal.SIGTERM, on_exit)
    
if __name__ == "__main__":
    asyncio.run(main())