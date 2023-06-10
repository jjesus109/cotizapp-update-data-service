import asyncio
import logging

from app.config import Config
from app.connections import (
    create_connection,
    create_consumer,
    set_logger,
)
from app.business import consumer_logic
from app.errors import MessageProcessingError

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
        if msg.error():
            log.error(f"Error while consuming messages: {msg.error()}")
        try:
            await consumer_logic(nosql_conn, msg)
        except MessageProcessingError as e:
            log.error(e)
            continue

if __name__ == "__main__":
    asyncio.run(main())
