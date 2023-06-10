import logging

from app.config import Config
from app.errors import DBConnectionError
from app.models import (
    ServiceModel,
    ProductModel,
    DataRelation,
    QuoterModel,
    SellModel
)

from motor.motor_asyncio import AsyncIOMotorClient
from motor.motor_asyncio import AsyncIOMotorDatabase
from confluent_kafka import Consumer
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
)

conf = Config()


def create_connection() -> AsyncIOMotorDatabase:
    url_connection = conf.mongodb_url
    database_name = conf.mongo_db
    try:
        client = AsyncIOMotorClient(url_connection)
    except (ConfigurationError, ConnectionFailure) as e:
        raise DBConnectionError(
            f"Could not connect to database due to: {e}"
        )
    return client[database_name]


def create_consumer() -> Consumer:

    kafka_conf = {
        "bootstrap.servers": conf.kafka_server,
        "security.protocol": conf.kafka_protocol,
        "sasl.mechanisms": conf.sasl_mechanism,
        "sasl.username": conf.sasl_username,
        "sasl.password": conf.sasl_pass,
        "group.id": conf.group_id,
        "auto.offset.reset": conf.auto_offset_reset,
    }
    return Consumer(kafka_conf)


def set_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s: %(message)s'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)


TYPED_MODEL = {
    "Service": DataRelation(
        model=ServiceModel,
        collection_name=conf.services_collec
    ),
    "Product": DataRelation(
        model=ProductModel,
        collection_name=conf.products_collec
    ),
    "Quoter": DataRelation(
        model=QuoterModel,
        collection_name=conf.quoters_collec
    ),
    "Sale": DataRelation(
        model=SellModel,
        collection_name=conf.sales_collec
    )
}
