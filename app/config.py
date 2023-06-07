from pydantic import BaseSettings


class Config(BaseSettings):
    mongodb_url: str
    mongo_db: str
    products_collec: str
    services_collec: str
    stream_consume: bool
    kafka_server: str
    kafka_protocol: str
    sasl_mechanism: str
    sasl_username: str
    sasl_pass: str
    kafka_topic: str
    auto_offset_reset: str
    group_id: str
