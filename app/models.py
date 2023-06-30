from datetime import datetime
from typing import TypedDict, Optional, List, Union

from pydantic import BaseModel, Field
from bson import ObjectId


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


class ProductModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    title: str
    list_price: float
    discount_price: float
    image: str
    stock_number: int
    brand: str
    product_id: int
    model: str
    sat_key: int
    weight: float

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class ServiceModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    name: str = Field(...)
    description: str = Field(...)
    client_price: float = Field(...)
    real_price: float = Field(...)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "name": "Mantenimiento",
                "description": "Mantenimiento preventivo y correctivo",
                "client_price": 522,
                "real_price": 200
            }
        }


class Client(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    name: str = Field(...)
    location: str = Field(...)
    email: str = Field(...)
    phone_number: int = Field(...)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class QuoterModel(BaseModel):

    id: PyObjectId
    name: str
    date: datetime
    subtotal: float
    iva: float
    total: float
    percentage_in_advance_pay: float
    revenue_percentage: float
    first_pay: float
    second_pay: float
    description: str
    client: Client
    services: Optional[List[ServiceModel]] = []
    products: Optional[List[ProductModel]] = []

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class SellModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    date: datetime = Field()
    quoter_id: str = Field()

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class DataRelation(TypedDict):
    model: BaseModel
    collection_name: str


content_acepted = Union[
    SellModel,
    QuoterModel,
    ProductModel,
    ServiceModel,
    Client
]


class MessageFormat(BaseModel):
    type: str
    content: content_acepted

    class Config:
        arbitrary_types_allowed = True
