from pydantic import BaseModel

class Order(BaseModel):
    id: int
    customer_name: str
    total_amount: float

class CreateOrder(BaseModel):
    customer_name: str
    total_amount: float