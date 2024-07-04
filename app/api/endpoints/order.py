from fastapi import APIRouter, HTTPException
from typing import List, Dict
from pydantic import BaseModel

class Order(BaseModel):
    id: int
    item_name: str
    item_price: float
    quantity: int
    total_price: float

# 固定数据
orders_db: List[Order] = [
    Order(id=1, item_name="Item A", item_price=10.0, quantity=2, total_price=20.0),
    Order(id=2, item_name="Item B", item_price=20.0, quantity=1, total_price=20.0)
]

router = APIRouter()

@router.post("/orders/", response_model=Order)
def create_order(order: Order):
    orders_db.append(order)
    return order

@router.get("/orders/", response_model=List[Order])
def read_orders(skip: int = 0, limit: int = 10):
    return orders_db[skip: skip + limit]

@router.get("/orders/{order_id}", response_model=Order)
def read_order(order_id: int):
    for order in orders_db:
        if order.id == order_id:
            return order
    raise HTTPException(status_code=404, detail="Order not found")