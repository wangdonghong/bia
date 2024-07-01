from fastapi import APIRouter, Depends
from typing import List
from app.schemas.orders import Order, CreateOrder
from app.services.orders import OrdersService

router = APIRouter()

@router.get("/", response_model=List[Order])
def get_orders(service: OrdersService = Depends(OrdersService)):
    return service.get_all()

@router.post("/", response_model=Order, status_code=201)
def create_order(order: CreateOrder, service: OrdersService = Depends(OrdersService)):
    return service.create(order)