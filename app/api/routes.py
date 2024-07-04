from fastapi import APIRouter
from app.api.endpoints import order

api_router = APIRouter()
api_router.include_router(order.router, prefix="/api", tags=["orders"])