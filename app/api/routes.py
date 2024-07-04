from fastapi import APIRouter
from app.api.endpoints import order, top_products

api_router = APIRouter()
api_router.include_router(order.router, prefix="/api", tags=["orders"])
api_router.include_router(top_products.router, prefix="/api", tags=["products"])