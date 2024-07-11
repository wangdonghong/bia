from fastapi import APIRouter
from app.api.endpoints import order, top_products, product_analysis, daily_product_report

api_router = APIRouter()
api_router.include_router(order.router, prefix="/api", tags=["orders"])
api_router.include_router(top_products.router, prefix="/api", tags=["products"])
api_router.include_router(product_analysis.router, prefix="/api", tags=["analysis"])
api_router.include_router(daily_product_report.router, prefix="/api", tags=["daily_product_report"])