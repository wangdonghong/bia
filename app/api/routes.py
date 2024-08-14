from fastapi import APIRouter
from app.api.endpoints import order, top_products, product_analysis, daily_product_report, product_sales_analysis_spu, product_sales_report, product_sales_summary

api_router = APIRouter()
api_router.include_router(order.router, prefix="/api", tags=["orders"])
api_router.include_router(top_products.router, prefix="/api", tags=["products"])
api_router.include_router(product_analysis.router, prefix="/api", tags=["analysis"])
api_router.include_router(daily_product_report.router, prefix="/api", tags=["daily_product_report"])
api_router.include_router(product_sales_analysis_spu.router, prefix="/api", tags=["product_sales_analysis_spu"])
api_router.include_router(product_sales_report.router, prefix="/api", tags=["product_sales_report"])
api_router.include_router(product_sales_summary.router, prefix="/api", tags=["product_sales_summary"])