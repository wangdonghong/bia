from fastapi import FastAPI
from app.api.v1.endpoints import orders

app = FastAPI(title="BI Project API", version="1.0.0")

app.include_router(orders.router, prefix="/api/v1/orders", tags=["orders"])