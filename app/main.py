from fastapi import FastAPI

from app.api import create_router

app = FastAPI()

app.include_router(create_router())