from fastapi import APIRouter

from app.api.endpoints import example, order  # 导入新创建的 order 模块

def create_router():
    router = APIRouter()
    router.include_router(example.router)
    router.include_router(order.router)  # 挂载 order 路由
    return router