from fastapi import APIRouter

router = APIRouter()

@router.get("/orders/{order_id}")
async def get_order(order_id: int):
    # 这里可以编写获取订单信息的逻辑，比如从数据库中查询订单信息
    order = {"order_id": order_id, "product": "Example Product", "quantity": 1}
    return order