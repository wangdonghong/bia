from typing import List
from app.schemas.orders import Order, CreateOrder

class OrdersService:
    def get_all(self) -> List[Order]:
        # Implement your logic to fetch all orders
        return [
            Order(id=1, customer_name="John Doe", total_amount=100.0),
            Order(id=2, customer_name="Jane Smith", total_amount=50.0),
            Order(id=3, customer_name="Bob Johnson", total_amount=75.0),
        ]

    def create(self, order: CreateOrder) -> Order:
        # Implement your logic to create a new order
        return Order(id=4, customer_name=order.customer_name, total_amount=order.total_amount)