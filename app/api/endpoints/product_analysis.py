from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict, Any
from google.cloud import bigquery
from collections import defaultdict

router = APIRouter()

# Initialize the BigQuery client
client = bigquery.Client()
job_config = bigquery.QueryJobConfig(use_query_cache=True)

# Request model
class ApiRequest(BaseModel):
    start_date_today: ''
    end_date_today: ''
    start_date_yesterday: ''
    end_date_yesterday: ''
    page: int = 1
    limit: int = 50
    department_types: str = ''
    brand_department_id: int = None
    site_id: str = None
    online_time_start: str = None
    online_time_end: str = None

# Response model
class ApiResponse(BaseModel):
    dates: List[int]
    data: Dict[int, List[int]]

# Query function
def get_product_analysis(request: ApiRequest) -> Dict[str, Any]:
    # 计算偏移量
    offset = (request.page - 1) * request.limit
    # department_types_str = request.department_types
    # brand_department_id = request.brand_department_id
    # site_id = request.site_id
    # online_time_start = request.online_time_start
    # online_time_end = request.online_time_end
    # start_date_today = request.start_date_today
    # end_date_today = request.end_date_today
    # start_date_yesterday = request.start_date_yesterday
    # end_date_yesterday = request.end_date_yesterday

    # try:
    #     department_types = [int(dt.strip()) for dt in department_types_str.split(',') if dt.strip().isdigit()]
    # except ValueError:
    #     raise HTTPException(status_code=400, detail="Invalid department_types parameter")

    # department_type_condition = ""
    # if department_types:
    #     formatted_department_types = ','.join(map(str, department_types))
    #     department_type_condition = f"AND bd.department_type IN ({formatted_department_types})"

    # brand_department_condition = ""
    # if brand_department_id:
    #     brand_department_condition = f"AND s.brand_department_id = {brand_department_id}"

    # site_condition = ""
    # if site_id:
    #     try:
    #         site_ids = [int(id.strip()) for id in site_id.split(',') if id.strip().isdigit()]
    #         if site_ids:
    #             formatted_site_ids = ','.join(map(str, site_ids))
    #             site_condition = f"AND oi.site_id IN ({formatted_site_ids})"
    #     except ValueError:
    #         raise HTTPException(status_code=400, detail="Invalid site_id parameter")

    # online_time_condition = ""
    # if online_time_start and online_time_end:
    #     online_time_condition = f"AND oi.online_time BETWEEN '{online_time_start}' AND '{online_time_end}'"

    # date_today_condition = ""
    # if start_date_today and end_date_today:
    #     date_today_condition = f"AND oi.order_created_at BETWEEN '{start_date_today}' AND '{end_date_today}'"

    # date_yesterday_condition = ""
    # if start_date_yesterday and end_date_yesterday:
    #     date_yesterday_condition = f"AND oi.order_created_at BETWEEN '{start_date_yesterday}' AND '{end_date_yesterday}'"

    dates = [1, 2, 3, 4, 5]
    data = {
        1: [10, 20, 30, 40, 50],
        2: [15, 25, 35, 45, 55],
        3: [12, 22, 32, 42, 52],
    }

    return {"dates": dates, "data": dict(data)}


@router.post("/product-analysis", response_model=ApiResponse)
async def product_analysis(request: ApiRequest):
    try:
        response_data = get_product_analysis(request)
        return response_data
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))