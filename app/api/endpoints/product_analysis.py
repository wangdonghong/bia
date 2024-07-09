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
    start_date_today: str
    end_date_today: str
    start_date_yesterday: str
    end_date_yesterday: str
    page: int = 1
    limit: int = 50
    department_types: str = ''
    brand_department_id: int = None
    site_id: str = None
    online_time_start: str = None
    online_time_end: str = None

# Response model
class ApiResponse(BaseModel):
    dates: List[str]
    data: Dict[int, List[int]]

# Query function
def get_product_analysis(request: ApiRequest) -> Dict[str, Any]:
    # 计算偏移量
    offset = (request.page - 1) * request.limit


    

    return {"dates": offset}

@router.post("/product-analysis", response_model=ApiResponse)
async def product_analysis(request: ApiRequest):
    try:
        response_data = get_product_analysis(request)
        return response_data
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))