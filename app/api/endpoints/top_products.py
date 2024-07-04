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
class TopProductsRequest(BaseModel):
    start_date: str
    end_date: str
    limit: int = 100

# Response model
class TopProductsResponse(BaseModel):
    dates: List[str]
    data: Dict[int, List[int]]

# Query function
def get_top_products(start_date: str, end_date: str, limit: int) -> Dict[str, Any]:
    query = f"""
    WITH FilteredProducts AS (
      SELECT 
        product_id,
        SUM(daily_purchase_quantity) AS total_purchase_quantity
      FROM 
        `allwebi.mv_daily_product_sales`
      WHERE 
        order_date BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY 
        product_id
      ORDER BY 
        total_purchase_quantity DESC
      LIMIT {limit}
    )
    SELECT
      dd.item_date,
      fp.product_id,
      COALESCE(SUM(dps.daily_purchase_quantity), 0) AS total_quantity
    FROM
      FilteredProducts fp
    CROSS JOIN
      `allwebi.tb_date_dimension` AS dd
    LEFT JOIN
      `allwebi.mv_daily_product_sales` AS dps
    ON
      dd.item_date = dps.order_date
      AND fp.product_id = dps.product_id
    WHERE
      dd.item_date BETWEEN '{start_date}' AND '{end_date}'
      AND dd.type = 1  # Add the condition here
    GROUP BY
      dd.item_date, fp.product_id
    ORDER BY
      dd.item_date, fp.product_id
    """
    query_job = client.query(query, job_config=job_config)
    results = list(query_job.result())  # Convert the result iterator to a list

    dates = sorted({row['item_date'] for row in results})
    data = defaultdict(lambda: [0] * len(dates))

    date_index = {date: idx for idx, date in enumerate(dates)}

    for row in results:
        item_date = row['item_date']
        product_id = row['product_id']
        total_quantity = row['total_quantity']

        data[product_id][date_index[item_date]] = total_quantity

    return {"dates": dates, "data": dict(data)}

@router.post("/top-products", response_model=TopProductsResponse)
async def top_products(request: TopProductsRequest):
    try:
        response_data = get_top_products(request.start_date, request.end_date, request.limit)
        return response_data
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))