from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from google.cloud import bigquery

router = APIRouter()

# Request model
class ProductSalesSummaryParams(BaseModel):
    product_ids: List[str]
    start_date: str
    end_date: str

# Response model
class ProductSalesSummaryResponse(BaseModel):
    result: Dict[str, Any]  # Contains total_quantity and percentage

def query_product_sales_summary(params: ProductSalesSummaryParams) -> Dict[str, Any]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    query = """
    WITH product_sales AS (
        SELECT
            SUM(daily_purchase_quantity) AS total_quantity
        FROM
            `allwebi.mv_daily_product_sales`
        WHERE
            product_id IN UNNEST(@product_ids)
            AND order_date BETWEEN @start_date AND @end_date
    ),
    total_sales AS (
        SELECT
            SUM(daily_purchase_quantity) AS total_quantity
        FROM
            `allwebi.mv_daily_product_sales`
        WHERE
            order_date BETWEEN @start_date AND @end_date
    )
    SELECT
        p.total_quantity,
        CASE 
            WHEN t.total_quantity = 0 THEN 0 
            ELSE ROUND(p.total_quantity / t.total_quantity, 6) 
        END AS percentage
    FROM
        product_sales p
    CROSS JOIN total_sales t
    """

    query_params = [
        bigquery.ArrayQueryParameter("product_ids", "STRING", params.product_ids),
        bigquery.ScalarQueryParameter("start_date", "STRING", params.start_date),
        bigquery.ScalarQueryParameter("end_date", "STRING", params.end_date)
    ]

    job_config.query_parameters = query_params

    query_job = client.query(query, job_config=job_config)
    rows = list(query_job.result())

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    total_quantity = rows[0].total_quantity
    percentage = rows[0].percentage

    if not total_quantity:
        total_quantity = 0

    result = {
        "total_quantity": total_quantity,
        "percentage": percentage
    }

    return {
        "result": result
    }

@router.post("/product-sales-summary", response_model=ProductSalesSummaryResponse)
async def product_sales_summary(params: ProductSalesSummaryParams):
    try:
        response_data = query_product_sales_summary(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return response_data