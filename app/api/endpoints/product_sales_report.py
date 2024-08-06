from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from google.cloud import bigquery
from collections import defaultdict

router = APIRouter()

# Request model
class ProductSalesReportParams(BaseModel):
    product_ids: List[str]
    start_date: str
    end_date: str

# Response model
class ProductSalesReportResponse(BaseModel):
    total: int
    result: List[Dict[str, Any]]

# Query function
def query_product_sales_report(params: ProductSalesReportParams) -> Dict[str, Any]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    query = """
    WITH product_ids AS (
        SELECT id FROM UNNEST(@product_ids) AS id
    )
    SELECT 
        dd.item_date, 
        p.id AS product_id,
        COALESCE(dps.daily_purchase_quantity, 0) AS daily_purchase_quantity,
        COALESCE(dps.total_order_amount, 0) AS total_order_amount
    FROM 
        `allwebi.tb_date_dimension` AS dd
    CROSS JOIN
        product_ids p
    LEFT JOIN 
        `allwebi.mv_daily_product_sales` AS dps 
        ON dd.item_date = dps.order_date 
        AND dps.product_id = p.id
    WHERE 
        dd.type = 1
        AND dd.item_date >= @start_date 
        AND dd.item_date <= @end_date
    ORDER BY 
        dd.item_date, p.id
    """

    query_params = [
        bigquery.ArrayQueryParameter("product_ids", "STRING", params.product_ids),
        bigquery.ScalarQueryParameter("start_date", "DATE", params.start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", params.end_date)
    ]

    job_config.query_parameters = query_params

    query_job = client.query(query, job_config=job_config)
    rows = list(query_job.result())

    data_dict = defaultdict(lambda: defaultdict(list))
    for row in rows:
        data_dict["product_id"].append(row.product_id)
        data_dict["item_date"].append(row.item_date.strftime('%Y-%m-%d'))
        data_dict["daily_purchase_quantity"].append(row.daily_purchase_quantity)

    total = len(set(row.product_id for row in rows))
    data = [dict(data_dict)]

    return {
        "total": total,
        "result": data
    }

@router.post("/product-sales-report", response_model=ProductSalesReportResponse)
async def product_sales_report(params: ProductSalesReportParams):
    try:
        response_data = query_product_sales_report(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return response_data