from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from google.cloud import bigquery

router = APIRouter()

# Request model
class ProductSalesReportParams(BaseModel):
    product_ids: List[str]
    start_date: str
    end_date: str

# Response model
class ProductSalesReportResponse(BaseModel):
    total: int
    result: Dict[str, Any]  # 这里是一个字典，包含 xAxis、qty_data 和 gmv_data

# 结果字典的结构
class ResultData(BaseModel):
    xAxis: List[str]
    qty_data: List[int]
    gmv_data: List[float]

def query_product_sales_report(params: ProductSalesReportParams) -> Dict[str, Any]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    query = """
    SELECT 
        dd.item_date,
        COALESCE(SUM(dps.daily_purchase_quantity), 0) AS daily_purchase_quantity,
        COALESCE(SUM(
            CASE 
                WHEN s.currency = 'USD' THEN dps.total_order_amount 
                ELSE ROUND(dps.total_order_amount * er.rate_to_cny / er_usd.rate_to_cny, 2) 
            END
        ), 0) AS total_order_amount
    FROM 
        `allwebi.tb_date_dimension` AS dd
    LEFT JOIN 
        `allwebi.mv_daily_product_sales` AS dps
        ON dd.item_date = dps.order_date
    LEFT JOIN 
        `allwebi.tb_sites` AS s ON s.site_id = dps.site_id
    LEFT JOIN 
        `allwebi.tb_exchange_rates` AS er ON s.currency = er.currency_symbol AND dps.order_month = er.exchange_date
    LEFT JOIN 
        `allwebi.tb_exchange_rates` AS er_usd ON 'USD' = er_usd.currency_symbol AND dps.order_month = er_usd.exchange_date
    WHERE 
        dd.type = 1
        AND dps.product_id IN UNNEST(@product_ids)
        AND dd.item_date >= @start_date
        AND dd.item_date <= @end_date
    GROUP BY 
        dd.item_date
    ORDER BY 
        dd.item_date
    """

    query_params = [
        bigquery.ArrayQueryParameter("product_ids", "STRING", params.product_ids),
        bigquery.ScalarQueryParameter("start_date", "STRING", params.start_date),
        bigquery.ScalarQueryParameter("end_date", "STRING", params.end_date)
    ]

    job_config.query_parameters = query_params

    query_job = client.query(query, job_config=job_config)
    rows = list(query_job.result())

    item_dates = []
    qty_data = []
    gmv_data = []

    for row in rows:
        item_dates.append(row.item_date)
        qty_data.append(row.daily_purchase_quantity)
        gmv_data.append(row.total_order_amount)

    result = {
        "xAxis": item_dates,
        "qty_data": qty_data,
        "gmv_data": gmv_data
    }

    total = len(item_dates)

    return {
        "total": total,
        "result": result
    }

@router.post("/product-sales-report", response_model=ProductSalesReportResponse)
async def product_sales_report(params: ProductSalesReportParams):
    try:
        response_data = query_product_sales_report(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return response_data