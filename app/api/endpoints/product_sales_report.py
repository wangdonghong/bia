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
        COALESCE(
            CASE 
                WHEN s.currency = 'USD' THEN dps.total_order_amount 
                ELSE ROUND(dps.total_order_amount * er.rate_to_cny / er_usd.rate_to_cny, 2) 
            END, 
            0
        ) AS total_order_amount
    FROM 
        `allwebi.tb_date_dimension` AS dd
    CROSS JOIN
        product_ids p
    LEFT JOIN 
        `allwebi.mv_daily_product_sales` AS dps 
        ON dd.item_date = dps.order_date 
        AND dps.product_id = p.id
    LEFT JOIN 
        `allwebi.tb_sites` AS s ON s.site_id = dps.site_id
    LEFT JOIN 
        `allwebi.tb_exchange_rates` AS er ON s.currency = er.currency_symbol AND dps.order_month = er.exchange_date
    LEFT JOIN 
        `allwebi.tb_exchange_rates` AS er_usd ON 'USD' = er_usd.currency_symbol AND dps.order_month = er_usd.exchange_date
    WHERE 
        dd.type = 1
        AND dd.item_date >= @start_date
        AND dd.item_date <= @end_date
    ORDER BY 
        dd.item_date, p.id
    """

    query_params = [
        bigquery.ArrayQueryParameter("product_ids", "STRING", params.product_ids),
        bigquery.ScalarQueryParameter("start_date", "STRING", params.start_date),
        bigquery.ScalarQueryParameter("end_date", "STRING", params.end_date)
    ]

    job_config.query_parameters = query_params

    query_job = client.query(query, job_config=job_config)
    rows = list(query_job.result())

    data_dict = defaultdict(lambda: {"qty": [], "gmv": []})
    item_dates = []

    for row in rows:
        product_id = row.product_id
        data_dict[product_id]["qty"].append(row.daily_purchase_quantity)
        data_dict[product_id]["gmv"].append(row.total_order_amount)
        if row.item_date not in item_dates:
            item_dates.append(row.item_date)

    items = {product_id: {"qty": qty_data["qty"], "gmv": qty_data["gmv"]} for product_id, qty_data in data_dict.items()}

    result = [
        {
            "xAxis": item_dates,
            "items": [items]
        }
    ]

    total = len(items)

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