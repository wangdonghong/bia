from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from google.cloud import bigquery


router = APIRouter()


# Request model
class QueryParams(BaseModel):
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
class QueryResponse(BaseModel):
    total: int
    result: List[Dict[str, Any]]
    sql_query: str

# Query function
def query_bigquery(params: QueryParams) -> Dict[str, Any]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    try:
        department_types = [int(dt.strip()) for dt in params.department_types.split(',') if dt.strip().isdigit()]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid department_types parameter")

    department_type_condition = ""
    if department_types:
        formatted_department_types = ','.join(map(str, department_types))
        department_type_condition = f"AND bd.department_type IN ({formatted_department_types})"

    brand_department_condition = ""
    if params.brand_department_id:
        brand_department_condition = f"AND s.brand_department_id = {params.brand_department_id}"

    site_condition = ""
    if params.site_id:
        try:
            site_ids = [int(id.strip()) for id in params.site_id.split(',') if id.strip().isdigit()]
            if site_ids:
                formatted_site_ids = ','.join(map(str, site_ids))
                site_condition = f"AND oi.site_id IN ({formatted_site_ids})"
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid site_id parameter")

    online_time_condition = ""
    if params.online_time_start and params.online_time_end:
        online_time_condition = f"AND oi.online_time BETWEEN '{params.online_time_start}' AND '{params.online_time_end}'"

    date_today_condition = ""
    if params.start_date_today and params.end_date_today:
        date_today_condition = f"AND oi.order_created_at BETWEEN '{params.start_date_today}' AND '{params.end_date_today}'"

    date_yesterday_condition = ""
    if params.start_date_yesterday and params.end_date_yesterday:
        date_yesterday_condition = f"AND oi.order_created_at BETWEEN '{params.start_date_yesterday}' AND '{params.end_date_yesterday}'"

    # 定义 BigQuery 查询
    query = f"""
        WITH today AS (
            SELECT 
                oi.sku,
                MAX(oi.title) AS product_title,
                MAX(oi.link) AS link,
                MAX(oi.image_url) AS product_img,
                MAX(oi.online_time) AS online_time,
                ROUND(SUM(oi.price), 2) AS total_order_amount_today,
                oi.site_id,
                SUM(oi.quantity) AS total_purchase_quantity_today,
                MAX(s.brand) AS site_name,
                MAX(bd.department_name) AS department_name,
                MAX(oi.original_price) AS original_price,
                MAX(oipp.purchase_price) AS purchase_price,
                MAX(oipp.buyer) AS buyer,
                MAX(s.currency) AS currency
            FROM `allwebi.tb_order_items` as oi
            LEFT JOIN `allwebi.tb_order_item_purchase_price` as oipp ON oi.id = oipp.order_item_id
            LEFT JOIN `allwebi.tb_sites` as s ON oi.site_id = s.site_id
            LEFT JOIN `allwebi.tb_brand_department` as bd ON s.brand_department_id = bd.id
            WHERE 1=1 
            {date_today_condition}
            {department_type_condition}
            {brand_department_condition}
            {site_condition}
            {online_time_condition}
            GROUP BY oi.sku, oi.site_id
        ),
        yesterday AS (
            SELECT 
                oi.sku,
                oi.site_id,
                ROUND(SUM(oi.price), 2) AS total_order_amount_yesterday,
                SUM(oi.quantity) AS total_purchase_quantity_yesterday
            FROM `allwebi.tb_order_items` as oi
            WHERE 1=1 
            {date_yesterday_condition}
            {site_condition}
            {online_time_condition}
            GROUP BY oi.sku, oi.site_id
        ),
        results AS (
            SELECT
                today.sku as spu,
                today.product_title,
                today.link,
                today.online_time,
                today.total_order_amount_today as total_order_amount,
                today.site_id,
                today.total_purchase_quantity_today as total_purchase_quantity,
                yesterday.total_purchase_quantity_yesterday as sales_growth_rate,
                yesterday.total_order_amount_yesterday as revenue_growth_rate,
                today.site_name,
                today.product_img,
                today.department_name,
                today.original_price,
                today.purchase_price,
                today.buyer,
                CASE
                    WHEN today.purchase_price = 0 or today.original_price = 0 THEN '-'
                    ELSE CAST(ROUND(today.original_price * er.rate_to_cny / (today.purchase_price + 4.16), 2) AS STRING)
                END AS product_multiplier,
                CASE
                    WHEN yesterday.total_order_amount_yesterday IS NOT NULL AND yesterday.total_order_amount_yesterday != 0
                    THEN ROUND(((today.total_order_amount_today - yesterday.total_order_amount_yesterday) / yesterday.total_order_amount_yesterday) * 100, 2)
                    ELSE NULL
                END AS sales_growth_rate_b,
                er.rate_to_cny
            FROM today
            LEFT JOIN yesterday
            ON today.sku = yesterday.sku and today.site_id = yesterday.site_id
            LEFT JOIN `allwebi.tb_exchange_rates` AS er
            ON today.currency = er.currency_symbol AND FORMAT_TIMESTAMP('%Y-%m', CURRENT_TIMESTAMP()) = er.exchange_date
        )
        SELECT 
            *,
            (SELECT COUNT(*) FROM results) AS total_count
        FROM results
        ORDER BY total_purchase_quantity DESC
        LIMIT {params.limit} OFFSET {(params.page - 1) * params.limit}
    """

    query_job = client.query(query, job_config=job_config)

    # 处理查询结果
    rows = [dict(row) for row in query_job]

    if rows:
        total_count = rows[0].get('total_count', 0)
    else:
        total_count = 0

    return {
        "total": total_count,
        "result": rows,
        "sql_query": query
    }

@router.post("/product-analysis", response_model=QueryResponse)
async def index(params: QueryParams):
    # 执行查询
    try:
        response_data = query_bigquery(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


    return response_data