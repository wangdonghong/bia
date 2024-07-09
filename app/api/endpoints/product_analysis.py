from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from google.cloud import bigquery
import json
import hashlib

router = APIRouter()


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

@router.post("/product_analysis")
async def index(params: QueryParams):
    return {"dates": QueryParams}
    # 获取客户端传递的日期参数和分页参数
    start_date_today = params.start_date_today
    end_date_today = params.end_date_today
    start_date_yesterday = params.start_date_yesterday
    end_date_yesterday = params.end_date_yesterday
    page = params.page
    limit = params.limit
    department_types_str = params.department_types
    brand_department_id = params.brand_department_id
    site_id = params.site_id
    online_time_start = params.online_time_start
    online_time_end = params.online_time_end

    # 计算偏移量
    offset = (page - 1) * limit

    # 设置 BigQuery 客户端
    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    try:
        department_types = [int(dt.strip()) for dt in department_types_str.split(',') if dt.strip().isdigit()]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid department_types parameter")

    department_type_condition = ""
    if department_types:
        formatted_department_types = ','.join(map(str, department_types))
        department_type_condition = f"AND bd.department_type IN ({formatted_department_types})"

    brand_department_condition = ""
    if brand_department_id:
        brand_department_condition = f"AND s.brand_department_id = {brand_department_id}"

    site_condition = ""
    if site_id:
        try:
            site_ids = [int(id.strip()) for id in site_id.split(',') if id.strip().isdigit()]
            if site_ids:
                formatted_site_ids = ','.join(map(str, site_ids))
                site_condition = f"AND oi.site_id IN ({formatted_site_ids})"
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid site_id parameter")

    online_time_condition = ""
    if online_time_start and online_time_end:
        online_time_condition = f"AND oi.online_time BETWEEN '{online_time_start}' AND '{online_time_end}'"

    date_today_condition = ""
    if start_date_today and end_date_today:
        date_today_condition = f"AND oi.order_created_at BETWEEN '{start_date_today}' AND '{end_date_today}'"

    date_yesterday_condition = ""
    if start_date_yesterday and end_date_yesterday:
        date_yesterday_condition = f"AND oi.order_created_at BETWEEN '{start_date_yesterday}' AND '{end_date_yesterday}'"

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
        LIMIT {limit} OFFSET {offset}
    """

    # 执行查询
    query_job = client.query(query, job_config=job_config)

    # 处理查询结果
    rows = [dict(row) for row in query_job]

    if rows:
        total_count = rows[0].get('total_count', 0)
    else:
        total_count = 0

    response = {
        "total": total_count,
        "result": rows
    }
