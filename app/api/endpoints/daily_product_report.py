from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from datetime import date, datetime

router = APIRouter()

# Request model
class DailyProductReportParams(BaseModel):
    page: int = 1
    limit: int = 50
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    online_start_date: Optional[datetime] = None
    online_end_date: Optional[datetime] = None
    site_ids: Optional[str] = None  # 新增：逗号分隔的site_id字符串

# Response model
class DailyProductReportResponse(BaseModel):
    total_records: int
    result: List[Dict[str, Any]]

# Query function
def query_daily_product_report(params: DailyProductReportParams) -> Dict[str, Any]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    # Calculate offset
    offset = (params.page - 1) * params.limit

    # Base query
    base_query = """
        WITH main_query AS (
            SELECT 
                dps.product_id AS spu,
                dps.order_date,
                dps.order_month,
                dps.daily_purchase_quantity AS total_purchase_quantity,
                ROUND(dps.total_order_amount, 2) AS total_order_amount_original,
                COALESCE(NULLIF(dps.latest_online_time, ''), '-') AS online_time,
                CONCAT(IF(INSTR(dps.link, 'https') > 0, '', 'https://'), dps.link) AS link,
                g.main_image AS product_img,
                g.title AS product_title,
                s.brand AS site_name,
                s.site_type,
                s.currency,
                bd.department_name,
                '-' AS marketing_expenses,
                '-' AS procurement_ratio,
                '-' AS refund_ratio,
                CASE 
                    WHEN s.currency = 'USD' THEN dps.total_order_amount 
                    ELSE ROUND(dps.total_order_amount * er.rate_to_cny / er_usd.rate_to_cny, 2) 
                END AS total_order_amount
            FROM 
                `allwebi.vw_daily_product_sales` AS dps 
            LEFT JOIN 
                `allwebi.tb_sites` AS s 
                ON dps.site_id = s.site_id 
            LEFT JOIN 
                `allwebi.tb_brand_department` AS bd 
                ON s.brand_department_id = bd.id 
            LEFT JOIN 
                `allwebi.tb_goods` AS g 
                ON dps.product_id = g.p_id 
            LEFT JOIN 
                `allwebi.tb_exchange_rates` AS er  -- add this join
                ON s.currency = er.currency_symbol AND dps.order_month = er.exchange_date 
            LEFT JOIN 
                `allwebi.tb_exchange_rates` AS er_usd  -- add this join
                ON 'USD' = er_usd.currency_symbol AND dps.order_month = er_usd.exchange_date 
            {where_clause}
            ORDER BY 
                dps.order_date DESC,
                dps.total_order_amount DESC
        )
        SELECT *, (SELECT COUNT(*) FROM main_query) AS total_records
        FROM main_query
        LIMIT {limit} OFFSET {offset}
    """

    # Construct WHERE clause
    where_conditions = []
    if params.start_date:
        where_conditions.append("CAST(dps.order_date AS DATE) >= @start_date")
    if params.end_date:
        where_conditions.append("CAST(dps.order_date AS DATE) <= @end_date")
    if params.online_start_date:
        where_conditions.append("""
            (dps.latest_online_time != '' AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dps.latest_online_time) IS NOT NULL AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dps.latest_online_time) >= @online_start_date)
        """)
    if params.online_end_date:
        where_conditions.append("""
            (dps.latest_online_time != '' AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dps.latest_online_time) IS NOT NULL AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dps.latest_online_time) <= @online_end_date)
        """)
    if params.site_ids:
        site_ids = [id.strip() for id in params.site_ids.split(',')]
        where_conditions.append(f"dps.site_id IN UNNEST(@site_ids)")
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # Format the query with the WHERE clause and pagination
    query = base_query.format(
        where_clause=where_clause,
        limit=params.limit,
        offset=offset
    )

    # Set up query parameters
    query_params = []
    if params.start_date:
        query_params.append(bigquery.ScalarQueryParameter("start_date", "DATE", params.start_date))
    if params.end_date:
        query_params.append(bigquery.ScalarQueryParameter("end_date", "DATE", params.end_date))
    if params.online_start_date:
        query_params.append(bigquery.ScalarQueryParameter("online_start_date", "TIMESTAMP", params.online_start_date))
    if params.online_end_date:
        query_params.append(bigquery.ScalarQueryParameter("online_end_date", "TIMESTAMP", params.online_end_date))
    if params.site_ids:
        site_ids = [int(id.strip()) for id in params.site_ids.split(',')]
        query_params.append(bigquery.ArrayQueryParameter("site_ids", "INT64", site_ids))

    job_config.query_parameters = query_params

    # Execute the query
    query_job = client.query(query, job_config=job_config)

    # Process the query results
    rows = [dict(row) for row in query_job]

    if rows:
        total_records = rows[0].get('total_records', 0)
    else:
        total_records = 0

    return {
        "total_records": total_records,
        "result": rows
    }

@router.post("/daily-product-report", response_model=DailyProductReportResponse)
async def daily_product_report(params: DailyProductReportParams):
    try:
        response_data = query_daily_product_report(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return response_data