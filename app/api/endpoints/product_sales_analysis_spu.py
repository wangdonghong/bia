from fastapi import APIRouter, HTTPException, FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from datetime import date, datetime

router = APIRouter()

# 请求模型
class DailyProductReportParams(BaseModel):
    page: int = 1
    limit: int = 50
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    online_start_date: Optional[datetime] = None
    online_end_date: Optional[datetime] = None
    site_ids: Optional[str] = None  # 新增：逗号分隔的site_id字符串

# 响应模型
class DailyProductReportResponse(BaseModel):
    total: int
    result: List[Dict[str, Any]]

# 查询函数
def query_product_sales_analysis_spu(params: DailyProductReportParams) -> Dict[str, Any]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    # 计算偏移量
    offset = (params.page - 1) * params.limit

    # 基础查询
    base_query = """
        WITH total_sales AS (
            SELECT 
                SUM(CASE 
                      WHEN s.currency = 'USD' THEN dps1.total_order_amount 
                      ELSE ROUND(dps1.total_order_amount * er.rate_to_cny / er_usd.rate_to_cny, 2) 
                    END) AS total_sales,
                SUM(dps1.daily_purchase_quantity) AS total_quantity
            FROM 
                `allwebi.mv_daily_product_sales` AS dps1
            LEFT JOIN 
                `allwebi.tb_sites` AS s ON s.site_id = dps1.site_id
            LEFT JOIN 
                `allwebi.tb_exchange_rates` AS er ON s.currency = er.currency_symbol AND dps1.order_month = er.exchange_date
            LEFT JOIN 
                `allwebi.tb_exchange_rates` AS er_usd ON 'USD' = er_usd.currency_symbol AND dps1.order_month = er_usd.exchange_date
        ),
        main_query AS (
            SELECT 
                dps.product_id, 
                dps.site_id,
                '' as tags,
                SUM(dps.daily_purchase_quantity) AS total_daily_purchase_quantity,
                SUM(CASE 
                      WHEN s.currency = 'USD' THEN dps.total_order_amount 
                      ELSE ROUND(dps.total_order_amount * er.rate_to_cny / er_usd.rate_to_cny, 2) 
                    END) AS total_order_amount_usd,
                SUM(dps.total_order_amount) as total_order_amount,
                MAX(s.brand) AS site_name,
                MAX(bd.department_name) AS department_name,
                MAX(title) AS product_title,
                MAX(image_url) AS product_img,
                FORMAT('%.8f',SUM(dps.daily_purchase_quantity) / ts.total_quantity) AS quantity_proportion,
                FORMAT('%.8f',SUM(CASE 
                      WHEN s.currency = 'USD' THEN dps.total_order_amount 
                      ELSE ROUND(dps.total_order_amount * er.rate_to_cny / er_usd.rate_to_cny, 2) 
                    END) / ts.total_sales) AS sales_percentage
            FROM 
                `allwebi.mv_daily_product_sales` AS dps
            LEFT JOIN 
                `allwebi.tb_sites` AS s ON s.site_id = dps.site_id
            LEFT JOIN 
                `allwebi.tb_brand_department` AS bd ON s.brand_department_id = bd.id
            LEFT JOIN 
                `allwebi.tb_exchange_rates` AS er ON s.currency = er.currency_symbol AND dps.order_month = er.exchange_date
            LEFT JOIN 
                `allwebi.tb_exchange_rates` AS er_usd ON 'USD' = er_usd.currency_symbol AND dps.order_month = er_usd.exchange_date,
            total_sales AS ts 
            {where_clause}
            GROUP BY 
                dps.product_id, dps.site_id, ts.total_sales, ts.total_quantity
        )
        SELECT 
            main_query.*,
            (SELECT COUNT(*) FROM main_query) AS total_records
        FROM 
            main_query
        ORDER BY 
            total_daily_purchase_quantity DESC
        LIMIT {limit} OFFSET {offset}
    """

    # 构建 WHERE 子句
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
        where_conditions.append(f"dps.site_id IN UNNEST(@site_ids)")
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # 格式化查询语句
    query = base_query.format(
        where_clause=where_clause,
        limit=params.limit,
        offset=offset
    )

    # 设置查询参数
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

    # 执行查询
    query_job = client.query(query, job_config=job_config)

    # 处理查询结果
    rows = [dict(row) for row in query_job]

    if rows:
        total_records = rows[0].get('total_records', 0)
    else:
        total_records = 0

    return {
        "total": total_records,
        "result": rows
    }

@router.post("/product-sales-analysis-spu", response_model=DailyProductReportResponse)
async def product_sales_analysis_spu(params: DailyProductReportParams):
    try:
        response_data = query_product_sales_analysis_spu(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return response_data