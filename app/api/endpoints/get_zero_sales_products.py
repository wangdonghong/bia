from fastapi import APIRouter, HTTPException, FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from datetime import date, datetime

router = APIRouter()

# 请求模型
class GetZeroSalesProductsParams(BaseModel):
    page: int = 1
    limit: int = 50
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    online_start_date: Optional[datetime] = None
    online_end_date: Optional[datetime] = None
    site_ids: Optional[str] = None  # 逗号分隔的site_id字符串
    title_search: Optional[str] = None  # 新增：模糊搜索标题
    tag_search: Optional[str] = None

# 响应模型
class GetZeroSalesProductsResponse(BaseModel):
    total: int
    result: List[Dict[str, Any]]

# 查询函数
def query_get_zero_sales_products(params: GetZeroSalesProductsParams) -> Dict[str, Any]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(use_query_cache=True)

    # 计算偏移量
    offset = (params.page - 1) * params.limit

    # 基础查询
    base_query = """
        WITH main_query AS (
            SELECT p.p_id as product_id,p.title as product_title,p.online_time as product_online_time,p.main_image as product_img,p.tags,s.brand AS site_name, bd.department_name AS department_name
FROM `allwebi.tb_goods` p
LEFT JOIN `allwebi.mv_sold_products` sp ON p.p_id = sp.product_id 
LEFT JOIN 
                `allwebi.tb_sites` AS s ON s.site_id = p.site_id
            LEFT JOIN 
                `allwebi.tb_brand_department` AS bd ON s.brand_department_id = bd.id 
WHERE sp.product_id IS NULL
{where_clause}
        )
        SELECT 
            main_query.*,
            (SELECT COUNT(*) FROM main_query) AS total_records
        FROM 
            main_query
        ORDER BY 
            online_time DESC
        LIMIT {limit} OFFSET {offset}
    """

    # 构建 WHERE 子句
    where_conditions = []
    if params.start_date:
        where_conditions.append("CAST(p.create_time AS DATE) >= @start_date")
    if params.end_date:
        where_conditions.append("CAST(p.create_time AS DATE) <= @end_date")
    if params.online_start_date:
        where_conditions.append("""
            (p.online_time != '' AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', p.online_time) IS NOT NULL AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', p.online_time) >= @online_start_date)
        """)
    if params.online_end_date:
        where_conditions.append("""
            (p.online_time != '' AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', p.online_time) IS NOT NULL AND 
             SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', p.online_time) <= @online_end_date)
        """)
    if params.site_ids:
        where_conditions.append(f"p.site_id IN UNNEST(@site_ids)")
    if params.title_search:
        where_conditions.append("REGEXP_CONTAINS(p.title, CONCAT('(?i)', @title_search))")
    # if params.tag_search:
    #     where_conditions.append("REGEXP_CONTAINS(g.tags, CONCAT('(?i)', @tag_search))")
    if params.tag_search:
        tag_search_list = params.tag_search.split(',')
        tag_search_regex = r'(?i)(' + '|'.join(tag_search_list) + ')'
        where_conditions.append(f"REGEXP_CONTAINS(p.tags, '{tag_search_regex}')")


    where_clause = " AND ".join(where_conditions) if where_conditions else ""

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
    if params.title_search:
        query_params.append(bigquery.ScalarQueryParameter("title_search", "STRING", params.title_search))
    if params.tag_search:
        query_params.append(bigquery.ScalarQueryParameter("tag_search", "STRING", params.tag_search))

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

@router.post("/get-zero-sales-products", response_model=GetZeroSalesProductsResponse)
async def get_zero_sales_products(params: GetZeroSalesProductsParams):
    try:
        response_data = query_get_zero_sales_products(params)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return response_data