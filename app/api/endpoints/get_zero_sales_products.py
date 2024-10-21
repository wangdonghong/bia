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
    create_time: Optional[str] = None

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
            SELECT p.p_id as product_id,p.title as product_title,p.online_time,p.main_image as product_img,p.tags,s.brand AS site_name, bd.department_name AS department_name, p.create_time
FROM `allwebi.tb_goods` p
LEFT JOIN `allwebi.mv_sold_products` sp ON p.p_id = sp.product_id 
LEFT JOIN 
                `allwebi.tb_sites` AS s ON s.site_id = p.site_id
            LEFT JOIN 
                `allwebi.tb_brand_department` AS bd ON s.brand_department_id = bd.id 
WHERE sp.product_id IS NULL
        )
        SELECT 
            main_query.*,
            (SELECT COUNT(*) FROM main_query) AS total_records
        FROM 
            main_query
        WHERE 1=1
        {create_time_filter}
        ORDER BY 
            online_time DESC
        LIMIT {limit} OFFSET {offset}
    """

    create_time_filter = ""
    if params.create_time:
        create_time_filter = "AND DATE_FORMAT(main_query.create_time, '%Y-%m-%d %H:%i:%s') = '{create_time}'".format(create_time=params.create_time)

    query = base_query.format(
        limit=params.limit,
        offset=offset,
        create_time_filter=create_time_filter
    )

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