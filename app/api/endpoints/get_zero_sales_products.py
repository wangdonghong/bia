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
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    site_ids: Optional[str] = None

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
        {create_time_filter}
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

    create_time_filter = ""
    if params.start_date and params.end_date:
        start_date = datetime.strptime(params.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(params.end_date, "%Y-%m-%d").date()
        create_time_filter = "AND DATE(p.create_time) BETWEEN '{start_date}' AND '{end_date}'".format(
            start_date=start_date,
            end_date=end_date
        )
    elif params.start_date:
        start_date = datetime.strptime(params.start_date, "%Y-%m-%d").date()
        create_time_filter = "AND DATE(p.create_time) >= '{start_date}'".format(
            start_date=start_date
        )
    elif params.end_date:
        end_date = datetime.strptime(params.end_date, "%Y-%m-%d").date()
        create_time_filter = "AND DATE(p.create_time) <= '{end_date}'".format(
            end_date=end_date
        )

    site_id_filter = ""
    if params.site_ids:
        site_id_filter = "AND p.site_id IN UNNEST(@site_ids)"

    query = base_query.format(
        limit=params.limit,
        offset=offset,
        create_time_filter=create_time_filter,
        site_id_filter=site_id_filter
    )

    # 执行查询
    query_job = client.query(query, job_config=job_config, params={'site_ids': params.site_ids})

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