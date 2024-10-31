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
    online_start_date: Optional[str] = None
    online_end_date: Optional[str] = None
    site_ids: Optional[str] = None
    title_search: Optional[str] = None
    tag_search: Optional[str] = None
    custom_tag_search: Optional[str] = None

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
            SELECT p.p_id as product_id,p.title as product_title,p.online_time,p.create_time,p.main_image as product_img,p.tags,s.brand AS site_name, bd.department_name AS department_name
FROM `allwebi.tb_goods` p
LEFT JOIN `allwebi.mv_sold_products` sp ON p.p_id = sp.product_id 
LEFT JOIN 
                `allwebi.tb_sites` AS s ON s.site_id = p.site_id
            LEFT JOIN 
                `allwebi.tb_brand_department` AS bd ON s.brand_department_id = bd.id 
            LEFT JOIN 
            `allwebi.tb_goods_tag` AS gt ON gt.product_spu = p.p_id 
WHERE sp.product_id IS NULL
        {create_time_filter}
        {site_id_filter}
        {title_search_filter}
        {tag_search_filter}
        {online_time_filter}
        {custom_tag_filter}
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

    online_time_filter = ""
    if params.online_start_date and params.online_end_date:
        online_start_date = datetime.strptime(params.online_start_date, "%Y-%m-%d").date()
        online_end_date = datetime.strptime(params.online_end_date, "%Y-%m-%d").date()
        online_time_filter = "AND p.online_time != '' AND DATE(p.online_time) BETWEEN '{online_start_date}' AND '{online_end_date}'".format(
            online_start_date=online_start_date,
            online_end_date=online_end_date
        )
    elif params.online_start_date:
        online_start_date = datetime.strptime(params.online_start_date, "%Y-%m-%d").date()
        online_time_filter = "AND p.online_time != '' AND DATE(p.online_time) >= '{online_start_date}'".format(
            online_start_date=online_start_date
        )
    elif params.online_end_date:
        online_end_date = datetime.strptime(params.online_end_date, "%Y-%m-%d").date()
        online_time_filter = "AND p.online_time != '' AND DATE(p.online_time) <= '{online_end_date}'".format(
            online_end_date=online_end_date
        )

    site_id_filter = ""
    if params.site_ids:
        site_id_filter = "AND p.site_id IN UNNEST(@site_ids)"

    title_search_filter = ""
    if params.title_search:
        title_search_filter = "AND REGEXP_CONTAINS(p.title, CONCAT('(?i)', @title_search))"

    custom_tag_filter = ""
    if params.custom_tag_search:
        custom_tag_filter = "AND REGEXP_CONTAINS(gt.tag, CONCAT('(?i)', @custom_tag_search))"

    tag_search_filter = ""
    if params.tag_search:
        tag_search_list = params.tag_search.split(',')
        tag_search_regex = r'(?i)(' + '|'.join(tag_search_list) + ')'
        tag_search_filter = "AND REGEXP_CONTAINS(p.tags, '{tag_search_regex}')".format(
            tag_search_regex=tag_search_regex
        )

    query = base_query.format(
        limit=params.limit,
        offset=offset,
        create_time_filter=create_time_filter,
        site_id_filter=site_id_filter,
        title_search_filter=title_search_filter,
        tag_search_filter=tag_search_filter,
        online_time_filter=online_time_filter,
        custom_tag_filter=custom_tag_filter
    )

    query_params = []
    if params.site_ids:
        site_ids = [int(id.strip()) for id in params.site_ids.split(',')]
        query_params.append(bigquery.ArrayQueryParameter("site_ids", "INT64", site_ids))
    if params.title_search:
        query_params.append(bigquery.ScalarQueryParameter("title_search", "STRING", params.title_search))
    if params.tag_search:
        query_params.append(bigquery.ScalarQueryParameter("tag_search", "STRING", params.tag_search))
    if params.custom_tag_search:
        query_params.append(bigquery.ScalarQueryParameter("custom_tag_search", "STRING", params.custom_tag_search))

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