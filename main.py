# main.py

from fastapi import FastAPI, Query
from typing import Optional
from services import summary_service
from fastapi.middleware.cors import CORSMiddleware
from utils.utils import clean_float_json

app = FastAPI(title="QC Snapshot Summary API")

# ✅ Allow CORS for your Vue frontend (adjust origin in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with ["http://localhost:5173"] for Vue dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ 1. 批次合格率每日趋势
@app.get("/summary/pass-rate-by-day")
def pass_rate_by_day(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_pass_rate_by_day(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)


# ✅ 2. 班组异常字段对比
@app.get("/summary/abnormal-by-team")
def abnormal_by_team(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_abnormal_by_team(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)


# ✅ 3. 异常字段比例
@app.get("/summary/abnormal-ratio-by-field")
def abnormal_ratio_by_field(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_abnormal_ratio_by_field(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)


# ✅ 4. 时间 × 产品 异常热力图
@app.get("/summary/abnormal-heatmap")
def abnormal_heatmap_by_product_date(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_abnormal_heatmap_by_product_date(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)


# ✅ 5. 产品异常批次统计图
@app.get("/summary/abnormal-batches-by-product")
def abnormal_by_product(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_abnormal_batches_by_product(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)


# ✅ 6. 人员质检字段数量对比
@app.get("/summary/inspection-count-by-personnel")
def inspection_count_by_personnel_field_level(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_inspection_count_by_personnel_field_level(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)

# ✅ 7. 异常字段比例（合并 Others）
@app.get("/summary/abnormal-ratio-by-field-grouped")
def abnormal_ratio_by_field_grouped_other(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_abnormal_ratio_by_field_grouped_other(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)
