# main.py
from datetime import datetime

from fastapi import FastAPI, Query
from typing import Optional
from services import summary_service, document_export_service
from fastapi.middleware.cors import CORSMiddleware
from utils.utils import clean_float_json
from fastapi.responses import StreamingResponse
from services.reporting_service import export_summary_pdf

app = FastAPI(title="QC Snapshot Summary API")

# Allow CORS for your Vue frontend (adjust origin in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Can be replaced with ["http://localhost:3000"] for Vue dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. 批次合格率每日趋势
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


# 2. 班组异常字段对比
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


# 3. 异常字段比例
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


# 4. 时间 × 产品 异常热力图
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


# 5. 产品异常批次统计图
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


# 6. 人员质检字段数量对比
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

# 7. 异常字段比例（合并 Others）
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

# 8. 汇总卡片统计信息
@app.get("/summary/card-stats")
def get_card_stats(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_summary_card_stats(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)

# 9. 人员 KPI（检测字段数 + 异常率 + 提交表单数）
@app.get("/summary/personnel-kpi")
def personnel_kpi(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_kpi_by_inspector(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)

# 10. 复检记录列表
@app.get("/summary/retest-records")
def get_retest_records(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    df = summary_service.get_retest_records(start_date, end_date, team_id, shift_id, product_id, batch_id)
    return clean_float_json(df)

# 11. 导出质检记录文档
@app.get("/summary/document-list")
def export_documents(
    start_date: Optional[str] = Query(...),
    end_date: Optional[str] = Query(...),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None)
):
    docs = document_export_service.get_documents_list(
        start_date=start_date,
        end_date=end_date,
        team_id=team_id,
        shift_id=shift_id,
        product_id=product_id,
        batch_id=batch_id
    )
    return {
        "data": docs
    }

@app.get("/summary/export-pdf-report")
def export_pdf_report(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    shift_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    batch_id: Optional[int] = Query(None),
    timezone: Optional[str] = Query("UTC")  # default to UTC if not provided
):
    pdf_buffer = export_summary_pdf(
        output_path=None,
        start_date=start_date,
        end_date=end_date,
        team_id=team_id,
        shift_id=shift_id,
        product_id=product_id,
        batch_id=batch_id,
        timezone=timezone
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        content=pdf_buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=report_{timestamp}.pdf; filename*=UTF-8''%E8%B4%A8%E9%87%8F%E6%B1%87%E6%80%BB%E6%8A%A5%E5%91%8A_{timestamp}.pdf"
        }
    )

