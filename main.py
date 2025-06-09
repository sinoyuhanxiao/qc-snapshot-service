# main.py
import base64
import tempfile
from datetime import datetime

from fastapi import FastAPI, Query, Body
from typing import Optional

from my_requests.ExportPdfRequest import ExportPdfRequest
from services import summary_service, document_export_service
from fastapi.middleware.cors import CORSMiddleware
from utils.utils import clean_float_json
from fastapi.responses import StreamingResponse
from services.reporting_service import export_summary_pdf
from scripts import insert_snapshot_from_mongo
from loguru import logger
logger.add("logs/api.log", rotation="1 day", retention="7 days")

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

@app.post("/summary/export-pdf-report")
def export_pdf_report_with_charts(payload: ExportPdfRequest):
    chart_paths = {}

    for key, base64_data in payload.charts.items():
        if not base64_data.startswith("data:image"):
            continue
        header, base64_img = base64_data.split(",", 1)
        extension = header.split("/")[1].split(";")[0]
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{extension}")
        temp_file.write(base64.b64decode(base64_img))
        temp_file.close()
        chart_paths[key] = temp_file.name

    pdf_buffer = export_summary_pdf(
        output_path=None,
        start_date=payload.start_date,
        end_date=payload.end_date,
        team_id=payload.team_id,
        shift_id=payload.shift_id,
        product_id=payload.product_id,
        batch_id=payload.batch_id,
        timezone=payload.timezone,
        chart_paths=chart_paths
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        content=pdf_buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=report_{timestamp}.pdf"
        }
    )

@app.post("/snapshot/manual-trigger")
def manual_snapshot_trigger():
    insert_snapshot_from_mongo.run_manual_snapshot()
    return {"message": "Manual snapshot trigger completed"}
