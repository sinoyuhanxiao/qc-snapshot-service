from io import BytesIO
from typing import Optional

from fpdf import FPDF
from fpdf.enums import XPos, YPos
import pandas as pd
from services import summary_service
from zoneinfo import ZoneInfo
from datetime import datetime
from dateutil import parser

from services.chat_summary_service import generate_section_summary, generate_overall_summary, SECTION_PROMPTS
from utils.translation import COLUMN_TRANSLATIONS
from sqlalchemy import text
from db.postgres import pg_engine as engine
import os

EXCLUDED_COLUMNS = {
    "card_df": [],
    "df1": [],
    "df2": ["parent_id"],
    "df3": ["key"],
    "df5": [],
    "df7": ["submission_id", "collection_name", "comments", "qc_form_template_id", "approver_id", "related_shifts", "related_inspectors", "related_batches"]
}

def apply_exclusions(df: pd.DataFrame, key: str) -> pd.DataFrame:
    exclude = EXCLUDED_COLUMNS.get(key, [])
    return df.drop(columns=exclude, errors="ignore")

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("SimHei", "", os.path.join(os.path.dirname(__file__), "..", "assets", "fonts", "simhei.ttf"))
        self.set_font("SimHei", size=12)
        self.set_auto_page_break(auto=True, margin=15)

    def header(self):
        # insert logo
        logo_path = os.path.join(os.path.dirname(__file__), "..", "assets", "images", "sv_logo.png")
        self.image(logo_path, x=10, y=8, w=25)

        self.set_font("SimHei", size=16)
        self.cell(0, 10, "质量汇总分析报告", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.set_font("SimHei", size=10)
        # self.cell(0, 10, f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")

    def add_section_title(self, title):
        self.set_font("SimHei", size=12)
        self.set_fill_color(230, 230, 230)
        self.cell(0, 10, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT, fill=True)
        self.ln(2)

    def add_table(self, df: pd.DataFrame, col_widths=None):
        self.set_font("SimHei", size=10)
        if col_widths is None:
            col_widths = [190 / len(df.columns)] * len(df.columns)

        # Header
        for i, col in enumerate(df.columns):
            col_cn = COLUMN_TRANSLATIONS.get(col, col) # apply translation
            self.cell(col_widths[i], 8, str(col_cn), border=1, align="C")
        self.ln()

        # Rows
        for _, row in df.iterrows():
            for i, col in enumerate(df.columns):
                val = "-" if pd.isna(row[col]) or row[col] in ["None", None, "nan"] else str(row[col]).replace("•", "-")
                self.cell(col_widths[i], 7, val, border=1, align="C")
            self.ln()
        self.ln(5)

def export_summary_pdf(output_path=None,
                       start_date=None,
                       end_date=None,
                       team_id=None,
                       shift_id=None,
                       product_id=None,
                       batch_id=None,
                       timezone=None,
                       chart_paths: Optional[dict] = None):

    pdf = PDF()
    pdf.add_page()

    localized_start = (
        parser.isoparse(start_date)
        .astimezone(ZoneInfo("UTC"))
        .astimezone(ZoneInfo(timezone))
        .strftime("%Y-%m-%d")
    ) if start_date else "未指定"

    localized_end = (
        parser.isoparse(end_date)
        .astimezone(ZoneInfo("UTC"))
        .astimezone(ZoneInfo(timezone))
        .strftime("%Y-%m-%d")
    ) if end_date else "未指定"

    # filter display
    filter_lines = []
    if start_date or end_date:
        filter_lines.append(f"时间范围：{localized_start or '未指定'} 至 {localized_end or '未指定'}")

    names = get_filter_names(team_id=team_id, shift_id=shift_id, product_id=product_id, batch_id=batch_id)

    if team_id and names.get('team_name'):
        filter_lines.append(f"班组：{names['team_name']}")
    if shift_id and names.get('shift_name'):
        filter_lines.append(f"班次：{names['shift_name']}")
    if product_id and names.get('product_name'):
        filter_lines.append(f"产品：{names['product_name']}")
    if batch_id and names.get('batch_code'):
        filter_lines.append(f"批次：{names['batch_code']}")

    # 拼接过滤条件句子
    pdf.set_font("SimHei", size=10)
    pdf.set_x(10)

    filter_parts = []
    if team_id and names.get('team_name'):
        filter_parts.append(f"班组：{names['team_name']}")
    if shift_id and names.get('shift_name'):
        filter_parts.append(f"班次：{names['shift_name']}")
    if product_id and names.get('product_name'):
        filter_parts.append(f"产品：{names['product_name']}")
    if batch_id and names.get('batch_code'):
        filter_parts.append(f"批次：{names['batch_code']}")

    # 判断是否有任何筛选条件
    has_filters = any([
        team_id and names.get('team_name'),
        shift_id and names.get('shift_name'),
        product_id and names.get('product_name'),
        batch_id and names.get('batch_code'),
        start_date,
        end_date
    ])

    if has_filters:
        # 打印时间范围（单独一行）
        if start_date or end_date:
            pdf.set_font("SimHei", size=10)
            time_range_text = f"时间范围：{localized_start or '未指定'} 至 {localized_end or '未指定'}"
            pdf.set_x((210 - pdf.get_string_width(time_range_text)) / 2)  # 居中
            pdf.cell(pdf.get_string_width(time_range_text), 8, time_range_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            # pdf.ln(2)

        # 打印其他筛选条件（字段名常规大小，字段值加大字体）
        filter_parts = []
        if team_id and names.get('team_name'):
            filter_parts.append(f"班组：{names['team_name']}")
        if shift_id and names.get('shift_name'):
            filter_parts.append(f"班次：{names['shift_name']}")
        if product_id and names.get('product_name'):
            filter_parts.append(f"产品：{names['product_name']}")
        if batch_id and names.get('batch_code'):
            filter_parts.append(f"批次：{names['batch_code']}")

        # 计算总宽度并居中
        total_width = 0
        parts_with_width = []
        for i, part in enumerate(filter_parts):
            key, val = part.split("：")
            key_width = pdf.get_string_width(key + "：")
            val_width = pdf.get_string_width(val) + 2
            separator_width = pdf.get_string_width(" | ") if i < len(filter_parts) - 1 else 0
            parts_with_width.append((key, val, key_width, val_width, separator_width))
            total_width += key_width + val_width + separator_width

        pdf.set_x((210 - total_width) / 2)
        for key, val, key_width, val_width, sep_width in parts_with_width:
            pdf.set_font("SimHei", size=10)
            pdf.cell(key_width, 8, key + "：", align="C")
            pdf.set_font("SimHei", size=11)
            pdf.cell(val_width, 8, val, align="C")
            if sep_width:
                pdf.set_font("SimHei", size=10)
                pdf.cell(sep_width, 8, " | ", align="C")
        # pdf.ln(2)

    else:
        # 无任何筛选条件，打印说明文字
        pdf.set_font("SimHei", size=10)
        msg = "(未设置筛选条件，默认展示全部数据)"
        pdf.set_x((210 - pdf.get_string_width(msg)) / 2)
        pdf.cell(pdf.get_string_width(msg), 8, msg, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # 打印其他筛选条件（字段名常规大小，字段值加大字体）
    pdf.set_font("SimHei", size=10)

    # 1. 先计算整行的总宽度
    total_width = 0
    parts_with_width = []
    for i, part in enumerate(filter_parts):
        key, val = part.split("：")
        key_width = pdf.get_string_width(key + "：")
        val_width = pdf.get_string_width(val) + 2
        separator_width = pdf.get_string_width(" | ") if i < len(filter_parts) - 1 else 0
        parts_with_width.append((key, val, key_width, val_width, separator_width))
        total_width += key_width + val_width + separator_width

    # 2. 设置 X 使整体居中
    pdf.set_x((210 - total_width) / 2)

    # 3. 打印各个字段
    for key, val, key_width, val_width, sep_width in parts_with_width:
        pdf.set_font("SimHei", size=10)
        pdf.cell(key_width, 8, key + "：", align="C")
        pdf.set_font("SimHei", size=11)
        pdf.cell(val_width, 8, val, align="C")
        if sep_width:
            pdf.set_font("SimHei", size=10)
            pdf.cell(sep_width, 8, " | ", align="C")
    pdf.ln(10)


    # 汇总卡片数据
    pdf.add_section_title("总体情况")
    card_df = summary_service.get_summary_card_stats(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(apply_exclusions(card_df, "card_df"))

    # 1. 批次合格率趋势
    pdf.add_section_title("批次合格率趋势")
    if chart_paths and chart_paths.get("pass_rate"):
        pdf.image(chart_paths["pass_rate"], x=pdf.get_x(), w=190)
    pdf.ln(5)
    df1 = summary_service.get_pass_rate_by_day(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(apply_exclusions(df1, "df1"))
    summary_1 = generate_section_summary(df1, SECTION_PROMPTS["批次合格率趋势"])
    pdf.set_font("SimHei", size=10)
    pdf.multi_cell(0, 8, f"AI分析：\n{summary_1}")
    pdf.add_page()

    # 2. 班组异常字段对比
    pdf.add_section_title("班组异常对比")
    if chart_paths and chart_paths.get("abnormal_by_team"):
        pdf.image(chart_paths["abnormal_by_team"], x=pdf.get_x(), w=190)
    pdf.ln(5)
    df2 = summary_service.get_abnormal_by_team(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(apply_exclusions(df2, "df2"))
    summary_2 = generate_section_summary(df2, SECTION_PROMPTS["班组异常对比"])
    pdf.set_font("SimHei", size=10)
    pdf.multi_cell(0, 8, f"AI分析：\n{summary_2}")
    pdf.add_page()

    # 3. 异常类型分布
    pdf.add_section_title("异常类型分布")
    if chart_paths and chart_paths.get("abnormal_ratio"):
        pdf.image(chart_paths["abnormal_ratio"], x=pdf.get_x(), w=190)
    pdf.ln(5)
    df3 = summary_service.get_abnormal_ratio_by_field(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(apply_exclusions(df3, "df3"))
    summary_3 = generate_section_summary(df3, SECTION_PROMPTS["异常类型分布"])
    pdf.set_font("SimHei", size=10)
    pdf.multi_cell(0, 8, f"AI分析：\n{summary_3}")
    pdf.add_page()

    # 4. 产品异常批次统计
    pdf.add_section_title("产品异常批次统计")
    if chart_paths and chart_paths.get("abnormal_batches_by_product"):
        pdf.image(chart_paths["abnormal_batches_by_product"], x=pdf.get_x(), w=190)
    pdf.ln(5)
    df4 = summary_service.get_abnormal_batches_by_product(start_date, end_date, team_id, shift_id, product_id, batch_id)
    df4 = apply_exclusions(df4, "df4")
    col_count = len(df4.columns)
    # 默认平均宽度
    default_width = 190 / col_count
    col_widths = []
    for col in df4.columns:
        if col == "product_name":
            col_widths.append(default_width + 20)  # 产品名列加宽
        else:
            col_widths.append(default_width - (20 / (col_count - 1)))  # 平均补偿其余列
    pdf.add_table(df4, col_widths=col_widths)
    summary_4 = generate_section_summary(df4, SECTION_PROMPTS["产品异常批次统计"])
    pdf.set_font("SimHei", size=10)
    pdf.multi_cell(0, 8, f"AI分析：\n{summary_4}")
    pdf.add_page()

    # 6. 人员检测 KPI
    pdf.add_section_title("质检人员 KPI")
    if chart_paths and chart_paths.get("kpi_by_inspector"):
        pdf.image(chart_paths["kpi_by_inspector"], x=pdf.get_x(), w=190)
    pdf.ln(5)
    df5 = summary_service.get_kpi_by_inspector(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(apply_exclusions(df5, "df5"))
    summary_5 = generate_section_summary(df5, SECTION_PROMPTS["质检人员 KPI"])
    pdf.set_font("SimHei", size=10)
    pdf.multi_cell(0, 8, f"AI分析：\n{summary_5}")
    pdf.add_page()

    # 7. 需复检清单
    pdf.add_section_title("需复检列表") 
    df7 = summary_service.get_retest_records(start_date, end_date, team_id, shift_id, product_id, batch_id)
    df7 = apply_exclusions(df7, "df7")
    col_count = len(df7.columns)
    default_width = 190 / col_count
    col_widths = []
    for col in df7.columns:
        if col in ["qc_form_template_name", "related_products"]:
            col_widths.append(default_width + 20)  # 表单名称与产品列加宽
        else:
            col_widths.append(default_width - (40 / (col_count - 2)))  # 平均补偿其余列
    pdf.add_table(df7, col_widths=col_widths)
    summary_7 = generate_section_summary(df7, SECTION_PROMPTS["需复检列表"])
    pdf.set_font("SimHei", size=10)
    pdf.multi_cell(0, 8, f"AI分析：\n{summary_7}")
    pdf.add_page()

    # 8. 汇总部分
    filtered_data = {
        "总体情况": card_df,
        "批次合格率趋势": df1,
        "班组异常字段对比": df2,
        "异常类型分布": df3,
        "产品异常批次统计": df4,
        "质检人员 KPI": df5, # leave df6 empty for now 产品 x 日期热力图
        "需复检列表": df7
    }

    summary_text = generate_overall_summary(filtered_data)

    if summary_text:
        pdf.add_section_title("AI整体总结")
        pdf.set_font("SimHei", size=10)

        # 清洗掉 GPT 输出中的不兼容字符
        safe_text = summary_text.replace("•", "-")
        pdf.multi_cell(0, 8, safe_text)

    # 8. 保存 PDF
    # pdf.output(output_path)
    buffer = BytesIO()
    pdf.output(buffer)
    buffer.seek(0)
    return buffer

def get_filter_names(team_id=None, shift_id=None, product_id=None, batch_id=None):
    name_map = {}

    if team_id:
        sql = "SELECT name FROM quality_management.team WHERE id = :id"
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"id": team_id}).fetchone()
            if result:
                name_map["team_name"] = result[0]

    if shift_id:
        sql = "SELECT name FROM quality_management.shift WHERE id = :id"
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"id": shift_id}).fetchone()
            if result:
                name_map["shift_name"] = result[0]

    if product_id:
        sql = "SELECT name FROM quality_management.qc_suggested_product WHERE id = :id"
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"id": product_id}).fetchone()
            if result:
                name_map["product_name"] = result[0]

    if batch_id:
        sql = "SELECT code FROM quality_management.qc_suggested_batch WHERE id = :id"
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"id": batch_id}).fetchone()
            if result:
                name_map["batch_code"] = result[0]

    return name_map

def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"../assets/reports/summary_report_{timestamp}.pdf"
    # export_summary_pdf(
    #     output_path=output_path,
    #     start_date='2025-05-01',y
    #     end_date='2025-05-30',
    #     team_id=136,
    #     shift_id=1,
    #     product_id=6,
    #     batch_id=14
    # )

    export_summary_pdf(
        output_path=output_path,
        start_date=None,
        end_date=None,
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )


if __name__ == "__main__":
    main()
