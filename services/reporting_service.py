from fpdf import FPDF
import pandas as pd
from datetime import datetime
from services import summary_service
from fpdf.enums import XPos, YPos
import os

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("SimHei", "", os.path.join(os.path.dirname(__file__), "..", "assets", "fonts", "simhei.ttf"))
        self.set_font("SimHei", size=12)
        self.set_auto_page_break(auto=True, margin=15)

    def header(self):
        self.set_font("SimHei", size=16)
        self.cell(0, 10, "质量汇总分析报告", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.set_font("SimHei", size=10)
        self.cell(0, 10, f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.ln(5)

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
            self.cell(col_widths[i], 8, str(col), border=1, align="C")
        self.ln()

        # Rows
        for _, row in df.iterrows():
            for i, col in enumerate(df.columns):
                val = str(row[col]).replace("•", "-")
                self.cell(col_widths[i], 7, val, border=1)
            self.ln()
        self.ln(5)

def export_summary_pdf(output_path="summary_report.pdf",
                       start_date=None,
                       end_date=None,
                       team_id=None,
                       shift_id=None,
                       product_id=None,
                       batch_id=None):
    pdf = PDF()
    pdf.add_page()

    # 1. 汇总卡片数据
    pdf.add_section_title("总体情况")
    card_df = summary_service.get_summary_card_stats(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(card_df)

    # 2. 批次合格率趋势
    pdf.add_section_title("批次合格率趋势")
    df1 = summary_service.get_pass_rate_by_day(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(df1)

    # 3. 班组异常字段对比
    pdf.add_section_title("班组异常对比")
    df2 = summary_service.get_abnormal_by_team(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(df2)

    # 4. 异常类型分布
    pdf.add_section_title("异常类型分布")
    df3 = summary_service.get_abnormal_ratio_by_field(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(df3)

    # 5. 产品异常批次统计
    pdf.add_section_title("产品异常批次统计")
    df5 = summary_service.get_abnormal_batches_by_product(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(df5)

    # 6. 人员检测 KPI
    pdf.add_section_title("质检人员 KPI")
    df6 = summary_service.get_kpi_by_inspector(start_date, end_date, team_id, shift_id, product_id, batch_id)
    pdf.add_table(df6)

    # 7. 保存 PDF
    pdf.output(output_path)

def main():
    export_summary_pdf(
        output_path="summary_report_sample.pdf",
        start_date="2025-05-01",
        end_date="2025-05-31"
    )

if __name__ == "__main__":
    main()
