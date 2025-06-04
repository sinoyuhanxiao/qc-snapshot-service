import os
from openai import OpenAI
from dotenv import load_dotenv
import pandas as pd
from typing import Optional

load_dotenv()  # 加载 .env 文件中的环境变量

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SECTION_PROMPTS = {
        "批次合格率趋势": """
    请分析最近时间段内的批次合格率趋势，包括波动、上升或下降的阶段，并指出潜在原因。如果有明显的异常点或改善趋势，请重点说明。最后，请提出是否需要调整质控策略。
    """,
        "班组异常对比": """
    请比较各个班组在检测中出现异常的频率，不合格率高可能代表检测出来的问题更多，并不代表这个班组水平差，请提出针对异常较高班组的建议。
    """,
        "异常类型分布": """
    请分析各类异常在质检中的分布情况，指出高频异常项及其可能成因（如物料、设备、环境）。如果有某类异常在本周期显著增加，请提出可能的风险及应对建议。
    """,
        "产品异常批次统计": """
    请分析不同产品的异常批次分布，指出哪些产品稳定性较差，是否存在特定批次集中问题。请尝试归因于设计、工艺或原料问题，并提出下一步建议（如重点监控、追加验证等）。
    """,
        "质检人员 KPI": """
    请分析各质检人员的检测数量与异常比，识别出异常比例较高的人员。能检测出越多的异常代表用心检测以及检测水平高，请讨论是否与培训、经验或工作负荷有关，并提出改进建议（如再培训或工作重分配）。
    """,
        "需复检列表": """
    请概括本期需要复检的记录特点，包括常见问题、涉及产品和班组等。分析是否为系统性问题还是个别情况，并指出复检完成的优先级建议与责任归属方向。
    """
}

def generate_section_summary(df: pd.DataFrame, prompt: str) -> Optional[str]:
    if df.empty:
        return "无数据可供分析"

    preview = df.head(5).to_string(index=False)

    full_prompt = f"""
        你是一个质量分析总结助手。请根据以下部分数据生成中文汇总段落：
        {prompt}
        
        以下是数据预览：
        {preview}
        
        要求：
        - 使用正式中文
        - 生成一段3-6句的总结文字
        - 用很具体数据逻辑支持你的分析，不要泛泛而谈
        """

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": full_prompt}]
    )

    raw_text = response.choices[0].message.content.strip()
    cleaned_text = "\n".join([line.strip() for line in raw_text.splitlines() if line.strip()])
    return cleaned_text

def generate_overall_summary(filtered_data: dict) -> str:
    """
    Accepts a dictionary with filtered QC data (e.g., card_df, df1, df2, ...)
    Converts it into a natural language prompt and generates a Chinese 3-sentence summary.
    """

    if all(df.empty for df in filtered_data.values()):
        return ""  # No data → no summary

    # Compose a human-readable summary input
    summary_text = ""
    for section, df in filtered_data.items():
        if not df.empty:
            summary_text += f"\n【{section}】\n{df.head(5).to_string(index=False)}\n"

    prompt = f"""
        你是一个质量分析总结助手。以下是来自多个质量管理模块的部分数据内容，请你用正式中文语言总结一段三段话的汇总报告，必须要有理有据，可用于管理层质量分析汇报。
        如果没有数据，请直接给出“无数据可供分析”即可。

        请从以下三个角度展开撰写三段话：
        1. 整体质量水平：例如合格率趋势、异常数量变化。
        2. 关键异常点：哪些产品、环节或字段问题频发，以及质检人员的检测效果。
        3. 改进建议或关注重点：从数据中引导出需特别关注的方向。

        数据如下：
        {summary_text}

        要求：
        - 使用中文书写
        - 三段话，每段不少于一句
        - 用正式但易懂的语言表达，有理有据
    """

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content.strip()
