import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()  # 加载 .env 文件中的环境变量

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_chinese_summary(filtered_data: dict) -> str:
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
        你是一个质量分析总结助手。以下是来自多个质量管理模块的部分数据内容，请你用正式中文语言总结一段三句话的汇总报告，可用于管理层质量分析汇报。
    
        请从以下三个角度简要总结：
        1. 整体质量水平：例如合格率趋势、异常数量变化。
        2. 关键异常点：哪些产品、环节或字段问题频发，以及质检人员的检测效果。
        3. 改进建议或关注重点：从数据中引导出需特别关注的方向。
    
        数据如下：
        {summary_text}
    
        要求：
        - 使用中文书写
        - 总共五句话
        - 用正式但易懂的语言表达
    """

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content.strip()
