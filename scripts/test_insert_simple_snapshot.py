"""
test_insert_simple_snapshot.py

This script queries `qc_alert_record` within the last 10 minutes to calculate:
- Total number of inspected fields
- Number of abnormal fields (based on control limits)
- Pass rate (%)

Then it inserts a summary snapshot into `qc_summary_snapshot`.

Purpose:
- Quickly test end-to-end PostgreSQL QC snapshot pipeline
- Validate schema logic and pass rate calculation

Author: Erik Yu
"""

from datetime import datetime, timedelta
from decimal import Decimal
from db.postgres import get_postgres_connection


def generate_snapshot():
    conn = get_postgres_connection()
    cursor = conn.cursor()

    now = datetime.utcnow()
    ten_minutes_ago = now - timedelta(minutes=10)

    query = """
    SELECT
        COUNT(*) AS total_fields,
        COUNT(*) FILTER (
            WHERE (
                alert_type = 'number' AND
                inspection_value IS NOT NULL AND (
                    (lower_control_limit IS NOT NULL AND inspection_value < lower_control_limit) OR
                    (upper_control_limit IS NOT NULL AND inspection_value > upper_control_limit)
                )
            )
        ) AS abnormal_fields
    FROM quality_management.qc_alert_record
    WHERE created_at >= %s AND created_at <= %s
    """

    cursor.execute(query, (ten_minutes_ago, now))
    total_fields, abnormal_fields = cursor.fetchone()

    pass_rate = 1 - Decimal(abnormal_fields or 0) / Decimal(total_fields or 1)
    pass_rate_percent = round(pass_rate * 100, 2)

    print("✅ QC 快照统计：")
    print(f"时间范围：{ten_minutes_ago} ~ {now}")
    print(f"总字段数：{total_fields}")
    print(f"异常字段数：{abnormal_fields}")
    print(f"合格率：{pass_rate_percent}%")

    insert_sql = """
    INSERT INTO quality_management.qc_summary_snapshot (
        snapshot_time,
        total_fields,
        abnormal_fields,
        pass_rate_percent
    ) VALUES (%s, %s, %s, %s)
    """

    cursor.execute(insert_sql, (now, total_fields, abnormal_fields, pass_rate_percent))
    conn.commit()

    print("📥 快照数据已成功写入 qc_summary_snapshot ✅")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    generate_snapshot()
