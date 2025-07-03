from typing import Optional
from sqlalchemy import text
import pandas as pd
from db.postgres import pg_engine as engine

# 通用 WHERE 条件构造

def build_filter_conditions_for_general():
    return """
        (:start_date IS NULL OR base.snapshot_time::date >= :start_date)
        AND (:end_date IS NULL OR base.snapshot_time::date <= :end_date)
        AND (:team_id IS NULL OR team.team_id = :team_id)
        AND (:shift_id IS NULL OR shift.shift_id = :shift_id)
        AND (:product_id IS NULL OR product.product_id = :product_id)
        AND (:batch_id IS NULL OR batch.batch_id = :batch_id)
    """
def build_filter_conditions_for_team():
    return """
        (:start_date IS NULL OR base.snapshot_time::date >= :start_date)
        AND (:end_date IS NULL OR base.snapshot_time::date <= :end_date)
        AND (:team_id IS NULL OR st.team_id = :team_id)
        AND (:shift_id IS NULL OR shift.shift_id = :shift_id)
        AND (:product_id IS NULL OR product.product_id = :product_id)
        AND (:batch_id IS NULL OR batch.batch_id = :batch_id)
    """

# 1. 批次合格率每日趋势

def get_pass_rate_by_day(start_date: Optional[str], end_date: Optional[str],
                         team_id: Optional[int], shift_id: Optional[int],
                         product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    sql = f"""
        SELECT base.snapshot_time::date AS snapshot_date,
               COUNT(DISTINCT batch.batch_id) AS total_batches,
               COUNT(DISTINCT CASE WHEN item.abnormal_count > 0 THEN batch.batch_id END) AS abnormal_batches,
               ROUND(
                   1 - COUNT(DISTINCT CASE WHEN item.abnormal_count > 0 THEN batch.batch_id END)::decimal /
                       NULLIF(COUNT(DISTINCT batch.batch_id), 0), 4
               ) AS pass_rate
        FROM quality_management.qc_snapshot_base base
        JOIN quality_management.qc_snapshot_item item ON base.id = item.snapshot_id
        LEFT JOIN quality_management.qc_snapshot_team team ON base.id = team.snapshot_id
        LEFT JOIN quality_management.qc_snapshot_shift shift ON base.id = shift.snapshot_id
        LEFT JOIN quality_management.qc_snapshot_product product ON base.id = product.snapshot_id
        LEFT JOIN quality_management.qc_snapshot_batch batch ON base.id = batch.snapshot_id
        WHERE {build_filter_conditions_for_general()}
        GROUP BY base.snapshot_time::date
        ORDER BY base.snapshot_time::date
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

# 2. 班组异常对比（大/小组）
def get_abnormal_by_team(start_date: Optional[str], end_date: Optional[str],
                         team_id: Optional[int], shift_id: Optional[int],
                         product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.team t ON st.team_id = t.id",
        "JOIN quality_management.qc_snapshot_item i ON st.snapshot_id = i.snapshot_id",
        "JOIN quality_management.qc_snapshot_base b ON st.snapshot_id = b.id"
    ]
    where = []

    if start_date:
        where.append("b.snapshot_time >= :start_date")
    if end_date:
        where.append("b.snapshot_time <= :end_date")

    if team_id is not None:
        where.append("st.team_id = :team_id")

    if shift_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_shift ss ON st.snapshot_id = ss.snapshot_id")
        where.append("ss.shift_id = :shift_id")

    if product_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_product sp ON st.snapshot_id = sp.snapshot_id")
        where.append("sp.product_id = :product_id")

    if batch_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_batch sb ON st.snapshot_id = sb.snapshot_id")
        where.append("sb.batch_id = :batch_id")

    join_clause = "\n".join(joins)
    where_clause = " AND ".join(where) if where else "1=1"

    sql = f"""
        SELECT
            st.team_id,
            st.team_name,
            t.parent_id,
            SUM(i.abnormal_count) AS abnormal_fields,
            SUM(i.total_count) - SUM(i.abnormal_count) AS normal_count,
            SUM(i.total_count) AS total_fields,
            ROUND(
                1.0 - SUM(i.abnormal_count)::decimal / NULLIF(SUM(i.total_count), 0), 4
            ) AS pass_rate
        FROM quality_management.qc_snapshot_team st
        {join_clause}
        WHERE {where_clause}
        GROUP BY st.team_id, st.team_name, t.parent_id
        ORDER BY ROUND(
                1.0 - SUM(i.abnormal_count)::decimal / NULLIF(SUM(i.total_count), 0), 4
            ) DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())


def get_abnormal_ratio_by_field(start_date: Optional[str], end_date: Optional[str],
                                team_id: Optional[int], shift_id: Optional[int],
                                product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.qc_snapshot_base b ON i.snapshot_id = b.id"
    ]
    where = []

    if start_date:
        where.append("b.snapshot_time >= :start_date")
    if end_date:
        where.append("b.snapshot_time <= :end_date")

    if team_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_team st ON i.snapshot_id = st.snapshot_id")
        where.append("st.team_id = :team_id")

    if shift_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_shift ss ON i.snapshot_id = ss.snapshot_id")
        where.append("ss.shift_id = :shift_id")

    if product_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_product sp ON i.snapshot_id = sp.snapshot_id")
        where.append("sp.product_id = :product_id")

    if batch_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_batch sb ON i.snapshot_id = sb.snapshot_id")
        where.append("sb.batch_id = :batch_id")

    join_clause = "\n".join(joins)
    where_clause = " AND ".join(where) if where else "1=1"

    sql = f"""
        SELECT 
            i.key,
            i.label,
            SUM(i.abnormal_count) AS abnormal_fields,
            SUM(i.total_count) - SUM(i.abnormal_count) AS normal_fields,
            SUM(i.total_count) AS total_fields,
            ROUND(
                SUM(i.abnormal_count) * 100.0 / NULLIF(SUM(i.total_count), 0), 2
            ) AS abnormal_percentage
        FROM quality_management.qc_snapshot_item i
        {join_clause}
        WHERE {where_clause}
        GROUP BY i.key, i.label
        ORDER BY abnormal_percentage DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

def get_abnormal_heatmap_by_product_date(start_date: Optional[str], end_date: Optional[str],
                                          team_id: Optional[int], shift_id: Optional[int],
                                          product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.qc_snapshot_item i ON base.id = i.snapshot_id",
        "JOIN quality_management.qc_snapshot_product sp ON base.id = sp.snapshot_id"
    ]
    where = []

    if start_date:
        where.append("base.snapshot_time::date >= :start_date")
    if end_date:
        where.append("base.snapshot_time::date <= :end_date")

    if team_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_team st ON base.id = st.snapshot_id")
        where.append("st.team_id = :team_id")

    if shift_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_shift ss ON base.id = ss.snapshot_id")
        where.append("ss.shift_id = :shift_id")

    if product_id is not None:
        where.append("sp.product_id = :product_id")

    if batch_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_batch sb ON base.id = sb.snapshot_id")
        where.append("sb.batch_id = :batch_id")

    join_clause = "\n".join(joins)
    where_clause = " AND ".join(where) if where else "1=1"

    sql = f"""
        SELECT 
            base.snapshot_time::date AS snapshot_date,
            sp.product_id,
            sp.product_name,
            SUM(i.abnormal_count) AS abnormal_count
        FROM quality_management.qc_snapshot_base base
        {join_clause}
        WHERE {where_clause}
        GROUP BY snapshot_date, sp.product_id, sp.product_name
        ORDER BY snapshot_date, sp.product_name
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

def get_abnormal_batches_by_product(start_date: Optional[str], end_date: Optional[str],
                            team_id: Optional[int], shift_id: Optional[int],
                            product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.qc_snapshot_product sp ON base.id = sp.snapshot_id",
        "JOIN quality_management.qc_snapshot_batch sb ON base.id = sb.snapshot_id",
        "JOIN quality_management.qc_snapshot_item i ON base.id = i.snapshot_id"
    ]
    where = []

    if start_date:
        where.append("base.snapshot_time >= :start_date")
    if end_date:
        where.append("base.snapshot_time <= :end_date")

    if team_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_team st ON base.id = st.snapshot_id")
        where.append("st.team_id = :team_id")

    if shift_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_shift ss ON base.id = ss.snapshot_id")
        where.append("ss.shift_id = :shift_id")

    if product_id is not None:
        where.append("sp.product_id = :product_id")

    if batch_id is not None:
        where.append("sb.batch_id = :batch_id")

    join_clause = "\n".join(joins)
    where_clause = " AND ".join(where) if where else "1=1"

    sql = f"""
        SELECT
            sp.product_id,
            sp.product_name,
            COUNT(DISTINCT sb.batch_id) AS total_batches,
            COUNT(DISTINCT CASE WHEN i.abnormal_count > 0 THEN sb.batch_id END) AS abnormal_batches,
            ROUND(
                COUNT(DISTINCT CASE WHEN i.abnormal_count > 0 THEN sb.batch_id END)::decimal /
                NULLIF(COUNT(DISTINCT sb.batch_id), 0), 4
            ) AS abnormal_ratio
        FROM quality_management.qc_snapshot_base base
        {join_clause}
        WHERE {where_clause}
        GROUP BY sp.product_id, sp.product_name
        ORDER BY abnormal_ratio DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

def get_inspection_count_by_personnel_field_level(start_date: Optional[str], end_date: Optional[str],
                                                  team_id: Optional[int], shift_id: Optional[int],
                                                  product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.qc_snapshot_inspector ins ON base.id = ins.snapshot_id",
        "JOIN quality_management.qc_snapshot_item i ON base.id = i.snapshot_id"
    ]
    where = []

    if start_date:
        where.append("base.snapshot_time >= :start_date")
    if end_date:
        where.append("base.snapshot_time <= :end_date")

    if team_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_team st ON base.id = st.snapshot_id")
        where.append("st.team_id = :team_id")

    if shift_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_shift ss ON base.id = ss.snapshot_id")
        where.append("ss.shift_id = :shift_id")

    if product_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_product sp ON base.id = sp.snapshot_id")
        where.append("sp.product_id = :product_id")

    if batch_id is not None:
        joins.append("JOIN quality_management.qc_snapshot_batch sb ON base.id = sb.snapshot_id")
        where.append("sb.batch_id = :batch_id")

    join_clause = "\n".join(joins)
    where_clause = " AND ".join(where) if where else "1=1"

    sql = f"""
        SELECT
            ins.inspector_id,
            ins.inspector_name,
            COUNT(*) AS inspection_count,
            SUM(i.abnormal_count) AS abnormal_count,
            ROUND(
                1.0 - SUM(i.abnormal_count)::decimal / NULLIF(COUNT(*), 0), 4
            ) AS pass_rate
        FROM quality_management.qc_snapshot_base base
        {join_clause}
        WHERE {where_clause}
        GROUP BY ins.inspector_id, ins.inspector_name
        ORDER BY inspection_count DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())


# ✅ 测试入口
if __name__ == "__main__":
    import pandas as pd

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("expand_frame_repr", False)

    print("Running summary dashboard query...")

    df = get_pass_rate_by_day(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )

    print("批次合格率每日趋势结果：")
    print(df)

    df2 = get_abnormal_by_team(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )

    print("班组异常字段总数：")
    print(df2)

    df3 = get_abnormal_ratio_by_field(
        "2025-05-01",
        "2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )

    print("异常类型占比饼图：")
    print(df3)

    df4 = get_abnormal_heatmap_by_product_date(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )

    print("产品 × 字段 异常热力图：")
    print(df4)

    df5 = get_abnormal_batches_by_product(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )

    print("产品异常统计图：")
    print(df5)

    df6 = get_inspection_count_by_personnel_field_level(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )

    print("人员质检字段数量对比：")
    print(df6)
