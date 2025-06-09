from typing import Optional
from sqlalchemy import text
import pandas as pd
from db.postgres import pg_engine as engine

# 1. 批次合格率每日趋势
def get_pass_rate_by_day(start_date: Optional[str], end_date: Optional[str],
                         team_id: Optional[int], shift_id: Optional[int],
                         product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.qc_snapshot_item i ON base.id = i.snapshot_id",
        "JOIN quality_management.qc_snapshot_batch sb ON base.id = sb.snapshot_id"
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
        joins.append("JOIN quality_management.qc_snapshot_product sp ON base.id = sp.snapshot_id")
        where.append("sp.product_id = :product_id")

    if batch_id is not None:
        where.append("sb.batch_id = :batch_id")

    join_clause = "\n".join(joins)
    where_clause = " AND ".join(where) if where else "1=1"

    sql = f"""
        SELECT base.snapshot_time::date AS snapshot_date,
               COUNT(DISTINCT sb.batch_id) AS total_batches,
               COUNT(DISTINCT CASE WHEN i.abnormal_count > 0 THEN sb.batch_id END) AS abnormal_batches,
               ROUND(
                   1.0 - COUNT(DISTINCT CASE WHEN i.abnormal_count > 0 THEN sb.batch_id END)::decimal /
                   NULLIF(COUNT(DISTINCT sb.batch_id), 0), 4
               ) AS pass_rate
        FROM quality_management.qc_snapshot_base base
        {join_clause}
        WHERE {where_clause}
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
        where.append("b.snapshot_time::date >= :start_date")
    if end_date:
        where.append("b.snapshot_time::date <= :end_date")

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
        WITH raw_team_data AS (
            SELECT
                st.team_id,
                st.team_name,
                t.parent_id,
                SUM(i.abnormal_count) AS abnormal_fields,
                SUM(i.total_count) - SUM(i.abnormal_count) AS normal_fields,
                SUM(i.total_count) AS total_fields
            FROM quality_management.qc_snapshot_team st
            JOIN quality_management.team t ON st.team_id = t.id
            JOIN quality_management.qc_snapshot_item i ON st.snapshot_id = i.snapshot_id
            JOIN quality_management.qc_snapshot_base b ON st.snapshot_id = b.id
            {f"JOIN quality_management.qc_snapshot_shift ss ON st.snapshot_id = ss.snapshot_id" if shift_id is not None else ""}
            {f"JOIN quality_management.qc_snapshot_product sp ON st.snapshot_id = sp.snapshot_id" if product_id is not None else ""}
            {f"JOIN quality_management.qc_snapshot_batch sb ON st.snapshot_id = sb.snapshot_id" if batch_id is not None else ""}
            WHERE {where_clause}
            GROUP BY st.team_id, st.team_name, t.parent_id
        ),
        -- Now collect each parent with all its children (and itself)
        combined AS (
            SELECT
            COALESCE(parent.team_id, child.team_id) AS parent_id,
            COALESCE(parent.team_name, child.team_name) AS parent_name,
                child.team_id AS child_id,
                child.team_name AS child_name,
                child.abnormal_fields,
                child.normal_fields,
                child.total_fields
            FROM raw_team_data child
            LEFT JOIN raw_team_data parent
                ON child.parent_id = parent.team_id
            UNION
            -- add self rows for standalone teams (so they're not missed)
            SELECT
                team_id AS parent_id,
                team_name AS parent_name,
                team_id AS child_id,
                team_name AS child_name,
                abnormal_fields,
                normal_fields,
                total_fields
            FROM raw_team_data
        )
        SELECT
            parent_id AS team_id,
            parent_name AS team_name,
            SUM(abnormal_fields) AS abnormal_fields,
            SUM(normal_fields) AS normal_fields,
            SUM(total_fields) AS total_fields,
            ROUND(
                1.0 - SUM(abnormal_fields)::decimal / NULLIF(SUM(total_fields), 0), 4
            ) AS pass_rate
        FROM combined
        GROUP BY parent_id, parent_name
        ORDER BY pass_rate DESC
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
        where.append("b.snapshot_time::date >= :start_date")
    if end_date:
        where.append("b.snapshot_time::date <= :end_date")

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
            SUM(i.total_count) AS total_count,
            SUM(i.abnormal_count) AS abnormal_count,
            ROUND(
                SUM(i.abnormal_count) * 100.0 / NULLIF(SUM(i.total_count), 0), 2
            ) AS abnormal_percentage
        FROM quality_management.qc_snapshot_item i
        {join_clause}
        WHERE {where_clause}
        GROUP BY i.key, i.label
        ORDER BY abnormal_count DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

def get_abnormal_ratio_by_field_grouped_other(start_date: Optional[str], end_date: Optional[str],
                                              team_id: Optional[int], shift_id: Optional[int],
                                              product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.qc_snapshot_base b ON i.snapshot_id = b.id"
    ]
    where = []

    if start_date:
        where.append("b.snapshot_time::date >= :start_date")
    if end_date:
        where.append("b.snapshot_time::date <= :end_date")
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
            SUM(i.abnormal_count) AS abnormal_count
        FROM quality_management.qc_snapshot_item i
        {join_clause}
        WHERE {where_clause}
        GROUP BY i.key, i.label
        ORDER BY abnormal_count DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=locals())

    total = df['abnormal_count'].sum()
    df['percentage'] = df['abnormal_count'] / total

    above_20 = df[df['percentage'] >= 0.05][['key', 'label', 'abnormal_count']]
    below_20 = df[df['percentage'] < 0.05]

    if not below_20.empty:
        other_sum = below_20['abnormal_count'].sum()
        other_row = pd.DataFrame([{
            'key': None,
            'label': '其他',
            'abnormal_count': other_sum
        }])
        result_df = pd.concat([above_20, other_row], ignore_index=True)
    else:
        result_df = above_20

    return result_df

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
            SUM(i.total_count) AS inspection_count,
            SUM(i.total_count) - SUM(i.abnormal_count) AS normal_count,
            SUM(i.abnormal_count) AS abnormal_count,
            ROUND(
                1.0 - SUM(i.abnormal_count)::decimal / NULLIF(SUM(i.total_count), 0), 4
            ) AS pass_rate
        FROM quality_management.qc_snapshot_base base
        {join_clause}
        WHERE {where_clause}
        GROUP BY ins.inspector_id, ins.inspector_name
        ORDER BY inspection_count DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

def get_summary_card_stats(start_date: Optional[str], end_date: Optional[str],
                           team_id: Optional[int], shift_id: Optional[int],
                           product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    join_clause = """
        JOIN (
            SELECT snapshot_id, COUNT(DISTINCT batch_id) AS batch_count
            FROM quality_management.qc_snapshot_batch
            GROUP BY snapshot_id
        ) sb ON base.id = sb.snapshot_id

        JOIN (
            SELECT snapshot_id, SUM(total_count) AS total_items, SUM(abnormal_count) AS abnormal_items
            FROM quality_management.qc_snapshot_item
            GROUP BY snapshot_id
        ) i ON base.id = i.snapshot_id

        JOIN quality_management.qc_snapshot_inspector ins ON base.id = ins.snapshot_id
    """

    where_clause = []
    if start_date:
        where_clause.append("base.snapshot_time::date >= :start_date")
    if end_date:
        where_clause.append("base.snapshot_time::date <= :end_date")
    if team_id is not None:
        join_clause += "\nJOIN quality_management.qc_snapshot_team st ON base.id = st.snapshot_id"
        where_clause.append("st.team_id = :team_id")
    if shift_id is not None:
        join_clause += "\nJOIN quality_management.qc_snapshot_shift ss ON base.id = ss.snapshot_id"
        where_clause.append("ss.shift_id = :shift_id")
    if product_id is not None:
        join_clause += "\nJOIN quality_management.qc_snapshot_product sp ON base.id = sp.snapshot_id"
        where_clause.append("sp.product_id = :product_id")
    if batch_id is not None:
        # this column no longer exists in sb — batch_id isn't passed in the subquery
        # to filter by batch_id, need to join the full table again
        join_clause += "\nJOIN quality_management.qc_snapshot_batch sb_filter ON base.id = sb_filter.snapshot_id"
        where_clause.append("sb_filter.batch_id = :batch_id")

    where_sql = " AND ".join(where_clause) if where_clause else "1=1"

    sql = f"""
        SELECT 
            SUM(sb.batch_count) AS total_batches,
            SUM(
                CASE WHEN i.abnormal_items > 0 THEN sb.batch_count ELSE 0 END
            ) AS abnormal_batches,
            ROUND(
                1.0 - SUM(
                    CASE WHEN i.abnormal_items > 0 THEN sb.batch_count ELSE 0 END
                )::decimal / NULLIF(SUM(sb.batch_count), 0), 4
            ) AS batch_pass_rate,
            COUNT(DISTINCT ins.inspector_id) AS total_personnel,
            SUM(i.total_items) AS total_items,
            SUM(i.abnormal_items) AS abnormal_items,
            ROUND(
                1.0 - SUM(i.abnormal_items)::decimal / NULLIF(SUM(i.total_items), 0), 4
            ) AS item_pass_rate
        FROM quality_management.qc_snapshot_base base
        {join_clause}
        WHERE {where_sql}
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

def get_kpi_by_inspector(start_date: Optional[str], end_date: Optional[str],
                         team_id: Optional[int], shift_id: Optional[int],
                         product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    joins = [
        "JOIN quality_management.qc_snapshot_inspector ins ON base.id = ins.snapshot_id",
        "JOIN quality_management.qc_snapshot_item i ON base.id = i.snapshot_id"
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
            COUNT(DISTINCT base.id) AS forms_submitted,
            SUM(i.total_count) AS total_items_checked,
            SUM(i.abnormal_count) AS abnormal_items,
            ROUND(
                SUM(i.abnormal_count)::decimal / NULLIF(SUM(i.total_count), 0), 4
            ) AS abnormal_rate
        FROM quality_management.qc_snapshot_base base
        {join_clause}
        WHERE {where_clause}
        GROUP BY ins.inspector_id, ins.inspector_name
        ORDER BY forms_submitted DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=locals())

def get_retest_records(start_date: Optional[str], end_date: Optional[str],
                       team_id: Optional[int], shift_id: Optional[int],
                       product_id: Optional[int], batch_id: Optional[int]) -> pd.DataFrame:
    where_clauses = []

    if start_date:
        where_clauses.append("created_at::date >= :start_date")
    if end_date:
        where_clauses.append("created_at::date <= :end_date")
    if team_id is not None:
        where_clauses.append(":team_id = ANY(related_team_ids)")
    if shift_id is not None:
        where_clauses.append(":shift_id = ANY(related_shift_ids)")
    if product_id is not None:
        where_clauses.append(":product_id = ANY(related_product_ids)")
    if batch_id is not None:
        where_clauses.append(":batch_id = ANY(related_batch_ids)")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
        SELECT 
            qc_form_template_id,
            qc_form_template_name,
            comments,
            approver_id,
            approver_name,
            ARRAY_TO_STRING(related_products::text[], ',') AS related_products,
            ARRAY_TO_STRING(related_batches::text[], ',') AS related_batches,
            ARRAY_TO_STRING(related_teams::text[], ',') AS related_teams,
            ARRAY_TO_STRING(related_inspectors::text[], ',') AS related_inspectors,
            ARRAY_TO_STRING(related_shifts::text[], ',') AS related_shifts,
            submission_id,
            collection_name
        FROM quality_management.qc_snapshot_retest
        WHERE {where_sql}
        ORDER BY created_at DESC
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

    print("产品批次异常统计图：")
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

    df3_5 = get_abnormal_ratio_by_field_grouped_other(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )

    print("异常类型占比饼图 升级版：")
    print(df3_5)

    df_cards = get_summary_card_stats(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )
    print("卡片统计汇总信息：")
    print(df_cards)

    df_kpi = get_kpi_by_inspector(
        start_date="2025-05-26",
        end_date="2025-05-26",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )
    print("人员kpi汇总信息：")
    print(df_kpi)

    df_retest = get_retest_records(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=None,
        shift_id=None,
        product_id=None,
        batch_id=None
    )
    print("复检记录列表：")
    print(df_retest)

