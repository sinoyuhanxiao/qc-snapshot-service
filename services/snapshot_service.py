# services/snapshot_service.py

from db.postgres import get_postgres_connection, get_name_by_id

def insert_product_snapshot(cursor, snapshot_id, product_ids):
    for product_id in product_ids:
        name = get_name_by_id(cursor, "qc_suggested_product", "id", "name", product_id)
        cursor.execute("""
            INSERT INTO quality_management.qc_snapshot_product (snapshot_id, product_id, product_name)
            VALUES (%s, %s, %s)
        """, (snapshot_id, product_id, name))


def insert_batch_snapshot(cursor, snapshot_id, batch_ids):
    for batch_id in batch_ids:
        code = get_name_by_id(cursor, "qc_suggested_batch", "id", "code", batch_id)
        cursor.execute("""
            INSERT INTO quality_management.qc_snapshot_batch (snapshot_id, batch_id, batch_code)
            VALUES (%s, %s, %s)
        """, (snapshot_id, batch_id, code))


def insert_shift_snapshot(cursor, snapshot_id, shift_ids):
    for shift_id in shift_ids:
        name = get_name_by_id(cursor, "shift", "id", "name", shift_id)
        cursor.execute("""
            INSERT INTO quality_management.qc_snapshot_shift (snapshot_id, shift_id, shift_name)
            VALUES (%s, %s, %s)
        """, (snapshot_id, shift_id, name))


def insert_team_snapshot(cursor, snapshot_id, team_ids):
    for team_id in team_ids:
        name = get_name_by_id(cursor, "team", "id", "name", team_id)
        cursor.execute("""
            INSERT INTO quality_management.qc_snapshot_team (snapshot_id, team_id, team_name)
            VALUES (%s, %s, %s)
        """, (snapshot_id, team_id, name))


def insert_inspector_snapshot(cursor, snapshot_id, inspector_ids):
    for inspector_id in inspector_ids:
        name = get_name_by_id(cursor, "user", "id", "name", inspector_id)
        cursor.execute("""
            INSERT INTO quality_management.qc_snapshot_inspector (snapshot_id, inspector_id, inspector_name)
            VALUES (%s, %s, %s)
        """, (snapshot_id, inspector_id, name))
