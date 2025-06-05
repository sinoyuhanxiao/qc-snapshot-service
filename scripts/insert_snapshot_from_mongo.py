"""
insert_snapshot_from_mongo.py

This script loops through all valid MongoDB form collections (e.g., form_template_368_202505),
extracts related QC metadata from each document, and inserts corresponding snapshot records
into PostgreSQL snapshot tables.

It supports:
- Auto-fetch of qc_form_template_name from PostgreSQL
- Snapshot base + junctions (batch/product/shift/inspector/team)
- Snapshot item statistics (total + abnormal)

Author: Erik Yu
"""

from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import psycopg2
from bson import ObjectId
from config.settings import SNAPSHOT_TIME_WINDOW_MINUTES

from config.db_config import DB_CONFIG, MONGO_URI
from db.postgres import get_name_by_id
from db.mongo import get_mongo_db
from services.snapshot_service import (
    insert_product_snapshot,
    insert_batch_snapshot,
    insert_shift_snapshot,
    insert_team_snapshot,
    insert_inspector_snapshot
)
from utils.utils import get_recent_qc_collections
from utils.time_utils import get_snapshot_time_window
from collections import defaultdict
from loguru import logger

# PostgreSQL & MongoDB setup
PG_CONN = psycopg2.connect(**DB_CONFIG)
PG_CURSOR = PG_CONN.cursor()
mongo_db = get_mongo_db()

# Global start time and end time
start_time = None
end_time = None
inserted_retest_count = 0
inserted_snapshot_count = 0

def initialize_snapshot_time_range():
    global start_time, end_time
    PG_CURSOR.execute("""
        SELECT end_at 
        FROM quality_management.qc_snapshot_trigger_log 
        ORDER BY id DESC 
        LIMIT 1
    """)
    last_triggered_at = PG_CURSOR.fetchone()[0]

    if last_triggered_at and last_triggered_at.tzinfo:
        last_triggered_at = last_triggered_at.astimezone(timezone.utc)

    if not last_triggered_at:
        last_triggered_at = datetime.now(timezone.utc) - timedelta(minutes=SNAPSHOT_TIME_WINDOW_MINUTES)

    if last_triggered_at.tzinfo is None:
        last_triggered_at = last_triggered_at.replace(tzinfo=timezone.utc)

    start_time = last_triggered_at
    end_time = datetime.now(timezone.utc)

    print(f"🕒 Snapshot time window set from {start_time} to {end_time}")

def insert_snapshot_base(start_time, end_time, form_template_id, form_template_name):
    PG_CURSOR.execute("""
        INSERT INTO quality_management.qc_snapshot_base (
            snapshot_time, start_time, end_time, qc_form_template_id, qc_form_template_name
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (end_time, start_time, end_time, form_template_id, form_template_name))
    return PG_CURSOR.fetchone()[0]

def insert_qc_snapshot_retest(row: dict):
    keys = [
        "qc_form_template_id", "qc_form_template_name",
        "approver_id", "approver_name", "comments", "created_at",
        "related_product_ids", "related_products",
        "related_batch_ids", "related_batches",
        "related_team_ids", "related_teams",
        "related_inspector_ids", "related_inspectors",
        "related_shift_ids", "related_shifts",
        "submission_id", "collection_name"   # ✅ 新增字段
    ]
    values = [row.get(k) for k in keys]

    PG_CURSOR.execute(f"""
        INSERT INTO quality_management.qc_snapshot_retest (
            {', '.join(keys)}
        ) VALUES ({', '.join(['%s'] * len(keys))})
    """, values)

    logger.info(f"[RETEST] Inserted retest: submission_id={row.get('submission_id')}, collection={row.get('collection_name')}")

def insert_snapshot_retest_from_mongo():
    global inserted_retest_count
    global start_time, end_time
    collections = get_recent_qc_collections(SNAPSHOT_TIME_WINDOW_MINUTES)

    for collection_name in collections:
        print(collection_name)
        if not collection_name.startswith("form_template_"):
            continue

        try:
            form_template_id = int(collection_name.split("_")[2])
        except Exception:
            continue

        form_template_name = get_name_by_id(PG_CURSOR, "qc_form_template", "id", "name", form_template_id)
        if not form_template_name:
            continue

        mongo_collection = mongo_db[collection_name]
        cursor = mongo_collection.find({
            "approver_updated_at": {"$gte": start_time, "$lte": end_time}
        })

        for doc in cursor:
            approval_info = doc.get("approval_info", [])
            for approval in approval_info:
                if approval.get("suggest_retest") is True:
                    row = {
                        "qc_form_template_id": form_template_id,
                        "qc_form_template_name": form_template_name,
                        "approver_id": approval.get("user_id"),
                        "approver_name": approval.get("user_name"),
                        "comments": approval.get("comments"),
                        "created_at": approval.get("timestamp"),
                        "related_product_ids": doc.get("related_product_ids", []),
                        "related_products": [p.strip() for p in
                                             doc.get("related_products", "").split(",")] if doc.get(
                            "related_products") else [],
                        "related_batch_ids": doc.get("related_batch_ids", []),
                        "related_batches": [b.strip() for b in
                                            doc.get("related_batches", "").split(",")] if doc.get(
                            "related_batches") else [],
                        "related_team_ids": [doc.get("related_team_id")] if doc.get("related_team_id") else [],
                        "related_teams": [doc.get("related_teams")] if doc.get("related_teams") else [],
                        "related_inspector_ids": doc.get("related_inspector_ids", []),
                        "related_inspectors": [i.strip() for i in
                                               doc.get("related_inspectors", "").split(",")] if doc.get(
                            "related_inspectors") else [],
                        "related_shift_ids": [doc.get("related_shift_id")] if doc.get("related_shift_id") else [],
                        "related_shifts": [doc.get("related_shifts")] if doc.get("related_shifts") else [],
                        "submission_id": str(doc.get("_id")),
                        "collection_name": collection_name
                    }
                    insert_qc_snapshot_retest(row)
                    inserted_retest_count += 1
                    print(
                        f"✅ Retest inserted for template {form_template_id}, approver {approval.get('user_name')}")
                    break

    PG_CONN.commit()

def insert_snapshot_items(snapshot_id, form_template_id, mongo_collection):
    mapping_doc = mongo_db["form_template_key_label_pairs"].find_one({"qc_form_template_id": form_template_id})
    if not mapping_doc:
        print(f"❌ No snapshot items that is alerted found for template {form_template_id}")
        return

    fields = mapping_doc.get("fields", [])
    global start_time, end_time

    for field in fields:
        key = field["key"]
        label = field["label"]

        total_count = mongo_collection.count_documents({
            key: {"$exists": True},
            "created_at": {"$gte": start_time, "$lte": end_time}
        })

        # Debug print
        print(f"🧪 Checking abnormal count for form_template_id={form_template_id}, key={key}, range=({start_time}, {end_time})")

        PG_CURSOR.execute("""
            SELECT COUNT(*) FROM quality_management.qc_alert_record
            WHERE qc_form_template_id = %s
              AND inspection_item_key = %s
              AND created_at BETWEEN %s AND %s
        """, (form_template_id, key, start_time, end_time))
        abnormal_count = PG_CURSOR.fetchone()[0]

        PG_CURSOR.execute(""" 
            INSERT INTO quality_management.qc_snapshot_item (
                snapshot_id, key, label, total_count, abnormal_count
            ) VALUES (%s, %s, %s, %s, %s)
        """, (snapshot_id, key, label, total_count, abnormal_count))

    print(f"📦 Inserted {len(fields)} snapshot items for template {form_template_id}")


def process_document(doc, collection_name):
    try:
        form_template_id = int(collection_name.split("_")[2])
    except Exception:
        print(f"⚠️ Cannot extract form_template_id from {collection_name}")
        return

    created_at = doc.get("created_at")
    if isinstance(created_at, dict) and "$date" in created_at:
        created_at = datetime.fromisoformat(created_at["$date"].replace("Z", "+00:00"))
    elif isinstance(created_at, str):
        created_at = datetime.fromisoformat(created_at)
    else:
        created_at = datetime.utcnow()

    # Fetch form name from qc_form_template
    form_template_name = get_name_by_id(PG_CURSOR, "qc_form_template", "id", "name", form_template_id)
    if not form_template_name:
        print(f"❌ Cannot find form_template_name for ID {form_template_id}")
        return

    global start_time, end_time
    snapshot_id = insert_snapshot_base(start_time, end_time, form_template_id, form_template_name)
    print(f"✅ Inserted snapshot_base for template {form_template_id} with id {snapshot_id}")

    insert_batch_snapshot(PG_CURSOR, snapshot_id, doc.get("related_batch_ids", []))
    insert_product_snapshot(PG_CURSOR, snapshot_id, doc.get("related_product_ids", []))

    shift_id = doc.get("related_shift_id")
    if shift_id:
        insert_shift_snapshot(PG_CURSOR, snapshot_id, [shift_id])

    team_id = doc.get("related_team_id")
    if team_id:
        insert_team_snapshot(PG_CURSOR, snapshot_id, [team_id])

    inspector_ids = doc.get("related_inspector_ids", [])
    insert_inspector_snapshot(PG_CURSOR, snapshot_id, inspector_ids)

    start_time, end_time = get_snapshot_time_window()
    insert_snapshot_items(snapshot_id, form_template_id, mongo_db[collection_name])

    PG_CONN.commit()
    print("🎉 Snapshot inserted for one document.\n")


def main():
    global start_time, end_time
    global inserted_retest_count
    inserted_retest_count = 0
    collections = get_recent_qc_collections(SNAPSHOT_TIME_WINDOW_MINUTES)

    for collection_name in collections:
        print(f"\n📂 Processing collection: {collection_name}")
        mongo_collection = mongo_db[collection_name]
        cursor = mongo_collection.find({
            "created_at": {
                "$gte": start_time,
                "$lte": end_time
            }
        })

        template_groups = defaultdict(list)

        for doc in cursor:
            form_template_id = int(collection_name.split("_")[2])
            template_groups[form_template_id].append((doc, collection_name))

        for form_template_id, docs in template_groups.items():
            process_template_group(form_template_id, docs)

    # 增加复检记录插入
    insert_snapshot_retest_from_mongo()

    # Insert snapshot trigger log
    PG_CURSOR.execute("""
        INSERT INTO quality_management.qc_snapshot_trigger_log (start_at, end_at, note)
        VALUES (%s, %s, %s)
    """, (
        start_time,
        end_time,
        f"Inserted {inserted_snapshot_count} snapshots, {inserted_retest_count} retests"
    ))

    PG_CONN.commit()

def process_template_group(form_template_id, doc_tuples):
    from utils.time_utils import get_snapshot_time_window
    global start_time, end_time
    global inserted_snapshot_count
    inserted_snapshot_count += 1
    # Fetch form name
    form_template_name = get_name_by_id(PG_CURSOR, "qc_form_template", "id", "name", form_template_id)
    if not form_template_name:
        print(f"❌ Cannot find form_template_name for ID {form_template_id}")
        return

    # Insert one base row
    snapshot_id = insert_snapshot_base(start_time, end_time, form_template_id, form_template_name)
    print(f"✅ Inserted snapshot_base for template {form_template_id} with id {snapshot_id}")

    # Aggregate related fields
    product_ids = set()
    batch_ids = set()
    shift_ids = set()
    team_ids = set()
    inspector_ids = set()

    for doc, _ in doc_tuples:
        product_ids.update(doc.get("related_product_ids", []))
        batch_ids.update(doc.get("related_batch_ids", []))
        inspector_ids.update(doc.get("related_inspector_ids", []))

        shift_id = doc.get("related_shift_id")
        if shift_id: shift_ids.add(shift_id)

        team_id = doc.get("related_team_id")
        if team_id: team_ids.add(team_id)

    insert_batch_snapshot(PG_CURSOR, snapshot_id, list(batch_ids))
    insert_product_snapshot(PG_CURSOR, snapshot_id, list(product_ids))
    insert_shift_snapshot(PG_CURSOR, snapshot_id, list(shift_ids))
    insert_team_snapshot(PG_CURSOR, snapshot_id, list(team_ids))
    insert_inspector_snapshot(PG_CURSOR, snapshot_id, list(inspector_ids))

    # Track total fields inserted (to print final summary)
    field_total = 0
    collection_names_seen = set()

    for _, collection_name in doc_tuples:
        if collection_name in collection_names_seen:
            continue  # prevent double-counting same collection
        collection_names_seen.add(collection_name)

        mongo_collection = mongo_db[collection_name]
        insert_snapshot_items(snapshot_id, form_template_id, mongo_collection)

    PG_CONN.commit()

    print(
        f"🧾 Summary for form_template_id {form_template_id}: "
        f"{len(batch_ids)} batches, {len(product_ids)} products, "
        f"{len(shift_ids)} shifts, {len(team_ids)} teams, "
        f"{len(inspector_ids)} inspectors, from {len(collection_names_seen)} collections."
    )

def run_manual_snapshot():
    global start_time, end_time, inserted_snapshot_count, inserted_retest_count
    inserted_snapshot_count = 0
    inserted_retest_count = 0

    initialize_snapshot_time_range()
    snapshot_start = start_time
    snapshot_end = end_time

    collections = get_recent_qc_collections(SNAPSHOT_TIME_WINDOW_MINUTES)

    for collection_name in collections:
        print(f"\n📂 Processing collection: {collection_name}")
        mongo_collection = mongo_db[collection_name]
        cursor = mongo_collection.find({
            "created_at": {
                "$gte": start_time,
                "$lte": end_time
            }
        })

        template_groups = defaultdict(list)
        for doc in cursor:
            try:
                form_template_id = int(collection_name.split("_")[2])
            except Exception:
                continue
            template_groups[form_template_id].append((doc, collection_name))

        for form_template_id, docs in template_groups.items():
            process_template_group(form_template_id, docs)

    # Insert retest records
    insert_snapshot_retest_from_mongo()

    # Log snapshot trigger with is_manual = 1
    PG_CURSOR.execute("""
        INSERT INTO quality_management.qc_snapshot_trigger_log (start_at, end_at, note, is_manual)
        VALUES (%s, %s, %s, %s)
    """, (
        snapshot_start,
        snapshot_end,
        f"Inserted {inserted_snapshot_count} snapshots, {inserted_retest_count} retests",
        1
    ))
    PG_CONN.commit()
    print("✅ Manual snapshot run completed.")

import schedule
import time

def job():
    print(f"⏳ Scheduled snapshot job triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — setting is every {SNAPSHOT_TIME_WINDOW_MINUTES} minutes")
    initialize_snapshot_time_range()
    actual_minutes = round((end_time - start_time).total_seconds() / 60, 2)
    print(f"🧮 Actual snapshot range = {actual_minutes} minutes ({start_time} ~ {end_time})")
    main()

# Schedule the job every Snap shot time minutes
schedule.every(SNAPSHOT_TIME_WINDOW_MINUTES).minutes.do(job)


if __name__ == "__main__":
    job()  # Run immediately once
    while True:
        schedule.run_pending()
        time.sleep(1)

# if __name__ == "__main__":
#     print("🔍 Running standalone retest insert test...")
#     insert_snapshot_retest_from_mongo()
#     print("✅ Retest insert test completed.")