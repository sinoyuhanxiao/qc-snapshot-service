"""
test_insert_snapshot_from_mongo.py

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

# PostgreSQL & MongoDB setup
PG_CONN = psycopg2.connect(**DB_CONFIG)
PG_CURSOR = PG_CONN.cursor()
mongo_db = get_mongo_db()


def insert_snapshot_base(start_time, end_time, form_template_id, form_template_name):
    PG_CURSOR.execute("""
        INSERT INTO quality_management.qc_snapshot_base (
            snapshot_time, start_time, end_time, qc_form_template_id, qc_form_template_name
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (end_time, start_time, end_time, form_template_id, form_template_name))
    return PG_CURSOR.fetchone()[0]


def insert_snapshot_items(snapshot_id, start_time, end_time, form_template_id, mongo_collection):
    mapping_doc = mongo_db["form_template_key_label_pairs"].find_one({"qc_form_template_id": form_template_id})
    if not mapping_doc:
        print(f"❌ No snapshot items that is alerted found for template {form_template_id}")
        return

    fields = mapping_doc.get("fields", [])
    print(f"🕒 Snapshot time window: {start_time} → {end_time}")

    for field in fields:
        key = field["key"]
        label = field["label"]

        total_count = mongo_collection.count_documents({
            key: {"$exists": True},
            "created_at": {"$gte": start_time, "$lte": end_time}
        })

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

    start_time, end_time = get_snapshot_time_window()
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
    insert_snapshot_items(snapshot_id, start_time, end_time, form_template_id, mongo_db[collection_name])

    PG_CONN.commit()
    print("🎉 Snapshot inserted for one document.\n")


def main():
    collections = get_recent_qc_collections(SNAPSHOT_TIME_WINDOW_MINUTES)

    for collection_name in collections:
        print(f"\n📂 Processing collection: {collection_name}")
        mongo_collection = mongo_db[collection_name]
        cursor = mongo_collection.find({
            "created_at": {
                "$gte": datetime.utcnow() - timedelta(minutes=SNAPSHOT_TIME_WINDOW_MINUTES)
            }
        })

        template_groups = defaultdict(list)

        for doc in cursor:
            form_template_id = int(collection_name.split("_")[2])
            template_groups[form_template_id].append((doc, collection_name))

        for form_template_id, docs in template_groups.items():
            process_template_group(form_template_id, docs)

def process_template_group(form_template_id, doc_tuples):
    from utils.time_utils import get_snapshot_time_window
    start_time, end_time = get_snapshot_time_window()

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
        insert_snapshot_items(snapshot_id, start_time, end_time, form_template_id, mongo_collection)

    PG_CONN.commit()

    print(
        f"🧾 Summary for form_template_id {form_template_id}: "
        f"{len(batch_ids)} batches, {len(product_ids)} products, "
        f"{len(shift_ids)} shifts, {len(team_ids)} teams, "
        f"{len(inspector_ids)} inspectors, from {len(collection_names_seen)} collections."
    )

if __name__ == "__main__":
    main()
