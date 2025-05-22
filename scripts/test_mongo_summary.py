"""
test_mongo_summary.py

Test: Provides utility functions to count:
1. Total form submissions over a time window
2. Unique related IDs (batches, products, inspectors)

Used for statistical overview of QC activity in MongoDB.

Author: Erik Yu
"""

from datetime import datetime, timedelta
from db.mongo import get_mongo_db
from utils.utils import get_relevant_collection_suffixes

def count_form_submissions(minutes=10000):
    """Count total number of documents submitted in the past `minutes`."""
    db = get_mongo_db()
    time_threshold = datetime.utcnow() - timedelta(minutes=minutes)
    suffixes = get_relevant_collection_suffixes(minutes)

    collections = [
        name for name in db.list_collection_names()
        if name.startswith("form_template_") and any(suffix in name for suffix in suffixes)
    ]

    total_submissions = 0
    for coll_name in collections:
        count = db[coll_name].count_documents({ "created_at": { "$gte": time_threshold } })
        print(f"📄 {coll_name} 提交表单数: {count}")
        total_submissions += count

    print(f"\n✅ 总提交表单数（过去 {minutes} 分钟）: {total_submissions}")
    return total_submissions


def count_unique_ids_from_fields(minutes=10000):
    """Count unique batch/product/inspector IDs over the past `minutes`."""
    db = get_mongo_db()
    time_threshold = datetime.utcnow() - timedelta(minutes=minutes)
    suffixes = get_relevant_collection_suffixes(minutes)

    collections = [
        name for name in db.list_collection_names()
        if name.startswith("form_template_") and any(suffix in name for suffix in suffixes)
    ]

    batch_ids = set()
    product_ids = set()
    inspector_ids = set()

    for coll_name in collections:
        cursor = db[coll_name].find({"created_at": {"$gte": time_threshold}})
        for doc in cursor:
            batch_ids.update(doc.get("related_batch_ids", []))
            product_ids.update(doc.get("related_product_ids", []))
            inspector_ids.update(doc.get("related_inspector_ids", []))

    print(f"\n✅ 总检测批次数（去重）: {len(batch_ids)}")
    print(f"   IDs: {sorted(batch_ids)}")
    print(f"\n✅ 总检测产品数（去重）: {len(product_ids)}")
    print(f"   IDs: {sorted(product_ids)}")
    print(f"\n✅ 总质检人员数（去重）: {len(inspector_ids)}")
    print(f"   IDs: {sorted(inspector_ids)}")

    return {
        "batch_count": len(batch_ids),
        "batch_ids": sorted(batch_ids),
        "product_count": len(product_ids),
        "product_ids": sorted(product_ids),
        "inspector_count": len(inspector_ids),
        "inspector_ids": sorted(inspector_ids)
    }


if __name__ == "__main__":
    count_form_submissions(10000)
    count_unique_ids_from_fields(10000)
