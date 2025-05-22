"""
test_change_time.py

This script traverses all MongoDB collections prefixed with "form_template_",
finds documents where `created_at` is stored as a string, and converts them
to proper ISO `datetime` objects in place.

Useful for fixing legacy data inconsistencies before running snapshot services.

Author: Erik Yu
"""

from datetime import datetime
from db.mongo import get_mongo_db


def fix_created_at_format():
    db = get_mongo_db()
    collections = db.list_collection_names()

    for coll_name in collections:
        if not coll_name.startswith("form_template_"):
            continue

        coll = db[coll_name]
        cursor = coll.find({"created_at": {"$type": "string"}})
        count = 0

        for doc in cursor:
            try:
                iso_str = doc["created_at"].strip().replace("Z", "")  # clean up ISO string
                new_date = datetime.fromisoformat(iso_str)
                coll.update_one({"_id": doc["_id"]}, {"$set": {"created_at": new_date}})
                count += 1
            except Exception as e:
                print(f"⚠️ Failed to convert {doc['_id']}: {e}")

        print(f"✅ {coll_name} converted {count} documents")

    print("\n🎉 All done.")


if __name__ == "__main__":
    fix_created_at_format()
