# services/document_export_service.py
import copy

from db.mongo import get_mongo_db
from db.postgres import pg_engine
from sqlalchemy import text
from datetime import datetime
from utils.utils import get_recent_qc_collections
import json
from pathlib import Path
from typing import Optional
from utils.document_formatter import format_single_document

def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def make_json_safe(obj):
    """
    Recursively sanitize object to be JSON serializable without circular refs.
    Includes bson.ObjectId and datetime.
    """
    from bson import ObjectId
    from datetime import datetime

    def clean(o, visited=None):
        if visited is None:
            visited = set()

        oid = id(o)
        if oid in visited:
            return "circular_ref"
        visited.add(oid)

        if isinstance(o, dict):
            return {str(k): clean(v, visited) for k, v in o.items()}
        elif isinstance(o, list):
            return [clean(i, visited) for i in o]
        elif isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(o, (str, int, float, bool)) or o is None:
            return o
        else:
            return str(o)

    return clean(obj)

def fetch_documents_by_time_range(start_date: str, end_date: str,
                                   team_id: Optional[int] = None,
                                   shift_id: Optional[int] = None,
                                   product_id: Optional[int] = None,
                                   batch_id: Optional[int] = None):
    """
    Fetch documents from all sharded MongoDB form collections by created_at,
    and append qc_form_template_id and qc_form_template_name to each doc.
    Filters: team_id, shift_id, product_id, batch_id (each compared against related_XXX fields)
    """
    db = get_mongo_db()
    collections = get_recent_qc_collections(999999)

    with pg_engine.connect() as conn:
        sql = """
            SELECT id, name FROM quality_management.qc_form_template
        """
        form_templates = conn.execute(text(sql)).fetchall()
        template_map = {int(row[0]): row[1] for row in form_templates}

    # Load key-label mappings
    key_label_map = {}
    mapping_collection = db["form_template_key_label_pairs"]
    for doc in mapping_collection.find():
        template_id = int(doc.get("qc_form_template_id"))
        key_label_map[template_id] = {
            item["key"]: item.get("label", item["key"])
            for item in doc.get("fields", [])
        }

    documents = []
    for collection_name in collections:
        if not collection_name.startswith("form_template_"):
            continue
        try:
            form_template_id = int(collection_name.split("_")[2])
        except Exception:
            continue

        form_template_name = template_map.get(form_template_id)
        if not form_template_name:
            continue

        if collection_name not in db.list_collection_names():
            continue

        collection = db[collection_name]
        cursor = collection.find({
            "created_at": {
                "$gte": datetime.fromisoformat(start_date),
                "$lte": datetime.fromisoformat(end_date)
            }
        })

        for raw_doc in cursor:
            doc = copy.deepcopy(raw_doc)
            if team_id and doc.get("related_team_id") != team_id:
                continue
            if shift_id and doc.get("related_shift_id") != shift_id:
                continue
            if product_id and product_id not in doc.get("related_product_ids", []):
                continue
            if batch_id and batch_id not in doc.get("related_batch_ids", []):
                continue

            doc["qc_form_template_id"] = form_template_id
            doc["qc_form_template_name"] = form_template_name

            formatted = format_single_document(doc, form_template_id)
            documents.append(formatted)
    return documents

def run_and_export_documents(start_date: str, end_date: str,
                              team_id: Optional[int] = None,
                              shift_id: Optional[int] = None,
                              product_id: Optional[int] = None,
                              batch_id: Optional[int] = None):
    """
    Run the fetch function and write the results to a JSON file under assets/test.
    """
    docs = fetch_documents_by_time_range(
        start_date=start_date,
        end_date=end_date,
        team_id=team_id,
        shift_id=shift_id,
        product_id=product_id,
        batch_id=batch_id
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path("../assets/test")
    output_path.mkdir(parents=True, exist_ok=True)
    full_path = output_path / f"exported_docs_{timestamp}.json"

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=convert_datetime)

    print(f"✅ Exported {len(docs)} documents to {full_path}")


def get_documents_list(start_date: str, end_date: str,
                       team_id: Optional[int] = None,
                       shift_id: Optional[int] = None,
                       product_id: Optional[int] = None,
                       batch_id: Optional[int] = None):
    """
    Just return the list of documents as JSON serializable data for API endpoint.
    """
    docs = fetch_documents_by_time_range(
        start_date=start_date,
        end_date=end_date,
        team_id=team_id,
        shift_id=shift_id,
        product_id=product_id,
        batch_id=batch_id
    )
    safe_docs = make_json_safe(docs)
    return safe_docs


if __name__ == "__main__":
    run_and_export_documents(
        start_date="2025-05-01",
        end_date="2025-05-31",
        team_id=152,
        shift_id=2,
        product_id=None,
        batch_id=None
    )
