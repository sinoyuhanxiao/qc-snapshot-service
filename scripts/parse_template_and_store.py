"""
Extract and store key-label mappings from QC form templates into MongoDB.

This script connects to PostgreSQL to fetch dynamic form templates from
`quality_management.qc_form_template`, parses each template's `widgetList`
to extract input field `name` and `label`, and stores the results into
MongoDB collection `form_template_key_label_pairs`.

The resulting collection acts as a reusable lookup for dynamic snapshot
processing, especially for understanding what each field means in human terms.

Usage:
    python scripts/parse_form_template_fields.py

Example MongoDB Output:
    {
        "qc_form_template_id": 368,
        "fields": [
            { "key": "moisture", "label": "水分" },
            { "key": "sense_it", "label": "感官指标" },
            ...
        ]
    }

Author: Erik Yu
"""

# scripts/parse_form_template_fields.py
import json
from db.postgres import get_postgres_connection
from db.mongo import get_mongo_db

def extract_input_key_label_pairs(widget_list, results):
    for widget in widget_list:
        if widget.get("formItemFlag") is True:
            options = widget.get("options", {})
            key = options.get("name")
            label = options.get("label")
            if key and label:
                results.append({"key": key, "label": label})

        # Recurse into nested structures
        if "widgetList" in widget:
            extract_input_key_label_pairs(widget["widgetList"], results)

        if "cols" in widget:
            for col in widget.get("cols", []):
                if "widgetList" in col:
                    extract_input_key_label_pairs(col["widgetList"], results)


def main():
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    mongo_db = get_mongo_db()
    output_collection = mongo_db["form_template_key_label_pairs"]

    cursor.execute("""
        SELECT id, form_template_json
        FROM quality_management.qc_form_template
        WHERE form_template_json IS NOT NULL
    """)
    rows = cursor.fetchall()

    for form_template_id, form_json_str in rows:
        try:
            form_json = json.loads(form_json_str)
            widget_list = form_json.get("widgetList", [])

            key_label_results = []
            extract_input_key_label_pairs(widget_list, key_label_results)

            mongo_doc = {
                "qc_form_template_id": form_template_id,
                "fields": key_label_results
            }

            output_collection.replace_one(
                {"qc_form_template_id": form_template_id},
                mongo_doc,
                upsert=True
            )

            print(f"✅ Stored {len(key_label_results)} fields for template {form_template_id}")
        except Exception as e:
            print(f"❌ Error on template {form_template_id}: {e}")

    cursor.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
