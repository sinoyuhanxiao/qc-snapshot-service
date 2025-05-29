# utils/document_formatter.py
import copy
import json
from pymongo import MongoClient
from bson import ObjectId
from sqlalchemy import text
from db.postgres import pg_engine


def get_form_template_json(form_id: int):
    with pg_engine.connect() as conn:
        result = conn.execute(text("SELECT form_template_json FROM quality_management.qc_form_template WHERE id = :id"),
                              {"id": form_id}).fetchone()
        if not result or not result[0]:
            raise ValueError(f"No form template found for formId {form_id}")
        return json.loads(result[0])


def build_key_label_mapping(widget_list, key_map, option_item_map, field_divider_map, current_divider="uncategorized"):
    for widget in widget_list:
        widget_type = widget.get("type")
        options = widget.get("options", {})

        if widget_type == "divider" and "label" in options:
            current_divider = options["label"]
        elif widget_type == "grid":
            for col in widget.get("cols", []):
                build_key_label_mapping(col.get("widgetList", []), key_map, option_item_map, field_divider_map,
                                        current_divider)
        elif "name" in options and "label" in options:
            name, label = options["name"], options["label"]
            key_map[name] = label
            field_divider_map[name] = current_divider

            if "optionItems" in options:
                option_item_map[label] = {str(opt["value"]): opt["label"] for opt in options["optionItems"]}

        if "widgetList" in widget:
            build_key_label_mapping(widget["widgetList"], key_map, option_item_map, field_divider_map, current_divider)


def format_single_document(document, form_id):
    document = copy.deepcopy(document)
    template = get_form_template_json(form_id)
    widget_list = template.get("widgetList", [])

    key_map, option_item_map, field_divider_map = {}, {}, {}
    build_key_label_mapping(widget_list, key_map, option_item_map, field_divider_map)

    reserved_keys = {"_id", "created_at", "created_by"}
    grouped_data = {}
    formatted_doc = {}

    for key, value in document.items():
        formatted_key = key_map.get(key, key)
        divider = field_divider_map.get(key, "uncategorized")

        # Format optionItems
        if formatted_key in option_item_map:
            mapping = option_item_map[formatted_key]
            if isinstance(value, list):
                value = [mapping.get(str(v), str(v)) for v in value]
            else:
                value = mapping.get(str(value), value)

        if key in reserved_keys:
            formatted_doc[formatted_key] = value
        else:
            grouped_data.setdefault(divider, {})[formatted_key] = value

    # Convert exceeded_info
    if "exceeded_info" in document:
        formatted_exceeded = {}
        for key, val in document["exceeded_info"].items():
            formatted_key = key_map.get(key, key)
            formatted_exceeded[formatted_key] = val
        formatted_doc["exceeded_info"] = formatted_exceeded

    formatted_doc.update(grouped_data)
    return copy.deepcopy(formatted_doc)
