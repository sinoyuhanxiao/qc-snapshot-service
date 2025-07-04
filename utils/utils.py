from datetime import datetime, timedelta
from db.mongo import get_mongo_db

def get_relevant_collection_suffixes(minutes: int):
    now = datetime.utcnow()
    earliest_time = now - timedelta(minutes=minutes)

    suffixes = set()

    # 从 earliest_time 到 now，逐月收集 YYYYMM
    current = datetime(earliest_time.year, earliest_time.month, 1)
    while current <= now:
        suffixes.add(current.strftime("%Y%m"))
        # 跳到下个月
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)

    return sorted(suffixes)

def get_recent_qc_collections(minutes: int):
    db = get_mongo_db()
    suffixes = get_relevant_collection_suffixes(minutes)

    collections = [
        name for name in db.list_collection_names()
        if name.startswith("form_template_") and any(suffix in name for suffix in suffixes)
    ]
    return collections

def clean_float_json(df):
    return df.fillna(0).replace([float('inf'), float('-inf')], 0).to_dict(orient="records")

if __name__ == "__main__":
    print(get_relevant_collection_suffixes(1000000))