from bson import ObjectId
from datetime import datetime

def make_json_safe(obj):
    def clean(o, visited=None):
        if visited is None:
            visited = set()
        oid = id(o)
        if oid in visited:
            return "circular_ref"
        visited.add(oid)

        if isinstance(o, dict):
            return {str(k): clean(v, visited.copy()) for k, v in o.items()}
        elif isinstance(o, list):
            return [clean(i, visited.copy()) for i in o]
        elif isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(o, (str, int, float, bool)) or o is None:
            return o
        else:
            return str(o)

    return clean(obj)