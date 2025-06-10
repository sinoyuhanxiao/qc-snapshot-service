from pydantic import BaseModel
from typing import Optional, Dict

class ExportPdfRequest(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    team_id: Optional[int] = None
    shift_id: Optional[int] = None
    product_id: Optional[int] = None
    batch_id: Optional[int] = None
    timezone: Optional[str] = "UTC"
    charts: Optional[Dict[str, str]] = None  # safer than empty dict default
