from pydantic import BaseModel
from typing import Optional, Dict

class ExportPdfRequest(BaseModel):
    start_date: Optional[str]
    end_date: Optional[str]
    team_id: Optional[int]
    shift_id: Optional[int]
    product_id: Optional[int]
    batch_id: Optional[int]
    timezone: Optional[str] = "UTC"
    charts: Optional[Dict[str, str]] = {}
