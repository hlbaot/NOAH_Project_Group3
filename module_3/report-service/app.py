import json
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from report_service import get_ai_insight, get_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("report-service.api")


class UILogRequest(BaseModel):
    event: str
    source: str = "dashboard-ui"
    timestamp: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)


app = FastAPI(title="NOAH Report Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/report")
def report(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    logger.info("API /api/report requested | page=%s | page_size=%s", page, page_size)
    result = get_report(page=page, page_size=page_size)
    logger.info(
        "API /api/report completed | page=%s | returned_rows=%s | total_rows=%s",
        page,
        len(result.get("orders", [])),
        result.get("pagination", {}).get("total_rows", 0),
    )
    return result


@app.post("/api/ui-log")
def ui_log(payload: UILogRequest):
    logger.info(
        "UI event | source=%s | event=%s | details=%s | client_timestamp=%s",
        payload.source,
        payload.event,
        json.dumps(payload.details, ensure_ascii=False),
        payload.timestamp or "n/a",
    )
    return {"success": True}

@app.get("/api/ai-insight")
def ai_insight():
    report = get_report(page=1, page_size=1)
    insight = get_ai_insight(report["summary"], report["revenue_by_customer"], report["top_products"])
    return {"insight": insight}
