from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

from storage.writer import TelemetryWriter
from query.engine import TelemetryQueryEngine


router = APIRouter()


class TelemetryEvent(BaseModel):
    type: Literal["log", "metric", "trace"] = Field(
        ..., description="Kind of telemetry signal"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event timestamp in UTC",
    )
    customer_id: str = Field(..., description="Logical customer identifier")
    service: Optional[str] = Field(None, description="Service or component name")
    level: Optional[Literal["DEBUG", "INFO", "WARN", "ERROR"]] = None
    message: Optional[str] = None
    attributes: Dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary structured attributes"
    )


class IngestRequest(BaseModel):
    events: List[TelemetryEvent]


_writer = TelemetryWriter(base_path="data")
_query_engine = TelemetryQueryEngine(base_path="data")


@router.post("/ingest")
async def ingest_events(payload: IngestRequest) -> Dict[str, Any]:
    try:
        events_data = []
        for e in payload.events:
            # mode="json" automatically converts datetime -> ISO string
            # and ensures all types are plain primitives before hitting the writer.
            # This prevents schema mismatches on Parquet append.
            raw = e.model_dump(mode="json")
            events_data.append(raw)

        count = _writer.append_events(events_data)
        return {"ingested": count}
    except Exception as e:
        import traceback
        error_detail = f"Error ingesting events: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@router.get("/query/errors")
async def query_errors(
    customer_id: str = Query(..., description="Customer ID to query"),
    hours: int = Query(1, description="Number of hours to look back")
) -> Dict[str, Any]:
    result = _query_engine.errors_for_customer_last_hours(customer_id, hours)
    return {
        "customer_id": customer_id,
        "hours": hours,
        "count": len(result.rows),
        "events": result.rows
    }