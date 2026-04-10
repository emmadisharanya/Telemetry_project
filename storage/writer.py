from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class TelemetryWriter:
    """
    Parquet writer that writes each batch as a new file instead of appending.
    This is much faster for large datasets.

    Layout:
      base_path/
        date=YYYY-MM-DD/
          customer_id=C123/
            telemetry_<uuid>.parquet
    """

    def __init__(self, base_path: str | Path) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _target_dir(self, event: Dict[str, Any]) -> Path:
        ts = event.get("timestamp")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if isinstance(ts, datetime):
            date_str = ts.date().isoformat()
        else:
            date_str = datetime.utcnow().date().isoformat()

        customer_id = event.get("customer_id", "unknown")
        dir_path = (
            self.base_path
            / f"date={date_str}"
            / f"customer_id={customer_id}"
        )
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path

    def append_events(self, events: Iterable[Dict[str, Any]]) -> int:
        rows: List[Dict[str, Any]] = list(events)
        if not rows:
            return 0

        # Normalize events
        normalized: List[Dict[str, Any]] = []
        for e in rows:
            e = dict(e)
            if isinstance(e.get("timestamp"), datetime):
                e["timestamp"] = e["timestamp"].isoformat()
            attrs = e.get("attributes") or {}
            if not isinstance(attrs, dict):
                attrs = {}
            e["attributes"] = json.dumps(attrs)
            normalized.append(e)

        # Group by directory
        by_dir: Dict[Path, List[Dict[str, Any]]] = {}
        for e in normalized:
            dir_path = self._target_dir(e)
            by_dir.setdefault(dir_path, []).append(e)

        # Write each group as a NEW file with unique name — no read/merge needed
        for dir_path, group in by_dir.items():
            df = pd.DataFrame(group)
            table = pa.Table.from_pandas(df, preserve_index=False)
            # Unique filename per batch — avoids read-modify-write entirely
            path = dir_path / f"telemetry_{uuid.uuid4().hex}.parquet"
            pq.write_table(table, path)

        return len(rows)