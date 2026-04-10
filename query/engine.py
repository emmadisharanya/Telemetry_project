from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from storage.reader import iter_parquet_paths_for_customers


@dataclass
class QueryResult:
    rows: List[Dict[str, Any]]


class TelemetryQueryEngine:
    """
    DuckDB-based query engine over Parquet telemetry files.
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        self.base_path = Path(base_path)

    def errors_for_customer_last_hours(
        self, customer_id: str, hours: int = 1
    ) -> QueryResult:
        """
        Return error events for a customer in the last N hours.
        If hours >= 9999, return all errors regardless of time.
        """
        conn = duckdb.connect(database=":memory:")
        paths = [str(p) for p in iter_parquet_paths_for_customers(self.base_path, [customer_id])]
        if not paths:
            return QueryResult(rows=[])

        now = datetime.now(timezone.utc)

        # If hours is 9999 or more, go back far enough to catch any dataset
        if hours >= 9999:
            start = datetime(1990, 1, 1, tzinfo=timezone.utc)
        else:
            start = now - timedelta(hours=hours)

        paths_str = ", ".join([f"'{p}'" for p in paths])
        query = f"""
        SELECT * FROM read_parquet([{paths_str}])
        WHERE level = 'ERROR'
          AND timestamp >= '{start.isoformat()}'
          AND timestamp <= '{now.isoformat()}'
        ORDER BY timestamp DESC
        """

        df = conn.execute(query).df()

        # Drop the partition column DuckDB adds from the folder name
        if "date" in df.columns:
            df = df.drop(columns=["date"])

        # Parse attributes back from JSON string to dict
        if "attributes" in df.columns:
            df["attributes"] = df["attributes"].apply(
                lambda x: json.loads(x) if isinstance(x, str) else x
            )

        return QueryResult(rows=df.to_dict(orient="records"))