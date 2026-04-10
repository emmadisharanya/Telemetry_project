

This is a lightweight telemetry ingestion and query demo inspired by Firetiger's architecture:

- **Ingest via HTTP** using FastAPI
- **Store as Parquet on local disk** in a partitioned layout (`date=` / `customer_id=`)
- **Query with DuckDB** directly on Parquet
- **Customer-centric CLI** for queries like: *"show me all errors for customer X in the last hour"*

### Running the demo

1. **Install dependencies** (Python 3.10+ recommended):

```bash
pip install -r requirements.txt
```

2. **Start the ingestion API**:

```bash
python -m uvicorn main:app --reload
```

This exposes:

- `POST /api/ingest` – accept batches of telemetry events

3. **Send some sample telemetry**:

```bash
curl -X POST http://localhost:8000/api/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "type": "log",
        "timestamp": "2026-02-19T20:00:00Z",
        "customer_id": "acme-corp",
        "service": "billing-api",
        "level": "ERROR",
        "message": "Payment failed",
        "attributes": {
          "request_id": "req-123",
          "http_status": 500
        }
      }
    ]
  }'
```

Events are written as Parquet under `data/` using a simple Iceberg-style partitioning scheme:

- `data/date=YYYY-MM-DD/customer_id=CUSTOMER/telemetry.parquet`

4. **Query errors for a customer**:

```bash
python cli.py errors --customer acme-corp --hours 1
```

This spins up DuckDB in-process, scans only relevant Parquet files, and returns JSON rows for all `ERROR` events for that customer in the last hour.

### Design Notes

- **BYOC & open formats**: all telemetry lands as Parquet on local disk; you can point DuckDB, Spark, or any other engine directly at `data/`.
- **Customer-centric querying**: the primary query path is `customer_id` + time range + error level, matching the "customer not aggregate" story.
- **Extensible schema**: `attributes` is a free-form map so you can evolve event shapes without schema migrations.

