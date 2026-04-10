from storage.writer import TelemetryWriter
from datetime import datetime, timezone

writer = TelemetryWriter("data")
events = [{
    "type": "log",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "customer_id": "test-customer",
    "level": "ERROR",
    "message": "Test error message"
}]

try:
    count = writer.append_events(events)
    print(f"Success! Ingested {count} events")
except Exception as e:
    import traceback
    print(f"Error: {e}")
    print(traceback.format_exc())
