"""Quick test script to ingest sample telemetry events."""
import requests
import json
from datetime import datetime, timezone

url = "http://localhost:8000/api/ingest"

events = [
    {
        "type": "log",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "acme-corp",
        "service": "billing-api",
        "level": "ERROR",
        "message": "Payment failed",
        "attributes": {
            "request_id": "req-123",
            "http_status": 500
        }
    },
    {
        "type": "log",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "acme-corp",
        "service": "auth-service",
        "level": "ERROR",
        "message": "Authentication timeout",
        "attributes": {
            "request_id": "req-456",
            "http_status": 504
        }
    },
    {
        "type": "log",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "acme-corp",
        "service": "payment-service",
        "level": "WARN",
        "message": "Retry limit approaching",
        "attributes": {
            "request_id": "req-789",
            "retry_count": 4
        }
    },
    {
        "type": "log",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "tech-startup",
        "service": "api-gateway",
        "level": "INFO",
        "message": "Request processed successfully",
        "attributes": {
            "request_id": "req-999",
            "http_status": 200
        }
    },
    {
        "type": "log",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "tech-startup",
        "service": "api-gateway",
        "level": "ERROR",
        "message": "Rate limit exceeded",
        "attributes": {
            "request_id": "req-000",
            "http_status": 429
        }
    }
]

response = requests.post(url, json={"events": events})
print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}")
print(f"\nExpected 'ingested': {len(events)}")