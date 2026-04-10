import requests
import json

try:
    r = requests.post('http://localhost:8000/api/ingest', json={
        'events': [{
            'type': 'log',
            'customer_id': 'test-customer',
            'level': 'ERROR',
            'message': 'Test error message'
        }]
    })
    print(f'Status: {r.status_code}')
    print(f'Content-Type: {r.headers.get("content-type", "unknown")}')
    try:
        print(f'Response: {json.dumps(r.json(), indent=2)}')
    except:
        print(f'Response (text): {r.text}')
except Exception as e:
    import traceback
    print(f'Request Error: {e}')
    print(traceback.format_exc())
