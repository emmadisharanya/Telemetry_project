from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn

from api.ingest import router as ingest_router


def create_app() -> FastAPI:
    app = FastAPI(title="Firetiger Telemetry Demo", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/", response_class=HTMLResponse)
    async def root():
        return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Firetiger Telemetry</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0a0a;
    --surface: #111111;
    --surface2: #1a1a1a;
    --border: #222222;
    --accent: #ff4d00;
    --accent2: #ff8c00;
    --text: #f0f0f0;
    --muted: #666666;
    --error: #ff4d00;
    --warn: #ffaa00;
    --info: #00aaff;
    --success: #00cc88;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Syne', sans-serif;
    min-height: 100vh;
    overflow-x: hidden;
  }

  /* Grid background */
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background-image:
      linear-gradient(rgba(255,77,0,0.03) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255,77,0,0.03) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events: none;
    z-index: 0;
  }

  .container {
    max-width: 1400px;
    margin: 0 auto;
    padding: 40px 32px;
    position: relative;
    z-index: 1;
  }

  /* Header */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 48px;
    padding-bottom: 24px;
    border-bottom: 1px solid var(--border);
  }

  .logo {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .logo-icon {
    width: 36px;
    height: 36px;
    background: var(--accent);
    clip-path: polygon(50% 0%, 100% 100%, 0% 100%);
    animation: pulse 2s ease-in-out infinite;
  }

  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.7; }
  }

  .logo h1 {
    font-size: 22px;
    font-weight: 800;
    letter-spacing: -0.5px;
  }

  .logo span { color: var(--accent); }

  .status-badge {
    display: flex;
    align-items: center;
    gap: 8px;
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    color: var(--success);
    background: rgba(0,204,136,0.08);
    border: 1px solid rgba(0,204,136,0.2);
    padding: 6px 12px;
    border-radius: 4px;
  }

  .status-dot {
    width: 6px;
    height: 6px;
    background: var(--success);
    border-radius: 50%;
    animation: blink 1.5s ease-in-out infinite;
  }

  @keyframes blink {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
  }

  /* Metrics grid */
  .metrics-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 32px;
  }

  .metric-card {
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 24px;
    border-radius: 2px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
  }

  .metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--accent-color, var(--accent));
  }

  .metric-card:hover { border-color: #333; }

  .metric-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 12px;
    font-family: 'Space Mono', monospace;
  }

  .metric-value {
    font-size: 36px;
    font-weight: 800;
    letter-spacing: -2px;
    color: var(--text);
    line-height: 1;
  }

  .metric-sub {
    font-size: 11px;
    color: var(--muted);
    margin-top: 8px;
    font-family: 'Space Mono', monospace;
  }

  /* Main layout */
  .main-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    margin-bottom: 24px;
  }

  .panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 2px;
    overflow: hidden;
  }

  .panel-header {
    padding: 16px 24px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: space-between;
  }

  .panel-title {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--muted);
    font-family: 'Space Mono', monospace;
  }

  .panel-body { padding: 24px; }

  /* Customer error bars */
  .customer-row {
    margin-bottom: 20px;
  }

  .customer-meta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
  }

  .customer-name {
    font-size: 13px;
    font-weight: 600;
  }

  .customer-stats {
    display: flex;
    gap: 12px;
    font-family: 'Space Mono', monospace;
    font-size: 11px;
  }

  .stat-errors { color: var(--error); }
  .stat-warns { color: var(--warn); }
  .stat-info { color: var(--info); }

  .bar-track {
    height: 4px;
    background: var(--border);
    border-radius: 2px;
    overflow: hidden;
  }

  .bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 1s cubic-bezier(0.4, 0, 0.2, 1);
  }

  /* Error reasons */
  .reason-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 12px 0;
    border-bottom: 1px solid var(--border);
  }

  .reason-row:last-child { border-bottom: none; }

  .reason-code {
    font-family: 'Space Mono', monospace;
    font-size: 13px;
    font-weight: 700;
    color: var(--error);
    min-width: 40px;
  }

  .reason-desc {
    flex: 1;
    font-size: 13px;
    color: var(--text);
  }

  .reason-count {
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    color: var(--muted);
  }

  .reason-bar {
    width: 60px;
    height: 3px;
    background: var(--border);
    border-radius: 2px;
    overflow: hidden;
  }

  .reason-bar-fill {
    height: 100%;
    background: var(--error);
    border-radius: 2px;
    transition: width 1s ease;
  }

  /* Ingest + Query panels */
  .full-width { grid-column: 1 / -1; }

  .two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    margin-bottom: 24px;
  }

  textarea {
    width: 100%;
    height: 140px;
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    padding: 12px;
    border-radius: 2px;
    resize: vertical;
    outline: none;
    transition: border-color 0.2s;
    margin-bottom: 12px;
  }

  textarea:focus { border-color: var(--accent); }

  input {
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: 'Space Mono', monospace;
    font-size: 12px;
    padding: 8px 12px;
    border-radius: 2px;
    outline: none;
    transition: border-color 0.2s;
    margin-right: 8px;
    margin-bottom: 12px;
  }

  input:focus { border-color: var(--accent); }

  .btn {
    background: var(--accent);
    color: white;
    border: none;
    padding: 10px 20px;
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
    cursor: pointer;
    border-radius: 2px;
    transition: all 0.2s;
  }

  .btn:hover {
    background: var(--accent2);
    transform: translateY(-1px);
  }

  .btn:active { transform: translateY(0); }

  .btn-outline {
    background: transparent;
    border: 1px solid var(--accent);
    color: var(--accent);
  }

  .btn-outline:hover {
    background: var(--accent);
    color: white;
  }

  pre {
    background: var(--surface2);
    border: 1px solid var(--border);
    padding: 16px;
    border-radius: 2px;
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    overflow-x: auto;
    margin-top: 12px;
    max-height: 200px;
    overflow-y: auto;
    color: var(--success);
    line-height: 1.6;
  }

  /* Health scores */
  .health-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 12px;
  }

  .health-card {
    background: var(--surface2);
    border: 1px solid var(--border);
    padding: 16px;
    border-radius: 2px;
  }

  .health-name {
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 8px;
  }

  .health-bar-track {
    height: 6px;
    background: var(--border);
    border-radius: 3px;
    overflow: hidden;
    margin-bottom: 6px;
  }

  .health-bar-fill {
    height: 100%;
    border-radius: 3px;
    transition: width 1.2s cubic-bezier(0.4, 0, 0.2, 1);
  }

  .health-score {
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    color: var(--muted);
  }

  .loading {
    color: var(--muted);
    font-family: 'Space Mono', monospace;
    font-size: 12px;
    text-align: center;
    padding: 32px;
  }

  .tag {
    display: inline-block;
    font-family: 'Space Mono', monospace;
    font-size: 9px;
    padding: 2px 6px;
    border-radius: 2px;
    font-weight: 700;
    letter-spacing: 1px;
  }

  .tag-error { background: rgba(255,77,0,0.15); color: var(--error); }
  .tag-warn  { background: rgba(255,170,0,0.15); color: var(--warn); }
  .tag-info  { background: rgba(0,170,255,0.15); color: var(--info); }

  @media (max-width: 900px) {
    .metrics-grid { grid-template-columns: repeat(2, 1fr); }
    .main-grid, .two-col { grid-template-columns: 1fr; }
    .health-grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <header>
    <div class="logo">
      <div class="logo-icon"></div>
      <h1>Fire<span>tiger</span> Telemetry</h1>
    </div>
    <div class="status-badge">
      <div class="status-dot"></div>
      SYSTEM OPERATIONAL
    </div>
  </header>

  <!-- Top metrics -->
  <div class="metrics-grid">
    <div class="metric-card" style="--accent-color: var(--info)">
      <div class="metric-label">Total Events</div>
      <div class="metric-value" id="totalEvents">—</div>
      <div class="metric-sub">across all customers</div>
    </div>
    <div class="metric-card" style="--accent-color: var(--error)">
      <div class="metric-label">Total Errors</div>
      <div class="metric-value" id="totalErrors" style="color:var(--error)">—</div>
      <div class="metric-sub">HTTP 5xx responses</div>
    </div>
    <div class="metric-card" style="--accent-color: var(--warn)">
      <div class="metric-label">Total Warnings</div>
      <div class="metric-value" id="totalWarns" style="color:var(--warn)">—</div>
      <div class="metric-sub">HTTP 4xx responses</div>
    </div>
    <div class="metric-card" style="--accent-color: var(--success)">
      <div class="metric-label">Customers</div>
      <div class="metric-value" id="totalCustomers" style="color:var(--success)">—</div>
      <div class="metric-sub">active tenants</div>
    </div>
  </div>

  <!-- Charts row -->
  <div class="main-grid">

    <!-- Errors per customer -->
    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Errors per Customer</span>
        <span class="tag tag-error">LIVE</span>
      </div>
      <div class="panel-body" id="customerBars">
        <div class="loading">Loading customer data...</div>
      </div>
    </div>

    <!-- Most common error reasons -->
    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Most Common Error Reasons</span>
        <span class="tag tag-error">TOP 5</span>
      </div>
      <div class="panel-body" id="errorReasons">
        <div class="loading">Loading error analysis...</div>
      </div>
    </div>

  </div>

  <!-- Health scores + Resolution time -->
  <div class="two-col">

    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Customer Health Score</span>
        <span class="tag tag-info">COMPUTED</span>
      </div>
      <div class="panel-body">
        <div class="health-grid" id="healthGrid">
          <div class="loading">Loading health data...</div>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Est. Time to Resolve</span>
        <span class="tag tag-warn">PER CUSTOMER</span>
      </div>
      <div class="panel-body" id="resolvePanel">
        <div class="loading">Computing resolution times...</div>
      </div>
    </div>

  </div>

  <!-- Ingest + Query -->
  <div class="two-col">

    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Ingest Events</span>
      </div>
      <div class="panel-body">
        <textarea id="ingestData">{
  "events": [
    {
      "type": "log",
      "customer_id": "acme-corp",
      "service": "billing-api",
      "level": "ERROR",
      "message": "Payment failed",
      "attributes": { "http_status": 500 }
    },
    {
      "type": "log",
      "customer_id": "acme-corp",
      "service": "auth-service",
      "level": "ERROR",
      "message": "Auth timeout",
      "attributes": { "http_status": 504 }
    },
    {
      "type": "log",
      "customer_id": "tech-startup",
      "service": "api-gateway",
      "level": "INFO",
      "message": "Request OK",
      "attributes": { "http_status": 200 }
    }
  ]
}</textarea>
        <button class="btn" onclick="ingestEvents()">Ingest Events</button>
        <div id="ingestResult"></div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Query Errors</span>
      </div>
      <div class="panel-body">
        <input type="text" id="customerId" placeholder="customer_id" value="retail-co" style="width:160px">
        <input type="number" id="hours" placeholder="hours" value="9999" style="width:80px">
        <button class="btn" onclick="queryErrors()">Query</button>
        <button class="btn btn-outline" onclick="loadMetrics()" style="margin-left:8px">Refresh Metrics</button>
        <div id="queryResult"></div>
      </div>
    </div>

  </div>

</div>

<script>
// ── Helpers ──────────────────────────────────────────────────────────────────

function fmt(n) {
  if (n >= 1000000) return (n/1000000).toFixed(1) + 'M';
  if (n >= 1000) return (n/1000).toFixed(1) + 'K';
  return n;
}

function healthColor(score) {
  if (score >= 90) return '#00cc88';
  if (score >= 70) return '#ffaa00';
  return '#ff4d00';
}

// ── Load metrics ─────────────────────────────────────────────────────────────

async function loadMetrics() {
  try {
    const res  = await fetch('/api/metrics/summary');
    const data = await res.json();

    // Top stats
    document.getElementById('totalEvents').textContent    = fmt(data.total_events);
    document.getElementById('totalErrors').textContent    = fmt(data.total_errors);
    document.getElementById('totalWarns').textContent     = fmt(data.total_warns);
    document.getElementById('totalCustomers').textContent = data.customers.length;

    // Customer error bars
    const maxErrors = Math.max(...data.customers.map(c => c.errors), 1);
    document.getElementById('customerBars').innerHTML = data.customers.map(c => {
      const pct = (c.errors / maxErrors * 100).toFixed(1);
      const health = (100 - (c.errors / Math.max(c.total_events,1) * 100 * 20)).toFixed(0);
      return `
        <div class="customer-row">
          <div class="customer-meta">
            <span class="customer-name">${c.customer_id}</span>
            <div class="customer-stats">
              <span class="stat-errors">${c.errors} ERR</span>
              <span class="stat-warns">${c.warns} WRN</span>
              <span class="stat-info">${fmt(c.info)} OK</span>
            </div>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width:${pct}%; background: ${c.errors > 0 ? 'var(--error)' : 'var(--success)'}"></div>
          </div>
        </div>`;
    }).join('');

    // Error reasons
    const maxCount = Math.max(...data.error_reasons.map(r => r.count), 1);
    document.getElementById('errorReasons').innerHTML = data.error_reasons.length === 0
      ? '<div class="loading">No errors found</div>'
      : data.error_reasons.map(r => `
        <div class="reason-row">
          <span class="reason-code">${r.http_status}</span>
          <span class="reason-desc">${r.message}</span>
          <div class="reason-bar">
            <div class="reason-bar-fill" style="width:${(r.count/maxCount*100).toFixed(0)}%"></div>
          </div>
          <span class="reason-count">${r.count}x</span>
        </div>`).join('');

    // Health scores
    document.getElementById('healthGrid').innerHTML = data.customers.map(c => {
      const errRate = c.errors / Math.max(c.total_events, 1);
      const score = Math.max(0, Math.min(100, Math.round((1 - errRate * 50) * 100)));
      const color = healthColor(score);
      return `
        <div class="health-card">
          <div class="health-name">${c.customer_id}</div>
          <div class="health-bar-track">
            <div class="health-bar-fill" style="width:${score}%; background:${color}"></div>
          </div>
          <div class="health-score" style="color:${color}">${score}% healthy</div>
        </div>`;
    }).join('');

    // Resolution time estimate (based on error count — simple heuristic)
    document.getElementById('resolvePanel').innerHTML = data.customers.map(c => {
      const mins = c.errors === 0 ? 0 : Math.min(240, c.errors * 8);
      const color = mins === 0 ? '#00cc88' : mins > 60 ? '#ff4d00' : '#ffaa00';
      return `
        <div class="reason-row">
          <span class="customer-name" style="min-width:120px;font-size:13px">${c.customer_id}</span>
          <div class="reason-bar" style="width:80px">
            <div class="reason-bar-fill" style="width:${Math.min(100,(mins/240*100)).toFixed(0)}%; background:${color}"></div>
          </div>
          <span class="reason-count" style="color:${color}">${mins === 0 ? 'No issues' : mins + ' min'}</span>
        </div>`;
    }).join('');

  } catch(e) {
    console.error('Metrics load failed:', e);
  }
}

// ── Ingest ───────────────────────────────────────────────────────────────────

async function ingestEvents() {
  const resultDiv = document.getElementById('ingestResult');
  try {
    const json = JSON.parse(document.getElementById('ingestData').value);
    const res  = await fetch('/api/ingest', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(json)
    });
    const data = await res.json();
    resultDiv.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
    loadMetrics();
  } catch(e) {
    resultDiv.innerHTML = `<pre style="color:var(--error)">Error: ${e.message}</pre>`;
  }
}

// ── Query ────────────────────────────────────────────────────────────────────

async function queryErrors() {
  const customerId = document.getElementById('customerId').value;
  const hours      = document.getElementById('hours').value;
  const resultDiv  = document.getElementById('queryResult');
  try {
    const res  = await fetch(`/api/query/errors?customer_id=${customerId}&hours=${hours}`);
    const data = await res.json();
    resultDiv.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
  } catch(e) {
    resultDiv.innerHTML = `<pre style="color:var(--error)">Error: ${e.message}</pre>`;
  }
}

// ── Init ─────────────────────────────────────────────────────────────────────
loadMetrics();
setInterval(loadMetrics, 30000); // refresh every 30 seconds
</script>
</body>
</html>"""

    @app.get("/health")
    async def health():
        return {"status": "healthy", "service": "firetiger-telemetry-demo"}

    @app.get("/api/metrics/summary")
    async def metrics_summary():
        import duckdb
        from pathlib import Path

        base = Path("data")
        paths = [str(p) for p in base.rglob("*.parquet")]

        if not paths:
            return {
                "total_events": 0,
                "total_errors": 0,
                "total_warns": 0,
                "customers": [],
                "error_reasons": []
            }

        paths_str = ", ".join([f"'{p}'" for p in paths])
        conn = duckdb.connect(":memory:")

        # Customer breakdown
        customers_df = conn.execute(f"""
            SELECT
                customer_id,
                COUNT(*) as total_events,
                SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) as errors,
                SUM(CASE WHEN level = 'WARN'  THEN 1 ELSE 0 END) as warns,
                SUM(CASE WHEN level = 'INFO'  THEN 1 ELSE 0 END) as info
            FROM read_parquet([{paths_str}])
            GROUP BY customer_id
            ORDER BY errors DESC
        """).df()

        # Top error reasons
        import json as _json
        reasons_df = conn.execute(f"""
            SELECT
                message,
                COUNT(*) as count
            FROM read_parquet([{paths_str}])
            WHERE level = 'ERROR'
            GROUP BY message
            ORDER BY count DESC
            LIMIT 5
        """).df()

        # Parse http_status from attributes for display
        error_reasons = []
        for _, row in reasons_df.iterrows():
            # extract status code from message like "GET /path -> 501"
            msg = row['message']
            status = msg.split('->')[-1].strip() if '->' in msg else '5xx'
            error_reasons.append({
                "message": msg[:50] + ('...' if len(msg) > 50 else ''),
                "http_status": status,
                "count": int(row['count'])
            })

        total_events = int(customers_df['total_events'].sum())
        total_errors = int(customers_df['errors'].sum())
        total_warns  = int(customers_df['warns'].sum())

        return {
            "total_events": total_events,
            "total_errors": total_errors,
            "total_warns":  total_warns,
            "customers": customers_df.to_dict(orient="records"),
            "error_reasons": error_reasons
        }

    app.include_router(ingest_router, prefix="/api")
    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)