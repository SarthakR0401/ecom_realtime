from flask import Flask, jsonify, render_template_string
from pymongo import MongoClient
from datetime import datetime, timezone
import tzlocal  # pip install tzlocal

app = Flask(__name__)
client = MongoClient("mongodb://localhost:27017")
db = client['ecom_analytics']

INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>E-Commerce Real-Time Analytics</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background-color: #f8fafc; font-family: 'Inter', sans-serif; }
    h1 { color: #1e3a8a; }
    .card { background: white; border-radius: 1rem; box-shadow: 0 4px 12px rgba(0,0,0,0.05); padding: 1rem; }
  </style>
</head>
<body class="p-6">
  <h1 class="text-3xl font-bold mb-4">E-Commerce Real-Time Analytics</h1>

  <div class="grid grid-cols-2 gap-6">
    <div class="card">
      <h2 class="text-xl font-semibold mb-2">Top Products (per minute)</h2>
      <canvas id="topBar" height="200"></canvas>
    </div>

    <div class="card">
      <h2 class="text-xl font-semibold mb-2">Category Distribution</h2>
      <canvas id="catPie" height="200"></canvas>
    </div>

    <div class="card col-span-2">
      <h2 class="text-xl font-semibold mb-2">Views Trend (last 10 windows)</h2>
      <canvas id="trendLine" height="100"></canvas>
    </div>

    <div class="card col-span-2">
      <h2 class="text-xl font-semibold mb-2">Recent Alerts</h2>
      <table class="min-w-full divide-y divide-gray-200">
        <thead>
          <tr class="bg-gray-50">
            <th class="px-4 py-2 text-left text-sm font-medium text-gray-600">Timestamp</th>
            <th class="px-4 py-2 text-left text-sm font-medium text-gray-600">Product ID</th>
            <th class="px-4 py-2 text-left text-sm font-medium text-gray-600">Count</th>
          </tr>
        </thead>
        <tbody id="alertTable" class="divide-y divide-gray-100"></tbody>
      </table>
    </div>
  </div>

  <script>
    const palette = [
      '#6366f1','#22c55e','#f97316','#e11d48','#06b6d4',
      '#84cc16','#a855f7','#facc15','#14b8a6','#ef4444'
    ];

    let barChart, pieChart, lineChart;

    async function fetchData() {
      const [topRes, catRes, trendRes, alertRes] = await Promise.all([
        fetch('/api/top'),
        fetch('/api/category'),
        fetch('/api/trend'),
        fetch('/api/alerts?_=' + Date.now()) // cache-buster
      ]);
      const top = await topRes.json();
      const cat = await catRes.json();
      const trend = await trendRes.json();
      const alerts = await alertRes.json();
      updateTop(top);
      updatePie(cat);
      updateTrend(trend);
      updateAlerts(alerts);
    }

    function updateTop(data) {
      const labels = data.map(x => x.product_id);
      const counts = data.map(x => x.count);
      const colors = labels.map((_,i)=> palette[i % palette.length]);
      if (barChart) barChart.destroy();
      barChart = new Chart(document.getElementById('topBar'), {
        type: 'bar',
        data: { labels, datasets: [{ label: 'Views', data: counts, backgroundColor: colors }] },
        options: { plugins: { legend: { display: false }}, scales: { y: { beginAtZero:true } } }
      });
    }

    function updatePie(data) {
      const labels = data.map(x=>x.category);
      const counts = data.map(x=>x.count);
      const colors = labels.map((_,i)=> palette[i % palette.length]);
      if (pieChart) pieChart.destroy();
      pieChart = new Chart(document.getElementById('catPie'), {
        type: 'doughnut',
        data: { labels, datasets: [{ data: counts, backgroundColor: colors }] },
        options: { plugins: { legend: { position: 'bottom' } } }
      });
    }

    function updateTrend(data) {
      const labels = data.map(x=>x.window_start);
      const counts = data.map(x=>x.total_views);
      if (lineChart) lineChart.destroy();
      lineChart = new Chart(document.getElementById('trendLine'), {
        type: 'line',
        data: {
          labels,
          datasets: [{ label: 'Total Views', data: counts, borderColor: '#2563eb', fill: false, tension: 0.3 }]
        },
        options: { scales: { y: { beginAtZero:true } } }
      });
    }

    function updateAlerts(data) {
      const tbody = document.getElementById('alertTable');
      tbody.innerHTML = '';
      data.forEach(a => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td class="px-4 py-2 text-sm text-gray-700">${a.ts}</td>
          <td class="px-4 py-2 text-sm text-gray-700">${a.product_id}</td>
          <td class="px-4 py-2 text-sm text-gray-700">${a.count}</td>
        `;
        tbody.appendChild(tr);
      });
    }

    fetchData();
    setInterval(fetchData, 5000);
  </script>
</body>
</html>
"""

# -----------------------
# API ROUTES
# -----------------------

@app.route("/api/top")
def api_top():
    doc = db.top_products.find().sort("ts", -1).limit(1)
    doc = list(doc)
    if not doc:
        return jsonify([])
    return jsonify(doc[0]['top'])


@app.route("/api/category")
def api_category():
    pipeline = [
        {"$match": {"window_start": {"$exists": True}}},
        {"$group": {"_id": "$category", "count": {"$sum": "$count"}}},
        {"$project": {"category": "$_id", "count": 1, "_id": 0}}
    ]
    data = list(db.product_views.aggregate(pipeline))
    return jsonify(data)


@app.route("/api/trend")
def api_trend():
    import tzlocal
    local_tz = tzlocal.get_localzone()

    pipeline = [
        {"$group": {"_id": "$window_start", "total_views": {"$sum": "$count"}}},
        {"$sort": {"_id": -1}},
        {"$limit": 10},
        {"$project": {"window_start": "$_id", "total_views": 1, "_id": 0}}
    ]

    data = list(db.product_views.aggregate(pipeline))

    # Convert each UTC timestamp to local system time (readable)
    for item in data:
        try:
            dt = datetime.fromisoformat(item["window_start"].replace("Z", "+00:00"))
            local_dt = dt.astimezone(local_tz)
            item["window_start"] = local_dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            item["window_start"] = item["window_start"]

    return jsonify(list(reversed(data)))  # chronological order



@app.route("/api/alerts")
def api_alerts():
    """Return the latest alerts with timestamps in system local time."""
    local_tz = tzlocal.get_localzone()
    data = list(db.alerts.find().sort("ts", -1).limit(10))
    for d in data:
        d["_id"] = str(d["_id"])
        ts = d.get("ts")
        if isinstance(ts, datetime):
            local_ts = ts.replace(tzinfo=timezone.utc).astimezone(local_tz)
            d["ts"] = local_ts.strftime("%Y-%m-%d %H:%M:%S")
        else:
            d["ts"] = str(ts)
    return jsonify(data)


@app.route("/")
def index():
    return render_template_string(INDEX_HTML)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
