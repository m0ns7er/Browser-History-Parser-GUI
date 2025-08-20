import os
import io
import csv
import sqlite3
import secrets
from datetime import datetime, timedelta, timezone
from flask import (
    Flask, request, redirect, url_for, render_template_string,
    Response, abort, flash
)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
FILE_INDEX = {}

# Time Epochs
EPOCH_CHROMIUM = datetime(1601, 1, 1, tzinfo=timezone.utc)
EPOCH_UNIX = datetime(1970, 1, 1, tzinfo=timezone.utc)
EPOCH_COCOA = datetime(2001, 1, 1, tzinfo=timezone.utc)

def chrome_to_utc(us_since_1601):
    try: return (EPOCH_CHROMIUM + timedelta(microseconds=int(us_since_1601))).isoformat()
    except: return ""

def firefox_to_utc(us_since_unix):
    try: return (EPOCH_UNIX + timedelta(microseconds=int(us_since_unix))).isoformat()
    except: return ""

def safari_to_utc(val):
    try:
        val = float(val)
        if val>1e14: seconds=val/1e9
        elif val>1e11: seconds=val/1e6
        elif val>1e10: seconds=val/1e3
        else: seconds=val
        return (EPOCH_COCOA + timedelta(seconds=seconds)).isoformat()
    except: return ""

def detect_kind(db_path):
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as con:
        con.row_factory=sqlite3.Row
        cur=con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names={row[0].lower() for row in cur.fetchall()}
    if {"urls","visits"}.issubset(names): return "chromium"
    if {"moz_places","moz_historyvisits"}.issubset(names): return "firefox"
    if {"history_items","history_visits"}.issubset(names): return "safari"
    if "downloads" in names: return "chromium"
    return "unknown"

def query_rows(db_path, kind, table_type, limit=5000):
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as con:
        con.row_factory = sqlite3.Row
        cur=con.cursor()

        if table_type=="urls":
            if kind=="chromium":
                sql=f"""
                    SELECT v.id AS visit_id,
                           u.url AS url,
                           u.title AS title,
                           v.visit_time AS raw_time
                    FROM visits v
                    JOIN urls u ON v.url=u.id
                    ORDER BY v.visit_time DESC LIMIT {int(limit)}
                """
            elif kind=="firefox":
                sql=f"""
                    SELECT hv.id AS visit_id,
                           p.url AS url,
                           p.title AS title,
                           hv.visit_date AS raw_time
                    FROM moz_historyvisits hv
                    JOIN moz_places p ON hv.place_id=p.id
                    ORDER BY hv.visit_date DESC LIMIT {int(limit)}
                """
            elif kind=="safari":
                sql=f"""
                    SELECT hv.id AS visit_id,
                           hi.url AS url,
                           hi.title AS title,
                           hv.visit_time AS raw_time
                    FROM history_visits hv
                    JOIN history_items hi ON hv.history_item=hi.id
                    ORDER BY hv.visit_time DESC LIMIT {int(limit)}
                """
            else:
                sql=f"SELECT id AS visit_id,url,title,last_visit_time AS raw_time FROM urls ORDER BY last_visit_time DESC LIMIT {int(limit)}"
            cur.execute(sql)
            for r in cur.fetchall():
                if kind in ["chromium","unknown"]: visited_utc=chrome_to_utc(r["raw_time"])
                elif kind=="firefox": visited_utc=firefox_to_utc(r["raw_time"])
                elif kind=="safari": visited_utc=safari_to_utc(r["raw_time"])
                yield {"id": r["visit_id"], "url": r["url"], "title": r["title"] or "", "utc": visited_utc}

        elif table_type=="downloads":
            if kind=="chromium":
                sql=f"""
                    SELECT id, guid, current_path, target_path,
                    datetime((start_time / 1000000) - 11644473600, 'unixepoch') AS start_time_utc,
                    received_bytes, total_bytes, hash,
                    datetime((end_time / 1000000) - 11644473600, 'unixepoch') AS end_time_utc,
                    datetime((last_access_time / 1000000) - 11644473600, 'unixepoch') AS last_access_time_utc,
                    referrer, tab_url, tab_referrer_url, mime_type, original_mime_type
                    FROM downloads
                    ORDER BY start_time DESC LIMIT {int(limit)}
                """
                cur.execute(sql)
                for r in cur.fetchall():
                    yield {
                        "id": r["id"], "guid": r["guid"], "current_path": r["current_path"], "target_path": r["target_path"],
                        "start_time_utc": r["start_time_utc"], "received_bytes": r["received_bytes"], "total_bytes": r["total_bytes"],
                        "hash": r["hash"], "end_time_utc": r["end_time_utc"], "last_access_time_utc": r["last_access_time_utc"],
                        "referrer": r["referrer"], "tab_url": r["tab_url"], "tab_referrer_url": r["tab_referrer_url"],
                        "mime_type": r["mime_type"], "original_mime_type": r["original_mime_type"]
                    }

INDEX_HTML="""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Browser History Parser</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
<div class="container mt-4">

<div class="d-flex justify-content-between align-items-center mb-3">
    <h2>Browser History Parser</h2>
    <button id="darkModeBtn" class="btn btn-outline-secondary" title="Toggle Dark Mode">
        <i class="fas fa-moon"></i>
    </button>
</div>

<p class="text-muted">Upload a browser history SQLite file (Chrome/Firefox/Safari). Times converted to UTC.</p>
<form action="{{ url_for('upload') }}" method="post" enctype="multipart/form-data" class="row g-3">
<div class="col-md-4">
<label for="file" class="form-label">Choose SQLite file</label>
<input type="file" class="form-control" id="file" name="file" required>
</div>
<div class="col-md-3">
<label for="table" class="form-label">Select table to parse</label>
<select class="form-select" name="table" id="table" required>
<option value="urls">Visited URLs</option>
<option value="downloads">Downloads</option>
</select>
</div>
<div class="col-md-2">
<label for="limit" class="form-label">Max rows</label>
<input type="number" class="form-control" id="limit" name="limit" value="5000" min="1" max="100000">
</div>
<div class="col-md-3 align-self-end">
<button type="submit" class="btn btn-primary w-100">Parse</button>
</div>
</form>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.2/js/all.min.js"></script>
<script>
const darkBtn = document.getElementById("darkModeBtn");
darkBtn.addEventListener("click", () => {
    document.body.classList.toggle("bg-dark");
    document.body.classList.toggle("text-light");
});
</script>
</body>
</html>
"""

RESULTS_HTML="""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">

<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.2/css/all.min.css">

<style>
td.url, td.file_name, td.current_path, td.target_path{
    max-width: 250px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
}
tbody tr:hover{background-color:#e0f7fa;}
body.bg-dark{background-color:#121212!important;color:#e0e0e0!important;}
.table-hover tbody tr:hover{background-color:#1e1e1e!important;}
</style>
</head>
<body>
<div class="container mt-4">
<div class="d-flex justify-content-between align-items-center mb-3">
    <h2>Browser History Results</h2>
    <div>
       <button id="darkModeBtn" class="btn btn-outline-secondary me-2" title="Toggle Dark Mode">
            <i class="fas fa-moon"></i>
        </button>
        <a href="{{ url_for('index') }}" class="btn btn-secondary me-2">Upload Another File</a>
        <a href="{{ url_for('export_csv', token=token, table_type=table_type) }}" class="btn btn-primary">Download CSV</a>
    </div>
</div>
<p class="text-muted">
Detected: <strong>{{ kind|capitalize }}</strong> • Table: <strong>{{ table_type|capitalize }}</strong> • Rows: <strong>{{ rows|length }}</strong> • Times in <strong>UTC</strong>.
</p>

<div class="table-responsive" style="overflow-x:auto; min-width:1200px;">
<table id="resultsTable" class="table table-striped table-hover table-bordered display nowrap">
<thead>
{% if table_type=="urls" %}
<tr><th>Visit ID</th><th>URL</th><th>Title</th><th>Visited (UTC)</th></tr>
{% else %}
<tr>
<th>ID</th><th>GUID</th><th>Current Path</th><th>Target Path</th><th>Start Time UTC</th>
<th>Received Bytes</th><th>Total Bytes</th><th>Hash</th><th>End Time UTC</th><th>Last Access UTC</th>
<th>Referrer</th><th>Tab URL</th><th>Tab Referrer URL</th><th>MIME Type</th><th>Original MIME Type</th>
</tr>
{% endif %}
</thead>
<tbody>
{% for r in rows %}
<tr>
{% if table_type=="urls" %}
<td>{{ r.id }}</td>
<td class="url" title="{{ r.url }}"><a href="{{ r.url }}" target="_blank">{{ r.url }}</a></td>
<td>{{ r.title }}</td>
<td>{{ r.utc }}</td>
{% else %}
<td>{{ r.id }}</td><td>{{ r.guid }}</td><td class="current_path" title="{{ r.current_path }}">{{ r.current_path }}</td>
<td class="target_path" title="{{ r.target_path }}">{{ r.target_path }}</td>
<td>{{ r.start_time_utc }}</td><td>{{ r.received_bytes }}</td><td>{{ r.total_bytes }}</td>
<td>{{ r.hash }}</td><td>{{ r.end_time_utc }}</td><td>{{ r.last_access_time_utc }}</td>
<td class="url" title="{{ r.referrer }}"><a href="{{ r.referrer }}" target="_blank">{{ r.referrer }}</a></td>
<td class="url" title="{{ r.tab_url }}"><a href="{{ r.tab_url }}" target="_blank">{{ r.tab_url }}</a></td>
<td class="url" title="{{ r.tab_referrer_url }}"><a href="{{ r.tab_referrer_url }}" target="_blank">{{ r.tab_referrer_url }}</a></td>
<td>{{ r.mime_type }}</td><td>{{ r.original_mime_type }}</td>
{% endif %}
</tr>
{% endfor %}
</tbody>
</table>
</div>
</div>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>

<script>
$(document).ready(function() {
    $('#resultsTable').DataTable({
        paging: true,
        pageLength: 10,
        lengthMenu: [10,50,100,500],
        searching: false,
        ordering: true,
        order: [[3,'desc']],
        scrollX: true,
        autoWidth: false,
        dom: '<"d-flex justify-content-between mb-2"l>t<"d-flex justify-content-between mt-2"ip>',
        language: {
            lengthMenu:"Show _MENU_ entries",
            paginate: {first: "<<", previous: "<", next: ">", last: ">>"}
        }
    });

    // Dark mode toggle
    const darkBtn = document.getElementById("darkModeBtn");
    darkBtn.addEventListener("click", () => {
        document.body.classList.toggle("bg-dark");
        document.body.classList.toggle("text-light");
        const table = document.getElementById("resultsTable");
        table.classList.toggle("table-dark");
    });
});
</script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def index():
    return render_template_string(INDEX_HTML)

@app.route("/upload", methods=["POST"])
def upload():
    f = request.files.get("file")
    table_type = request.form.get("table","urls")
    limit = request.form.get("limit","5000")
    try: limit=max(1,min(100000,int(limit)))
    except: limit=5000
    if not f or f.filename.strip()=="": flash("Please choose a file."); return redirect(url_for("index"))
    filename=secure_filename(f.filename) or "history.sqlite"
    token=secrets.token_urlsafe(12)
    path=os.path.join(UPLOAD_DIR,f"{token}_{filename}")
    f.save(path)
    kind=detect_kind(path)
    rows=[type("Row",(),r) for r in query_rows(path, kind, table_type, limit)]
    FILE_INDEX[token]={"path":path,"kind":kind,"table_type":table_type}
    return render_template_string(RESULTS_HTML, rows=rows, token=token, kind=kind, table_type=table_type)

@app.route("/export/<token>.csv", methods=["GET"])
def export_csv(token):
    meta=FILE_INDEX.get(token)
    if not meta: abort(404)
    path=meta["path"]; kind=meta["kind"]; table_type=meta.get("table_type","urls")
    def generate():
        output=io.StringIO(); writer=csv.writer(output)
        if table_type=="urls": writer.writerow(["id","url","title","utc"])
        else:
            writer.writerow([
                "id","guid","current_path","target_path","start_time_utc","received_bytes","total_bytes","hash",
                "end_time_utc","last_access_time_utc","referrer","tab_url","tab_referrer_url","mime_type","original_mime_type"
            ])
        yield output.getvalue(); output.seek(0); output.truncate(0)
        for r in query_rows(path, kind, table_type, limit=1000000):
            if table_type=="urls": writer.writerow([r["id"],r["url"],r["title"],r["utc"]])
            else:
                writer.writerow([
                    r["id"], r["guid"], r["current_path"], r["target_path"], r["start_time_utc"], r["received_bytes"],
                    r["total_bytes"], r["hash"], r["end_time_utc"], r["last_access_time_utc"], r["referrer"],
                    r["tab_url"], r["tab_referrer_url"], r["mime_type"], r["original_mime_type"]
                ])
            yield output.getvalue(); output.seek(0); output.truncate(0)
    return Response(generate(), mimetype="text/csv",
                    headers={"Content-Disposition":f'attachment; filename="history_{table_type}_{token}.csv"'})

if __name__=="__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
