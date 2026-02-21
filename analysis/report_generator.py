"""
HTML report generator for federal procurement data.
Produces self-contained HTML files with inline CSS — no external dependencies.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from datetime import datetime, timedelta
from database import ProcurementDatabase
from analytics import ProcurementAnalytics
from sow_review import SOWReviewer


# ---------------------------------------------------------------------------
# CSS & HTML helpers
# ---------------------------------------------------------------------------

def _base_css():
    return """
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           color: #1a1a2e; background: #f0f2f5; padding: 2rem; }
    h1 { color: #16213e; margin-bottom: .5rem; }
    h2 { color: #0f3460; margin: 1.5rem 0 .75rem; border-bottom: 2px solid #e94560; padding-bottom: .25rem; }
    h3 { color: #16213e; margin: 1rem 0 .5rem; }
    .meta { color: #666; font-size: .85rem; margin-bottom: 1.5rem; }
    .stat-grid { display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 1.5rem; }
    .stat-box { background: #16213e; color: #fff; border-radius: 8px; padding: 1rem 1.5rem;
                min-width: 180px; flex: 1; }
    .stat-box .label { font-size: .75rem; text-transform: uppercase; letter-spacing: .05em; opacity: .8; }
    .stat-box .value { font-size: 1.6rem; font-weight: 700; margin-top: .25rem; }
    table { width: 100%; border-collapse: collapse; margin-bottom: 1.5rem; background: #fff;
            border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
    th { background: #0f3460; color: #fff; text-align: left; padding: .6rem .75rem; font-size: .8rem;
         text-transform: uppercase; letter-spacing: .03em; }
    td { padding: .55rem .75rem; border-bottom: 1px solid #eee; font-size: .85rem; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #f8f9ff; }
    .badge { display: inline-block; padding: .15rem .5rem; border-radius: 4px; font-size: .75rem;
             font-weight: 600; }
    .badge-sb { background: #d4edda; color: #155724; }
    .badge-open { background: #cce5ff; color: #004085; }
    .badge-urgent { background: #f8d7da; color: #721c24; }
    .section { background: #fff; border-radius: 8px; padding: 1.25rem 1.5rem; margin-bottom: 1.5rem;
               box-shadow: 0 1px 3px rgba(0,0,0,.1); }
    ul { margin-left: 1.25rem; margin-bottom: .75rem; }
    li { margin-bottom: .3rem; font-size: .85rem; }
    """


def _html_page(title, body):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>{_base_css()}</style>
</head>
<body>
<h1>{title}</h1>
<p class="meta">Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
{body}
</body>
</html>"""


def _html_table(headers, rows):
    if not rows:
        return "<p><em>No data available.</em></p>"
    head = "".join(f"<th>{h}</th>" for h in headers)
    body_rows = ""
    for row in rows:
        cells = "".join(f"<td>{c}</td>" for c in row)
        body_rows += f"<tr>{cells}</tr>\n"
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body_rows}</tbody></table>"


def _stat_boxes(stats):
    """stats: list of (label, value) tuples."""
    boxes = ""
    for label, value in stats:
        boxes += f'<div class="stat-box"><div class="label">{label}</div><div class="value">{value}</div></div>\n'
    return f'<div class="stat-grid">{boxes}</div>'


def _escape(val):
    """Minimal HTML escape."""
    if val is None:
        return ""
    return str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_REPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'reports')


def save_report(html, report_type, suffix=""):
    """Save HTML to reports/ directory with timestamped filename. Returns path."""
    os.makedirs(_REPORTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"{report_type}_{suffix}_{ts}.html" if suffix else f"{report_type}_{ts}.html"
    name = name.replace(" ", "_")
    path = os.path.join(_REPORTS_DIR, name)
    with open(path, "w") as f:
        f.write(html)
    return path


# ---------------------------------------------------------------------------
# Report generators
# ---------------------------------------------------------------------------

def generate_agency_report(db, agency_name):
    """Per-agency HTML report. Returns HTML string."""
    analytics = ProcurementAnalytics(db=db)
    reviewer = SOWReviewer(db)
    dive = analytics.agency_deep_dive(agency_name)

    # Stat boxes
    stats = dive.get("overall_stats", {})
    body = _stat_boxes([
        ("Total Opportunities", stats.get("total_opportunities", 0)),
        ("Small Business", stats.get("small_biz_count", 0)),
        ("Unique NAICS", stats.get("unique_naics", 0)),
        ("Date Range", f"{(stats.get('first_opportunity') or '?')[:10]} — {(stats.get('latest_opportunity') or '?')[:10]}"),
    ])

    # Open opportunities (deadline in future)
    cursor = db.conn.cursor()
    cursor.execute("""
        SELECT solicitation_number, title, naics_code, set_aside, response_deadline
        FROM solicitations
        WHERE department LIKE ? AND response_deadline > date('now')
        ORDER BY response_deadline ASC LIMIT 50
    """, (f"%{agency_name}%",))
    open_opps = cursor.fetchall()
    body += "<h2>Open Opportunities</h2>\n"
    body += _html_table(
        ["Sol #", "Title", "NAICS", "Set-Aside", "Deadline"],
        [[_escape(c) for c in row] for row in open_opps],
    )

    # Top NAICS
    body += "<h2>Top NAICS Codes</h2>\n"
    naics_rows = [(n["naics_code"], n.get("naics_description", ""), n["count"])
                  for n in dive.get("top_naics_codes", [])]
    body += _html_table(["NAICS", "Description", "Count"], naics_rows)

    # Set-aside distribution
    body += "<h2>Set-Aside Distribution</h2>\n"
    sa_rows = [(s["set_aside"], s["count"]) for s in dive.get("set_aside_distribution", [])]
    body += _html_table(["Set-Aside Type", "Count"], sa_rows)

    # SOW highlights — labor categories
    body += "<h2>Common Labor Categories</h2>\n"
    labor = reviewer.common_labor_categories(agency_name)
    if labor:
        body += _html_table(["Category", "Occurrences"], labor)
    else:
        body += "<p><em>No SOW/PWS documents parsed yet for this agency.</em></p>"

    # Opportunities with parsed documents
    body += "<h2>Opportunities with Parsed Documents</h2>\n"
    doc_opps = reviewer.opportunities_with_documents(agency_name)
    if doc_opps:
        body += _html_table(
            ["Notice ID", "Title", "Posted", "Docs", "Roles"],
            [[d["notice_id"], _escape(d["title"]), d["posted_date"], d["doc_count"], d["roles"]]
             for d in doc_opps],
        )
    else:
        body += "<p><em>No parsed documents found.</em></p>"

    return _html_page(f"Agency Report — {agency_name}", body)


def generate_new_opportunities_report(db, days_back=1, agencies=None):
    """Daily/weekly new-opportunities digest. Returns HTML string."""
    cursor = db.conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    if agencies is None:
        agencies = ["FHFA", "SEC", "OCC", "CFPB", "FCA"]

    body = _stat_boxes([("Period", f"Last {days_back} day(s)"), ("Agencies", len(agencies))])

    for agency in agencies:
        cursor.execute("""
            SELECT solicitation_number, title, naics_code, set_aside, posted_date, response_deadline
            FROM solicitations
            WHERE department LIKE ? AND posted_date >= ?
            ORDER BY posted_date DESC
        """, (f"%{agency}%", cutoff))
        rows = cursor.fetchall()
        body += f"<h2>{_escape(agency)} ({len(rows)} new)</h2>\n"
        body += _html_table(
            ["Sol #", "Title", "NAICS", "Set-Aside", "Posted", "Deadline"],
            [[_escape(c) for c in r] for r in rows],
        )

    label = "Daily" if days_back <= 2 else "Weekly"
    return _html_page(f"{label} New Opportunities Digest", body)


def generate_sow_review_report(db, notice_id):
    """Detailed SOW review for a single opportunity. Returns HTML string."""
    reviewer = SOWReviewer(db)
    review = reviewer.review_opportunity(notice_id)

    meta = review.get("metadata", {})
    body = '<div class="section">'
    body += f"<h2>Metadata</h2>"
    body += _html_table(["Field", "Value"], [
        ["Title", _escape(meta.get("title"))],
        ["Department", _escape(meta.get("department"))],
        ["Solicitation #", _escape(meta.get("solicitation_number"))],
        ["NAICS", _escape(meta.get("naics_code"))],
        ["Set-Aside", _escape(meta.get("set_aside"))],
        ["Posted", _escape(meta.get("posted_date"))],
        ["Deadline", _escape(meta.get("response_deadline"))],
    ])
    body += "</div>"

    # Documents
    docs = review.get("documents", [])
    if docs:
        body += "<h2>Documents</h2>\n"
        body += _html_table(
            ["Filename", "Type", "Role", "Parse Status"],
            [[_escape(d.get("filename")), d.get("file_type"), d.get("doc_role"), d.get("parse_status")]
             for d in docs],
        )

    # SOW analysis
    for sow in review.get("sow_analysis", []):
        body += '<div class="section">'
        body += f"<h2>SOW/PWS Analysis (confidence: {sow.get('confidence_score', 0):.0%})</h2>"
        if sow.get("scope_summary"):
            body += f"<h3>Scope</h3><p>{_escape(sow['scope_summary'])}</p>"
        if sow.get("period_of_performance"):
            body += f"<h3>Period of Performance</h3><p>{_escape(sow['period_of_performance'])}</p>"
        for field, label in [("key_tasks", "Key Tasks"), ("labor_categories", "Labor Categories"),
                             ("deliverables", "Deliverables"), ("compliance_reqs", "Compliance Requirements")]:
            items = sow.get(field, [])
            if items:
                body += f"<h3>{label}</h3><ul>"
                for item in items:
                    body += f"<li>{_escape(item)}</li>"
                body += "</ul>"
        body += "</div>"

    # Evaluation criteria
    criteria = review.get("evaluation_criteria", [])
    if criteria:
        body += "<h2>Evaluation Criteria</h2>\n"
        body += _html_table(
            ["#", "Factor", "Weight", "Phase", "Rating Method", "Page Limit"],
            [[c.get("factor_number", ""), _escape(c.get("factor_name")), _escape(c.get("factor_weight")),
              _escape(c.get("evaluation_phase")), _escape(c.get("rating_method")), _escape(c.get("page_limit"))]
             for c in criteria],
        )
        for c in criteria:
            subs = c.get("subfactors", [])
            if subs:
                body += f"<h3>{_escape(c.get('factor_name'))} — Subfactors</h3><ul>"
                for s in subs:
                    body += f"<li>{_escape(s)}</li>"
                body += "</ul>"

    title = _escape(meta.get("title", notice_id))
    return _html_page(f"SOW Review — {title}", body)


def generate_market_summary_report(db):
    """Cross-agency market summary. Returns HTML string."""
    analytics = ProcurementAnalytics(db=db)
    reviewer = SOWReviewer(db)
    summary = analytics.generate_market_summary()

    db_stats = summary.get("database_stats", {})
    date_range = db_stats.get("date_range", {})
    body = _stat_boxes([
        ("Total Opportunities", db_stats.get("total_opportunities", 0)),
        ("Active (Open)", db_stats.get("active_opportunities", 0)),
        ("Last 30 Days", db_stats.get("last_30_days", 0)),
        ("Date Range", f"{(date_range.get('earliest') or '?')[:10]} — {(date_range.get('latest') or '?')[:10]}"),
    ])

    # Top agencies
    body += "<h2>Top Agencies</h2>\n"
    body += _html_table(
        ["Agency", "Total Opps", "Small Biz", "SB %", "NAICS Codes"],
        [[_escape(a["department"]), a["total_opportunities"], a["small_biz_opportunities"],
          f"{a['small_biz_percentage']:.0f}%", a["unique_naics_codes"]]
         for a in summary.get("top_agencies", [])],
    )

    # Top NAICS
    body += "<h2>Top NAICS Codes</h2>\n"
    body += _html_table(
        ["NAICS", "Description", "Opps", "SB %", "Agencies"],
        [[n["naics_code"], _escape(n.get("naics_description", "")), n["total_opportunities"],
          f"{n['small_biz_percentage']:.0f}%", n["number_of_agencies"]]
         for n in summary.get("top_naics", [])],
    )

    # Set-aside distribution
    body += "<h2>Set-Aside Distribution</h2>\n"
    body += _html_table(
        ["Type", "Count", "Percentage", "Agencies Using"],
        [[_escape(s["set_aside_type"]), s["count"], f"{s['percentage']:.1f}%", s["agencies_using"]]
         for s in summary.get("set_aside_distribution", [])],
    )

    # Eval criteria comparison
    body += "<h2>Evaluation Criteria by Agency</h2>\n"
    agencies = ["FHFA", "SEC", "OCC", "CFPB", "FCA"]
    try:
        comparison = reviewer.compare_agencies_eval_criteria(agencies)
        all_factors = sorted({f for factors in comparison.values() for f in factors})
        if all_factors:
            headers = ["Factor"] + list(comparison.keys())
            rows = []
            for factor in all_factors:
                row = [_escape(factor)] + [comparison[a].get(factor, 0) for a in comparison]
                rows.append(row)
            body += _html_table(headers, rows)
        else:
            body += "<p><em>No evaluation criteria data yet.</em></p>"
    except Exception:
        body += "<p><em>Evaluation criteria comparison unavailable.</em></p>"

    return _html_page("Market Summary Report", body)
