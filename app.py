#!/usr/bin/env python3
"""
Flask web dashboard for federal procurement data.

Usage:
    python3 app.py              # Run on port 5000
    python3 app.py --debug      # Run with auto-reload
    python3 app.py --port 8080  # Custom port
"""

import argparse
import os
import sys
from datetime import datetime
from flask import Flask, render_template, request, send_from_directory

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analysis'))
from database import ProcurementDatabase
from analytics import ProcurementAnalytics
from sow_review import SOWReviewer

app = Flask(__name__)


def _get_db():
    """Create a fresh database connection (SQLite thread safety)."""
    return ProcurementDatabase()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    db = _get_db()
    try:
        analytics = ProcurementAnalytics(db=db)
        summary = analytics.generate_market_summary()
        db_stats = summary.get("database_stats", {})

        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT notice_id, title, department, naics_code, set_aside,
                   posted_date, response_deadline, is_small_business_setaside
            FROM solicitations ORDER BY posted_date DESC LIMIT 25
        """)
        cols = [d[0] for d in cursor.description]
        recent = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # Forecast summary
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT agency) as agencies
            FROM forecast_opportunities
        """)
        fc_row = cursor.fetchone()
        forecast_stats = {
            "total": fc_row[0] or 0,
            "agencies": fc_row[1] or 0,
        } if fc_row else {"total": 0, "agencies": 0}

        cursor.execute("""
            SELECT agency, COUNT(*) as cnt FROM forecast_opportunities
            GROUP BY agency ORDER BY cnt DESC
        """)
        forecast_by_agency = cursor.fetchall()

        cursor.execute("""
            SELECT agency, office_code, project_description, estimated_amount_category,
                   acquisition_strategy, estimated_quarter
            FROM forecast_opportunities
            ORDER BY agency, estimated_quarter, office_code
            LIMIT 15
        """)
        cols_fc = [d[0] for d in cursor.description]
        forecast_upcoming = [dict(zip(cols_fc, row)) for row in cursor.fetchall()]

        return render_template("dashboard.html",
                               stats=db_stats,
                               top_agencies=summary.get("top_agencies", []),
                               recent=recent,
                               forecast_stats=forecast_stats,
                               forecast_by_agency=forecast_by_agency,
                               forecast_upcoming=forecast_upcoming)
    finally:
        db.close()


@app.route("/agency/<name>")
def agency(name):
    db = _get_db()
    try:
        analytics = ProcurementAnalytics(db=db)
        reviewer = SOWReviewer(db)
        dive = analytics.agency_deep_dive(name)

        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT notice_id, solicitation_number, title, naics_code, set_aside,
                   posted_date, response_deadline
            FROM solicitations
            WHERE department LIKE ?
            ORDER BY posted_date DESC
        """, (f"%{name}%",))
        cols = [d[0] for d in cursor.description]
        opportunities = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # Forecast data for this agency
        cursor.execute("""
            SELECT office_code, office_name, project_description, estimated_amount_category,
                   acquisition_strategy, estimated_quarter, estimated_award_date
            FROM forecast_opportunities
            WHERE agency LIKE ?
            ORDER BY estimated_quarter, office_code
        """, (f"%{name}%",))
        fc_cols = [d[0] for d in cursor.description]
        forecast = [dict(zip(fc_cols, row)) for row in cursor.fetchall()]

        structured_rates = reviewer.common_labor_categories_structured(name)

        return render_template("agency.html",
                               agency_name=name,
                               stats=dive.get("overall_stats", {}),
                               top_naics=dive.get("top_naics_codes", []),
                               set_aside=dive.get("set_aside_distribution", []),
                               labor_cats=reviewer.common_labor_categories(name),
                               structured_rates=structured_rates,
                               doc_opps=reviewer.opportunities_with_documents(name),
                               opportunities=opportunities,
                               forecast=forecast)
    finally:
        db.close()


@app.route("/opportunity/<path:notice_id>")
def opportunity(notice_id):
    db = _get_db()
    try:
        reviewer = SOWReviewer(db)
        review = reviewer.review_opportunity(notice_id)

        # Group structured labor by category for compact display
        raw_labor = review.get("structured_labor", [])
        grouped_labor = {}
        has_clin = any(r.get("clin_number") for r in raw_labor)
        has_site = any(r.get("site_type") for r in raw_labor)
        for lc in raw_labor:
            key = (lc["category_name"], lc.get("clin_number", ""), lc.get("site_type", ""))
            if key not in grouped_labor:
                grouped_labor[key] = {
                    "category_name": lc["category_name"],
                    "category_title": lc.get("category_title", ""),
                    "clin_number": lc.get("clin_number", ""),
                    "site_type": lc.get("site_type", ""),
                    "min_rate": None,
                    "max_rate": None,
                    "total_hours": 0,
                    "periods": [],
                }
            g = grouped_labor[key]
            rate = lc.get("hourly_rate")
            if rate is not None:
                g["min_rate"] = min(g["min_rate"], rate) if g["min_rate"] is not None else rate
                g["max_rate"] = max(g["max_rate"], rate) if g["max_rate"] is not None else rate
            hours = lc.get("estimated_hours")
            if hours:
                g["total_hours"] += hours
            g["periods"].append({
                "period_name": lc.get("period_name", ""),
                "hourly_rate": rate,
                "estimated_hours": hours,
                "extended_price": lc.get("extended_price"),
            })

        labor_groups = sorted(grouped_labor.values(),
                              key=lambda g: (g["clin_number"] or "", g["category_name"]))

        return render_template("opportunity.html",
                               notice_id=notice_id,
                               meta=review.get("metadata", {}),
                               documents=review.get("documents", []),
                               sow_analysis=review.get("sow_analysis", []),
                               eval_criteria=review.get("evaluation_criteria", []),
                               labor_groups=labor_groups,
                               has_clin=has_clin,
                               has_site=has_site,
                               labor_total=len(raw_labor))
    finally:
        db.close()


@app.route("/forecast")
def forecast():
    db = _get_db()
    try:
        cursor = db.conn.cursor()
        agency_filter = request.args.get("agency", "")
        quarter_filter = request.args.get("quarter", "")
        office_filter = request.args.get("office", "")
        strategy_filter = request.args.get("strategy", "")

        # Dropdown options
        cursor.execute("SELECT DISTINCT agency FROM forecast_opportunities ORDER BY agency")
        agencies = [r[0] for r in cursor.fetchall()]
        cursor.execute("SELECT DISTINCT estimated_quarter FROM forecast_opportunities ORDER BY estimated_quarter")
        quarters = [r[0] for r in cursor.fetchall()]
        cursor.execute("SELECT DISTINCT office_code FROM forecast_opportunities ORDER BY office_code")
        offices = [r[0] for r in cursor.fetchall()]
        cursor.execute("SELECT DISTINCT acquisition_strategy FROM forecast_opportunities ORDER BY acquisition_strategy")
        strategies = [r[0] for r in cursor.fetchall()]

        clauses, params = [], []
        if agency_filter:
            clauses.append("agency = ?")
            params.append(agency_filter)
        if quarter_filter:
            clauses.append("estimated_quarter = ?")
            params.append(quarter_filter)
        if office_filter:
            clauses.append("office_code = ?")
            params.append(office_filter)
        if strategy_filter:
            clauses.append("acquisition_strategy = ?")
            params.append(strategy_filter)

        where = " AND ".join(clauses) if clauses else "1=1"
        cursor.execute(f"""
            SELECT id, agency, office_code, office_name, project_description,
                   estimated_amount_category, acquisition_strategy, estimated_quarter,
                   estimated_award_date, notes
            FROM forecast_opportunities
            WHERE {where}
            ORDER BY estimated_quarter, office_code
        """, params)
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # Summary counts
        cursor.execute(f"SELECT COUNT(*) FROM forecast_opportunities WHERE {where}", params)
        total = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM forecast_opportunities WHERE estimated_amount_category LIKE '%Above%' AND {where}", params)
        above_sat = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT estimated_quarter, COUNT(*) FROM forecast_opportunities
            WHERE {where} GROUP BY estimated_quarter ORDER BY estimated_quarter
        """, params)
        by_quarter = cursor.fetchall()

        cursor.execute(f"""
            SELECT acquisition_strategy, COUNT(*) FROM forecast_opportunities
            WHERE {where} GROUP BY acquisition_strategy ORDER BY COUNT(*) DESC
        """, params)
        by_strategy = cursor.fetchall()

        cursor.execute(f"""
            SELECT agency, COUNT(*) FROM forecast_opportunities
            WHERE {where} GROUP BY agency ORDER BY COUNT(*) DESC
        """, params)
        by_agency = cursor.fetchall()

        return render_template("forecast.html",
                               rows=rows,
                               total=total,
                               above_sat=above_sat,
                               by_quarter=by_quarter,
                               by_strategy=by_strategy,
                               by_agency=by_agency,
                               agencies=agencies,
                               quarters=quarters,
                               offices=offices,
                               strategies=strategies,
                               filters={"agency": agency_filter,
                                        "quarter": quarter_filter,
                                        "office": office_filter,
                                        "strategy": strategy_filter})
    finally:
        db.close()


@app.route("/search")
def search():
    db = _get_db()
    try:
        cursor = db.conn.cursor()

        # Populate filter dropdowns
        cursor.execute("SELECT DISTINCT department FROM solicitations WHERE department IS NOT NULL ORDER BY department")
        agencies = [r[0] for r in cursor.fetchall()]
        cursor.execute("SELECT DISTINCT naics_code FROM solicitations WHERE naics_code IS NOT NULL ORDER BY naics_code")
        naics_codes = [r[0] for r in cursor.fetchall()]
        cursor.execute("SELECT DISTINCT set_aside FROM solicitations WHERE set_aside IS NOT NULL ORDER BY set_aside")
        set_asides = [r[0] for r in cursor.fetchall()]

        filters = {
            "agency": request.args.get("agency", ""),
            "naics": request.args.get("naics", ""),
            "set_aside": request.args.get("set_aside", ""),
            "date_from": request.args.get("date_from", ""),
            "date_to": request.args.get("date_to", ""),
            "keyword": request.args.get("keyword", ""),
        }

        # Only run query if at least one filter is set
        has_filter = any(filters.values())
        results = None
        if has_filter:
            clauses = []
            params = []
            if filters["agency"]:
                clauses.append("department LIKE ?")
                params.append(f"%{filters['agency']}%")
            if filters["naics"]:
                clauses.append("naics_code = ?")
                params.append(filters["naics"])
            if filters["set_aside"]:
                clauses.append("set_aside = ?")
                params.append(filters["set_aside"])
            if filters["date_from"]:
                clauses.append("posted_date >= ?")
                params.append(filters["date_from"])
            if filters["date_to"]:
                clauses.append("posted_date <= ?")
                params.append(filters["date_to"])
            if filters["keyword"]:
                clauses.append("title LIKE ?")
                params.append(f"%{filters['keyword']}%")

            where = " AND ".join(clauses) if clauses else "1=1"
            cursor.execute(f"""
                SELECT notice_id, title, department, naics_code, set_aside, posted_date, response_deadline
                FROM solicitations
                WHERE {where}
                ORDER BY posted_date DESC
                LIMIT 200
            """, params)
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, row)) for row in cursor.fetchall()]

        return render_template("search.html",
                               agencies=agencies,
                               naics_codes=naics_codes,
                               set_asides=set_asides,
                               filters=filters,
                               results=results)
    finally:
        db.close()


@app.route("/reports")
def reports():
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
    files = []
    if os.path.isdir(reports_dir):
        for name in sorted(os.listdir(reports_dir), reverse=True):
            if name.endswith(".html"):
                path = os.path.join(reports_dir, name)
                stat = os.stat(path)
                files.append({
                    "name": name,
                    "size": f"{stat.st_size / 1024:.1f} KB",
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                })
    return render_template("reports.html", files=files)


@app.route("/reports/<path:filename>")
def serve_report(filename):
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
    return send_from_directory(reports_dir, filename)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procurement Dashboard")
    parser.add_argument("--debug", action="store_true", help="Enable debug/auto-reload")
    parser.add_argument("--port", type=int, default=5000, help="Port (default 5000)")
    args = parser.parse_args()
    app.run(debug=args.debug, port=args.port)
