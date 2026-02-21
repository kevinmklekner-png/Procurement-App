#!/usr/bin/env python3
"""
CLI for generating procurement HTML reports.

Usage:
    python3 run_reports.py daily              # Daily digest + agency reports
    python3 run_reports.py weekly             # Weekly digest + agency + market summary
    python3 run_reports.py --agency FHFA      # Single agency report
    python3 run_reports.py --sow <notice_id>  # Single SOW review report
    python3 run_reports.py --market           # Market summary only
"""

import argparse
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'analysis'))
from database import ProcurementDatabase
from report_generator import (
    generate_agency_report,
    generate_new_opportunities_report,
    generate_sow_review_report,
    generate_market_summary_report,
    save_report,
)

AGENCIES = ["FHFA", "SEC", "OCC", "CFPB", "FCA"]


def run_daily():
    db = ProcurementDatabase()
    try:
        # Daily digest
        html = generate_new_opportunities_report(db, days_back=1)
        path = save_report(html, "daily_digest")
        print(f"  Daily digest -> {path}")

        # Per-agency reports
        for agency in AGENCIES:
            html = generate_agency_report(db, agency)
            path = save_report(html, "agency", agency)
            print(f"  {agency} report -> {path}")
    finally:
        db.close()


def run_weekly():
    db = ProcurementDatabase()
    try:
        # Weekly digest
        html = generate_new_opportunities_report(db, days_back=7)
        path = save_report(html, "weekly_digest")
        print(f"  Weekly digest -> {path}")

        # Per-agency reports
        for agency in AGENCIES:
            html = generate_agency_report(db, agency)
            path = save_report(html, "agency", agency)
            print(f"  {agency} report -> {path}")

        # Market summary
        html = generate_market_summary_report(db)
        path = save_report(html, "market_summary")
        print(f"  Market summary -> {path}")
    finally:
        db.close()


def run_agency(agency_name):
    db = ProcurementDatabase()
    try:
        html = generate_agency_report(db, agency_name)
        path = save_report(html, "agency", agency_name)
        print(f"  {agency_name} report -> {path}")
    finally:
        db.close()


def run_sow(notice_id):
    db = ProcurementDatabase()
    try:
        html = generate_sow_review_report(db, notice_id)
        path = save_report(html, "sow_review", notice_id.replace("/", "_"))
        print(f"  SOW review -> {path}")
    finally:
        db.close()


def run_market():
    db = ProcurementDatabase()
    try:
        html = generate_market_summary_report(db)
        path = save_report(html, "market_summary")
        print(f"  Market summary -> {path}")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Generate procurement HTML reports")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("mode", nargs="?", choices=["daily", "weekly"],
                       help="Run daily or weekly report batch")
    group.add_argument("--agency", type=str, help="Generate report for a single agency")
    group.add_argument("--sow", type=str, metavar="NOTICE_ID",
                       help="Generate SOW review report for a notice")
    group.add_argument("--market", action="store_true", help="Generate market summary report")

    args = parser.parse_args()

    print("Procurement Report Generator")
    print("=" * 40)

    if args.mode == "daily":
        run_daily()
    elif args.mode == "weekly":
        run_weekly()
    elif args.agency:
        run_agency(args.agency)
    elif args.sow:
        run_sow(args.sow)
    elif args.market:
        run_market()

    print("\nDone. Reports saved to reports/ directory.")


if __name__ == "__main__":
    main()
