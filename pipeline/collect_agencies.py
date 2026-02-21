"""
Targeted collection of opportunities from specific financial regulatory agencies.
Uses keyword search to efficiently find agency-specific opportunities.

Usage:
    SAM_API_KEY=your-key python3 collect_agencies.py
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config
from sam_api import SAMApiClient
from database import ProcurementDatabase
from datetime import datetime, timedelta
import time

# Agencies to collect, with keywords and full names for matching
AGENCIES = [
    {
        "keyword": "FHFA",
        "full_name": "FEDERAL HOUSING FINANCE AGENCY",
        "label": "FHFA",
    },
    {
        "keyword": "Securities and Exchange Commission",
        "full_name": "SECURITIES AND EXCHANGE COMMISSION",
        "label": "SEC",
    },
    {
        "keyword": "Office of the Comptroller of the Currency",
        "full_name": "COMPTROLLER OF THE CURRENCY",
        "label": "OCC",
    },
    {
        "keyword": "Consumer Financial Protection Bureau",
        "full_name": "CONSUMER FINANCIAL PROTECTION",
        "label": "CFPB",
    },
    {
        "keyword": "Farm Credit Administration",
        "full_name": "FARM CREDIT ADMINISTRATION",
        "label": "FCA",
    },
]

NOTICE_TYPES = [
    "Solicitation",
    "Combined Synopsis/Solicitation",
    "Presolicitation",
    "Sources Sought",
    "Special Notice",
]


def collect_agency_opportunities(days_back: int = 365):
    """Pull opportunities for each target agency using keyword search."""
    print("=" * 80)
    print(f"Agency-Targeted Collection - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    config.SAM_API_KEY = config.SAM_API_KEY or __import__("os").environ.get("SAM_API_KEY", "")
    if not config.SAM_API_KEY:
        print("ERROR: Set SAM_API_KEY environment variable")
        return

    client = SAMApiClient()
    client.api_key = config.SAM_API_KEY
    db = ProcurementDatabase()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    # SAM.gov requires date ranges within 1 year
    date_ranges = []
    cursor = start_date
    while cursor < end_date:
        chunk_end = min(cursor + timedelta(days=180), end_date)
        date_ranges.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end + timedelta(days=1)

    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Agencies: {', '.join(a['label'] for a in AGENCIES)}")
    print(f"Notice types: {len(NOTICE_TYPES)}")
    print()

    grand_total = 0

    for agency in AGENCIES:
        print(f"\n{'─' * 60}")
        print(f"  {agency['label']} - {agency['full_name']}")
        print(f"{'─' * 60}")
        agency_count = 0

        for notice_type in NOTICE_TYPES:
            for from_d, to_d in date_ranges:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        opps = client.get_opportunities_paginated(
                            max_results=5000,
                            page_size=100,
                            posted_from=from_d,
                            posted_to=to_d,
                            notice_type=notice_type,
                            keyword=agency["keyword"],
                        )

                        # Filter to confirm agency match (keyword search may return broader results)
                        matched = [
                            o for o in opps
                            if agency["full_name"] in (o.department or "").upper()
                            or agency["label"].upper() in (o.department or "").upper()
                            or agency["full_name"] in (o.sub_tier or "").upper()
                            or agency["full_name"] in (o.office or "").upper()
                        ]

                        for o in matched:
                            sol = {
                                "notice_id": o.notice_id,
                                "solicitation_number": o.solicitation_number,
                                "title": o.title,
                                "description": o.description,
                                "department": o.department,
                                "sub_tier": o.sub_tier,
                                "office": o.office,
                                "naics_code": o.naics_code,
                                "naics_description": o.naics_description,
                                "set_aside": o.set_aside,
                                "type_of_notice": notice_type,
                                "posted_date": o.posted_date.isoformat() if o.posted_date else None,
                                "response_deadline": o.response_deadline.isoformat() if o.response_deadline else None,
                                "place_of_performance_city": o.place_of_performance,
                                "primary_contact_name": o.primary_contact,
                                "primary_contact_email": o.primary_contact_email,
                                "url": o.url,
                            }
                            db.insert_solicitation(sol)
                            agency_count += 1
                            print(f"    [{notice_type[:12]:12s}] {o.title[:60]}")

                        # Rate limit: pause between requests
                        time.sleep(3)
                        break  # success — exit retry loop

                    except Exception as e:
                        if "429" in str(e) and attempt < max_retries - 1:
                            wait = 10 * (attempt + 1)
                            print(f"    RATE LIMITED ({notice_type}, {from_d}) — waiting {wait}s (retry {attempt + 1}/{max_retries})")
                            time.sleep(wait)
                        else:
                            print(f"    ERROR ({notice_type}, {from_d}): {e}")
                            time.sleep(5)

        print(f"  >> {agency['label']} total: {agency_count}")
        grand_total += agency_count

    # Log the collection
    cursor = db.conn.cursor()
    cursor.execute(
        """INSERT INTO collection_log
           (collection_date, opportunities_collected, new_opportunities, updated_opportunities, errors_count)
           VALUES (?, ?, ?, ?, ?)""",
        (datetime.now().isoformat(), grand_total, grand_total, 0, 0),
    )
    db.conn.commit()

    # Summary
    print()
    print("=" * 80)
    print("COLLECTION SUMMARY")
    print("=" * 80)
    print(f"Total opportunities saved: {grand_total}")
    for agency in AGENCIES:
        cursor.execute(
            "SELECT COUNT(*) FROM solicitations WHERE UPPER(department) LIKE ?",
            (f"%{agency['full_name']}%",),
        )
        count = cursor.fetchone()[0]
        print(f"  {agency['label']:6s}: {count} in database")

    cursor.execute("SELECT COUNT(*) FROM solicitations")
    print(f"\nTotal opportunities in database: {cursor.fetchone()[0]}")
    db.close()
    print("Done!")


if __name__ == "__main__":
    import sys
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 365
    collect_agency_opportunities(days_back=days)
