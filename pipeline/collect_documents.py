"""
Download and parse solicitation documents for stored opportunities.

Fetches description HTML and resource link attachments (PDF/DOCX) from SAM.gov,
extracts text, classifies documents, and stores structured analysis.

Usage:
    python3 collect_documents.py --notice-id <id>
    python3 collect_documents.py --agency FHFA
    python3 collect_documents.py --all
"""

import argparse
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'analysis'))
import config
from sam_api import SAMApiClient
from database import ProcurementDatabase
from doc_parser import (
    extract_text, classify_document,
    build_sow_analysis, extract_evaluation_factors,
)

RATE_LIMIT_DELAY = 2  # seconds between API calls


def get_solicitation_id(db, notice_id: str):
    """Look up the solicitations table id for a notice_id."""
    cursor = db.conn.cursor()
    cursor.execute("SELECT id FROM solicitations WHERE notice_id = ?", (notice_id,))
    row = cursor.fetchone()
    return row["id"] if row else None


def store_document(db, doc: dict):
    """Insert a document record and return its id."""
    cursor = db.conn.cursor()
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO opportunity_documents
                (solicitation_id, notice_id, filename, file_url, file_type,
                 doc_role, raw_text, description_html,
                 download_status, parse_status, error_message,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            doc.get("solicitation_id"),
            doc["notice_id"],
            doc.get("filename", ""),
            doc["file_url"],
            doc.get("file_type", ""),
            doc.get("doc_role", "unknown"),
            doc.get("raw_text", ""),
            doc.get("description_html", ""),
            doc.get("download_status", "complete"),
            doc.get("parse_status", "complete"),
            doc.get("error_message", ""),
            datetime.now().isoformat(),
            datetime.now().isoformat(),
        ))
        db.conn.commit()
        return cursor.lastrowid if cursor.lastrowid else None
    except Exception as e:
        print(f"  Error storing document: {e}")
        db.conn.rollback()
        return None


def store_sow_analysis(db, analysis: dict):
    """Insert a SOW analysis record."""
    cursor = db.conn.cursor()
    cursor.execute('''
        INSERT INTO sow_analysis
            (document_id, solicitation_id, notice_id,
             scope_summary, period_of_performance, place_of_performance,
             key_tasks, labor_categories, deliverables,
             compliance_reqs, ordering_mechanism, billing_instructions,
             confidence_score, extraction_method, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        analysis["document_id"], analysis["solicitation_id"], analysis["notice_id"],
        analysis["scope_summary"], analysis["period_of_performance"],
        analysis["place_of_performance"],
        analysis["key_tasks"], analysis["labor_categories"], analysis["deliverables"],
        analysis["compliance_reqs"], analysis["ordering_mechanism"],
        analysis["billing_instructions"],
        analysis["confidence_score"], analysis["extraction_method"],
        analysis["created_at"],
    ))
    db.conn.commit()


def store_eval_factors(db, factors):
    """Insert evaluation criteria records."""
    cursor = db.conn.cursor()
    for f in factors:
        cursor.execute('''
            INSERT INTO evaluation_criteria
                (document_id, solicitation_id, notice_id,
                 evaluation_phase, factor_number, factor_name, factor_weight,
                 subfactors, description, page_limit, rating_method, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            f["document_id"], f["solicitation_id"], f["notice_id"],
            f["evaluation_phase"], f["factor_number"], f["factor_name"],
            f["factor_weight"], f["subfactors"], f["description"],
            f["page_limit"], f["rating_method"], f["created_at"],
        ))
    db.conn.commit()


def process_notice(client: SAMApiClient, db: ProcurementDatabase, notice_id: str):
    """Download and parse all documents for a single notice."""
    print(f"\nProcessing notice: {notice_id}")
    sol_id = get_solicitation_id(db, notice_id)

    # 1. Fetch description HTML
    print("  Fetching description HTML...")
    try:
        html = client.get_description_html(notice_id)
        if html:
            text = extract_text("description.html", html)
            role = classify_document("description.html", text)
            doc_id = store_document(db, {
                "solicitation_id": sol_id,
                "notice_id": notice_id,
                "filename": "description.html",
                "file_url": f"noticedesc:{notice_id}",
                "file_type": "html",
                "doc_role": role,
                "raw_text": text,
                "description_html": html,
            })
            if doc_id:
                _run_analysis(db, doc_id, sol_id, notice_id, role, text)
                print(f"  Description stored (role={role}, {len(text)} chars)")
    except Exception as e:
        print(f"  Error fetching description: {e}")

    time.sleep(RATE_LIMIT_DELAY)

    # 2. Fetch resource links
    print("  Fetching resource links...")
    try:
        links = client.get_resource_links(notice_id)
    except Exception as e:
        print(f"  Error fetching resource links: {e}")
        links = []

    if not links:
        print("  No resource links found.")
        return

    print(f"  Found {len(links)} resource link(s)")

    for link in links:
        time.sleep(RATE_LIMIT_DELAY)

        if isinstance(link, str):
            file_url = link
            filename = link.rsplit("/", 1)[-1].split("?")[0]
        elif isinstance(link, dict):
            file_url = link.get("url", link.get("href", ""))
            filename = link.get("name", link.get("filename", file_url.rsplit("/", 1)[-1]))
        else:
            continue

        if not file_url:
            continue

        # Determine file type
        lower_name = filename.lower()
        if lower_name.endswith(".pdf"):
            file_type = "pdf"
        elif lower_name.endswith(".docx"):
            file_type = "docx"
        elif lower_name.endswith(".doc"):
            file_type = "doc"
        else:
            file_type = lower_name.rsplit(".", 1)[-1] if "." in lower_name else "unknown"

        print(f"  Downloading: {filename} ({file_type})...")

        try:
            content = client.download_attachment(file_url)
            text = extract_text(filename, content)
            role = classify_document(filename, text)

            doc_id = store_document(db, {
                "solicitation_id": sol_id,
                "notice_id": notice_id,
                "filename": filename,
                "file_url": file_url,
                "file_type": file_type,
                "doc_role": role,
                "raw_text": text,
            })

            if doc_id:
                _run_analysis(db, doc_id, sol_id, notice_id, role, text)
                print(f"    Stored: role={role}, {len(text)} chars")

        except Exception as e:
            store_document(db, {
                "solicitation_id": sol_id,
                "notice_id": notice_id,
                "filename": filename,
                "file_url": file_url,
                "file_type": file_type,
                "download_status": "error",
                "parse_status": "error",
                "error_message": str(e),
            })
            print(f"    Error: {e}")


def _run_analysis(db, doc_id, sol_id, notice_id, role, text):
    """Run appropriate analysis based on document role."""
    if role in ("sow", "pws"):
        analysis = build_sow_analysis(doc_id, sol_id, notice_id, text)
        store_sow_analysis(db, analysis)

    if role in ("solicitation", "evaluation_criteria"):
        factors = extract_evaluation_factors(doc_id, sol_id, notice_id, text)
        if factors:
            store_eval_factors(db, factors)
            print(f"    Extracted {len(factors)} evaluation factors")

    # For solicitations, also try SOW extraction (they often contain both)
    if role == "solicitation":
        analysis = build_sow_analysis(doc_id, sol_id, notice_id, text)
        import json
        if json.loads(analysis["labor_categories"]) or json.loads(analysis["key_tasks"]):
            store_sow_analysis(db, analysis)
            print(f"    Also extracted SOW data from solicitation")


def collect_for_agency(client: SAMApiClient, db: ProcurementDatabase, agency: str):
    """Process all stored notices for an agency."""
    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT notice_id FROM solicitations WHERE department LIKE ?",
        (f"%{agency}%",)
    )
    notice_ids = [row["notice_id"] for row in cursor.fetchall()]
    print(f"Found {len(notice_ids)} notices for {agency}")

    for nid in notice_ids:
        # Skip already-processed notices
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM opportunity_documents WHERE notice_id = ?",
            (nid,)
        )
        if cursor.fetchone()["cnt"] > 0:
            print(f"  Skipping {nid} (already processed)")
            continue
        process_notice(client, db, nid)


def collect_all(client: SAMApiClient, db: ProcurementDatabase):
    """Process all stored notices that haven't been processed yet."""
    cursor = db.conn.cursor()
    cursor.execute('''
        SELECT s.notice_id FROM solicitations s
        LEFT JOIN opportunity_documents d ON s.notice_id = d.notice_id
        WHERE d.id IS NULL
        ORDER BY s.posted_date DESC
    ''')
    notice_ids = [row["notice_id"] for row in cursor.fetchall()]
    print(f"Found {len(notice_ids)} unprocessed notices")

    for nid in notice_ids:
        process_notice(client, db, nid)


def main():
    parser = argparse.ArgumentParser(description="Download and parse solicitation documents")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--notice-id", help="Process a single notice")
    group.add_argument("--agency", help="Process all notices for an agency")
    group.add_argument("--all", action="store_true", help="Process all unprocessed notices")
    args = parser.parse_args()

    config.validate_config()
    client = SAMApiClient()
    db = ProcurementDatabase()

    try:
        if args.notice_id:
            process_notice(client, db, args.notice_id)
        elif args.agency:
            collect_for_agency(client, db, args.agency)
        elif args.all:
            collect_all(client, db)
    finally:
        db.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
