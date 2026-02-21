"""
SOW/PWS review and evaluation criteria analysis.

Provides human-readable reviews of solicitation documents
and cross-agency comparison queries.

Usage:
    python3 sow_review.py --notice-id <id>
    python3 sow_review.py --compare FHFA SEC OCC
    python3 sow_review.py --labor-cats FHFA
    python3 sow_review.py --list FHFA
"""

import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from database import ProcurementDatabase


class SOWReviewer:
    """Review and analyze parsed solicitation documents."""

    def __init__(self, db: ProcurementDatabase):
        self.db = db
        self.cursor = db.conn.cursor()

    def get_labor_categories(self, notice_id: str) -> list:
        """Get structured labor categories from the labor_categories table."""
        self.cursor.execute('''
            SELECT category_name, category_title, clin_number,
                   hourly_rate, estimated_hours, extended_price,
                   period_name, period_number, site_type, source_file
            FROM labor_categories
            WHERE notice_id = ?
            ORDER BY period_number, site_type, category_name
        ''', (notice_id,))
        return [dict(r) for r in self.cursor.fetchall()]

    def common_labor_categories_structured(self, agency: str) -> list:
        """Aggregate structured labor categories with rate ranges for an agency.

        Returns list of dicts: category_name, min_rate, max_rate, avg_rate,
        total_hours, solicitation_count.
        """
        self.cursor.execute('''
            SELECT category_name,
                   MIN(hourly_rate) as min_rate,
                   MAX(hourly_rate) as max_rate,
                   AVG(hourly_rate) as avg_rate,
                   SUM(estimated_hours) as total_hours,
                   COUNT(DISTINCT notice_id) as solicitation_count
            FROM labor_categories
            WHERE agency LIKE ? AND category_name IS NOT NULL
            GROUP BY category_name
            ORDER BY solicitation_count DESC, category_name
        ''', (f"%{agency}%",))
        return [dict(r) for r in self.cursor.fetchall()]

    def review_opportunity(self, notice_id: str) -> dict:
        """Return a full structured review dict for a notice."""
        # Solicitation metadata
        self.cursor.execute(
            "SELECT * FROM solicitations WHERE notice_id = ?", (notice_id,)
        )
        sol = self.cursor.fetchone()
        meta = dict(sol) if sol else {}

        # Documents
        self.cursor.execute(
            "SELECT * FROM opportunity_documents WHERE notice_id = ?", (notice_id,)
        )
        docs = [dict(r) for r in self.cursor.fetchall()]

        # SOW analysis
        self.cursor.execute(
            "SELECT * FROM sow_analysis WHERE notice_id = ?", (notice_id,)
        )
        sow_rows = [dict(r) for r in self.cursor.fetchall()]
        for row in sow_rows:
            for field in ("key_tasks", "labor_categories", "deliverables", "compliance_reqs"):
                if row.get(field):
                    row[field] = json.loads(row[field])

        # Evaluation criteria
        self.cursor.execute(
            "SELECT * FROM evaluation_criteria WHERE notice_id = ? ORDER BY factor_number",
            (notice_id,)
        )
        eval_rows = [dict(r) for r in self.cursor.fetchall()]
        for row in eval_rows:
            if row.get("subfactors"):
                row["subfactors"] = json.loads(row["subfactors"])

        # Structured labor categories from Excel imports
        structured_labor = self.get_labor_categories(notice_id)

        return {
            "metadata": meta,
            "documents": docs,
            "sow_analysis": sow_rows,
            "evaluation_criteria": eval_rows,
            "structured_labor": structured_labor,
        }

    def print_review(self, notice_id: str):
        """Pretty-print a human-readable review."""
        review = self.review_opportunity(notice_id)
        meta = review["metadata"]

        if not meta:
            print(f"No solicitation found for notice_id: {notice_id}")
            return

        # Header
        print("=" * 70)
        print(f"SOLICITATION REVIEW: {meta.get('title', 'N/A')}")
        print("=" * 70)
        print(f"Notice ID:     {notice_id}")
        print(f"Sol. Number:   {meta.get('solicitation_number', 'N/A')}")
        print(f"Agency:        {meta.get('department', 'N/A')}")
        print(f"Sub-tier:      {meta.get('sub_tier', 'N/A')}")
        print(f"Office:        {meta.get('office', 'N/A')}")
        print(f"NAICS:         {meta.get('naics_code', 'N/A')} - {meta.get('naics_description', '')}")
        print(f"Set-aside:     {meta.get('set_aside', 'None')}")
        print(f"Posted:        {meta.get('posted_date', 'N/A')}")
        print(f"Deadline:      {meta.get('response_deadline', 'N/A')}")
        print(f"Location:      {meta.get('place_of_performance_city', '')}, {meta.get('place_of_performance_state', '')}")

        # Documents summary
        docs = review["documents"]
        if docs:
            print(f"\nDOCUMENTS ({len(docs)}):")
            for d in docs:
                print(f"  - {d['filename']} ({d['file_type']}) → {d['doc_role']}")
                text_len = len(d.get("raw_text", "") or "")
                print(f"    {text_len:,} chars extracted")

        # SOW Analysis
        for sow in review["sow_analysis"]:
            print("\n" + "-" * 70)
            print("SOW / PWS ANALYSIS")
            print("-" * 70)

            if sow.get("scope_summary"):
                print(f"\nSCOPE:")
                _print_wrapped(sow["scope_summary"], indent=2)

            if sow.get("period_of_performance"):
                print(f"\nPERIOD OF PERFORMANCE: {sow['period_of_performance']}")

            if sow.get("place_of_performance"):
                print(f"PLACE OF PERFORMANCE: {sow['place_of_performance']}")

            tasks = sow.get("key_tasks", [])
            if tasks:
                print(f"\nKEY TASKS ({len(tasks)}):")
                for i, t in enumerate(tasks, 1):
                    print(f"  {i}. {t}")

            cats = sow.get("labor_categories", [])
            if cats:
                print(f"\nLABOR CATEGORIES ({len(cats)}):")
                for c in cats:
                    print(f"  - {c}")

            deliverables = sow.get("deliverables", [])
            if deliverables:
                print(f"\nDELIVERABLES ({len(deliverables)}):")
                for d in deliverables:
                    print(f"  - {d}")

            compliance = sow.get("compliance_reqs", [])
            if compliance:
                print(f"\nCOMPLIANCE REQUIREMENTS:")
                print(f"  {', '.join(compliance)}")

            print(f"\nConfidence: {sow.get('confidence_score', 0):.0%}")

        # Evaluation Criteria
        eval_factors = review["evaluation_criteria"]
        if eval_factors:
            print("\n" + "-" * 70)
            print("EVALUATION CRITERIA")
            print("-" * 70)
            for f in eval_factors:
                phase_str = f" [{f['evaluation_phase']}]" if f.get("evaluation_phase") else ""
                print(f"\n  Factor {f['factor_number']}: {f['factor_name']}{phase_str}")
                if f.get("factor_weight"):
                    print(f"    Weight: {f['factor_weight']}")
                subs = f.get("subfactors", [])
                if subs:
                    print(f"    Subfactors:")
                    for s in subs:
                        print(f"      - {s}")
                if f.get("page_limit"):
                    print(f"    Page limit: {f['page_limit']}")
                if f.get("rating_method"):
                    print(f"    Rating method: {f['rating_method']}")

        if not review["sow_analysis"] and not eval_factors:
            print("\nNo parsed SOW or evaluation criteria found.")
            print("Run collect_documents.py first to download and parse documents.")

        print()

    def compare_agencies_eval_criteria(self, agencies) -> dict:
        """Cross-agency evaluation factor frequency analysis."""
        results = {}
        for agency in agencies:
            self.cursor.execute('''
                SELECT ec.factor_name, COUNT(*) as cnt
                FROM evaluation_criteria ec
                JOIN solicitations s ON ec.notice_id = s.notice_id
                WHERE s.department LIKE ?
                GROUP BY ec.factor_name
                ORDER BY cnt DESC
            ''', (f"%{agency}%",))
            results[agency] = {row["factor_name"]: row["cnt"] for row in self.cursor.fetchall()}
        return results

    def common_labor_categories(self, agency: str):
        """Aggregate labor categories across SOWs for an agency."""
        self.cursor.execute('''
            SELECT sa.labor_categories
            FROM sow_analysis sa
            JOIN solicitations s ON sa.notice_id = s.notice_id
            WHERE s.department LIKE ?
        ''', (f"%{agency}%",))

        counter = Counter()
        for row in self.cursor.fetchall():
            cats = json.loads(row["labor_categories"]) if row["labor_categories"] else []
            counter.update(cats)
        return counter.most_common(30)

    def opportunities_with_documents(self, agency: str):
        """List opportunities that have parsed documents."""
        self.cursor.execute('''
            SELECT s.notice_id, s.title, s.posted_date,
                   COUNT(d.id) as doc_count,
                   GROUP_CONCAT(DISTINCT d.doc_role) as roles
            FROM solicitations s
            JOIN opportunity_documents d ON s.notice_id = d.notice_id
            WHERE s.department LIKE ?
            GROUP BY s.notice_id
            ORDER BY s.posted_date DESC
        ''', (f"%{agency}%",))
        return [dict(r) for r in self.cursor.fetchall()]


def _print_wrapped(text: str, indent: int = 0, width: int = 68):
    """Print text wrapped to width with indent."""
    prefix = " " * indent
    words = text.split()
    line = prefix
    for word in words:
        if len(line) + len(word) + 1 > width:
            print(line)
            line = prefix + word
        else:
            line = line + " " + word if line.strip() else prefix + word
    if line.strip():
        print(line)


def main():
    parser = argparse.ArgumentParser(description="Review SOW/PWS and evaluation criteria")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--notice-id", help="Review a single opportunity")
    group.add_argument("--compare", nargs="+", metavar="AGENCY",
                       help="Compare evaluation criteria across agencies")
    group.add_argument("--labor-cats", metavar="AGENCY",
                       help="Show common labor categories for an agency")
    group.add_argument("--list", metavar="AGENCY",
                       help="List opportunities with parsed documents")
    args = parser.parse_args()

    db = ProcurementDatabase()
    reviewer = SOWReviewer(db)

    try:
        if args.notice_id:
            reviewer.print_review(args.notice_id)

        elif args.compare:
            results = reviewer.compare_agencies_eval_criteria(args.compare)
            print("=" * 60)
            print("CROSS-AGENCY EVALUATION CRITERIA COMPARISON")
            print("=" * 60)
            for agency, factors in results.items():
                print(f"\n{agency}:")
                if not factors:
                    print("  No evaluation criteria found.")
                    continue
                for name, count in factors.items():
                    print(f"  {name}: {count} occurrence(s)")

            # Show common factors
            all_factors = set()
            for factors in results.values():
                all_factors.update(factors.keys())
            if len(results) > 1:
                print(f"\nCommon factors across agencies:")
                for factor in sorted(all_factors):
                    agencies_with = [a for a, f in results.items() if factor in f]
                    if len(agencies_with) > 1:
                        print(f"  {factor}: used by {', '.join(agencies_with)}")

        elif args.labor_cats:
            cats = reviewer.common_labor_categories(args.labor_cats)
            print(f"Common Labor Categories for {args.labor_cats}:")
            if not cats:
                print("  None found. Run collect_documents.py first.")
            for name, count in cats:
                print(f"  {name}: {count}")

        elif args.list:
            opps = reviewer.opportunities_with_documents(args.list)
            print(f"Opportunities with documents for {args.list}:")
            if not opps:
                print("  None found. Run collect_documents.py first.")
            for o in opps:
                print(f"  {o['notice_id']} | {o['posted_date']} | {o['doc_count']} docs ({o['roles']})")
                print(f"    {o['title']}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
