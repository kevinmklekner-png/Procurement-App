"""Tests for the procurement database and analytics modules."""

import unittest
import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analysis'))
from database import ProcurementDatabase
from analytics import ProcurementAnalytics


class TestProcurementDatabase(unittest.TestCase):
    """Tests for ProcurementDatabase."""

    def setUp(self):
        self.db = ProcurementDatabase(':memory:')
        self._insert_sample_data()

    def tearDown(self):
        self.db.close()

    def _insert_sample_data(self):
        today = datetime.now()
        future = (today + timedelta(days=30)).isoformat()
        past = (today - timedelta(days=10)).isoformat()

        self.sample_solicitations = [
            {
                'notice_id': 'NOTICE-001',
                'solicitation_number': 'SOL-001',
                'title': 'IT Services Contract',
                'description': 'Need IT support services',
                'department': 'Department of Defense',
                'sub_tier': 'Army',
                'office': 'MICC',
                'naics_code': '541512',
                'naics_description': 'Computer Systems Design Services',
                'set_aside': 'SBA',
                'type_of_notice': 'Solicitation',
                'posted_date': past,
                'response_deadline': future,
                'place_of_performance_city': 'Washington',
                'primary_contact_name': 'John Smith',
                'primary_contact_email': 'john@example.gov',
                'url': 'https://sam.gov/opp/1',
            },
            {
                'notice_id': 'NOTICE-002',
                'solicitation_number': 'SOL-002',
                'title': 'Cybersecurity Assessment',
                'description': 'Annual cybersecurity audit',
                'department': 'Department of Defense',
                'sub_tier': 'Navy',
                'office': 'NAVFAC',
                'naics_code': '541512',
                'naics_description': 'Computer Systems Design Services',
                'set_aside': 'SBA',
                'type_of_notice': 'Solicitation',
                'posted_date': past,
                'response_deadline': future,
                'url': 'https://sam.gov/opp/2',
            },
            {
                'notice_id': 'NOTICE-003',
                'solicitation_number': 'SOL-003',
                'title': 'Office Supplies',
                'description': 'Bulk office supply purchase',
                'department': 'General Services Administration',
                'naics_code': '339940',
                'naics_description': 'Office Supplies Manufacturing',
                'set_aside': None,
                'type_of_notice': 'Combined Synopsis/Solicitation',
                'posted_date': past,
                'response_deadline': future,
                'url': 'https://sam.gov/opp/3',
            },
        ]

        for sol in self.sample_solicitations:
            self.db.insert_solicitation(sol)

        # Mark SBA set-asides as small business
        cursor = self.db.conn.cursor()
        cursor.execute(
            "UPDATE solicitations SET is_small_business_setaside = 1 WHERE set_aside = 'SBA'"
        )
        self.db.conn.commit()

    def test_tables_created(self):
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        self.assertIn('solicitations', tables)
        self.assertIn('agencies', tables)
        self.assertIn('naics_codes', tables)
        self.assertIn('contract_awards', tables)
        self.assertIn('collection_log', tables)
        self.assertIn('set_aside_stats', tables)

    def test_insert_solicitation(self):
        cursor = self.db.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM solicitations')
        self.assertEqual(cursor.fetchone()[0], 3)

    def test_insert_duplicate_notice_id(self):
        dup = {
            'notice_id': 'NOTICE-001',
            'title': 'Updated Title',
        }
        self.db.insert_solicitation(dup)
        cursor = self.db.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM solicitations')
        # Should still be 3 (replaced, not duplicated)
        self.assertEqual(cursor.fetchone()[0], 3)

    def test_get_agency_stats(self):
        stats = self.db.get_agency_stats('Department of Defense')
        self.assertEqual(stats['total_opps'], 2)
        self.assertEqual(stats['small_biz_opps'], 2)

    def test_get_agency_stats_no_results(self):
        stats = self.db.get_agency_stats('Nonexistent Agency')
        self.assertEqual(stats['total_opps'], 0)

    def test_get_naics_stats(self):
        stats = self.db.get_naics_stats('541512')
        self.assertEqual(stats['total_opps'], 2)

    def test_get_trends(self):
        trends = self.db.get_trends(30)
        self.assertIsInstance(trends, list)
        self.assertGreater(len(trends), 0)
        self.assertIn('date', trends[0])
        self.assertIn('count', trends[0])

    def test_indexes_created(self):
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
        indexes = [row[0] for row in cursor.fetchall()]
        self.assertIn('idx_posted_date', indexes)
        self.assertIn('idx_department', indexes)
        self.assertIn('idx_naics', indexes)
        self.assertIn('idx_set_aside', indexes)
        self.assertIn('idx_response_deadline', indexes)


class TestProcurementAnalytics(unittest.TestCase):
    """Tests for ProcurementAnalytics."""

    def setUp(self):
        self.analytics = ProcurementAnalytics(':memory:')
        self._insert_sample_data()

    def tearDown(self):
        self.analytics.close()

    def _insert_sample_data(self):
        db = self.analytics.db
        today = datetime.now()
        future = (today + timedelta(days=30)).isoformat()
        recent = (today - timedelta(days=5)).isoformat()
        older = (today - timedelta(days=20)).isoformat()

        solicitations = [
            {
                'notice_id': f'N-{i}',
                'solicitation_number': f'S-{i}',
                'title': f'Opportunity {i}',
                'department': dept,
                'naics_code': naics,
                'naics_description': naics_desc,
                'set_aside': sa,
                'posted_date': date,
                'response_deadline': future,
                'url': f'https://sam.gov/{i}',
            }
            for i, (dept, naics, naics_desc, sa, date) in enumerate([
                ('Department of Defense', '541512', 'Computer Systems Design', 'SBA', recent),
                ('Department of Defense', '541512', 'Computer Systems Design', 'SBA', recent),
                ('Department of Defense', '541330', 'Engineering Services', None, older),
                ('General Services Administration', '339940', 'Office Supplies', None, recent),
                ('General Services Administration', '541512', 'Computer Systems Design', 'SBA', older),
                ('Department of Energy', '541712', 'R&D Physical Sciences', None, recent),
            ])
        ]

        for sol in solicitations:
            db.insert_solicitation(sol)

        cursor = db.conn.cursor()
        cursor.execute(
            "UPDATE solicitations SET is_small_business_setaside = 1 WHERE set_aside = 'SBA'"
        )
        db.conn.commit()

    def test_agency_opportunity_report(self):
        report = self.analytics.agency_opportunity_report(limit=10)
        self.assertIsInstance(report, list)
        self.assertGreater(len(report), 0)
        # DoD should be first with 3 opportunities
        self.assertEqual(report[0]['department'], 'Department of Defense')
        self.assertEqual(report[0]['total_opportunities'], 3)
        self.assertIn('small_biz_percentage', report[0])

    def test_naics_market_analysis(self):
        report = self.analytics.naics_market_analysis(limit=10)
        self.assertIsInstance(report, list)
        self.assertGreater(len(report), 0)
        # 541512 should be first with 3 opportunities
        self.assertEqual(report[0]['naics_code'], '541512')
        self.assertEqual(report[0]['total_opportunities'], 3)

    def test_set_aside_trends(self):
        trends = self.analytics.set_aside_trends()
        self.assertIsInstance(trends, list)
        self.assertGreater(len(trends), 0)
        self.assertIn('set_aside_type', trends[0])
        self.assertIn('count', trends[0])
        self.assertIn('percentage', trends[0])

    def test_trending_opportunities(self):
        result = self.analytics.trending_opportunities(days=30)
        self.assertIn('growing', result)
        self.assertIn('shrinking', result)
        self.assertIsInstance(result['growing'], list)
        self.assertIsInstance(result['shrinking'], list)

    def test_agency_deep_dive(self):
        result = self.analytics.agency_deep_dive('Department of Defense')
        self.assertEqual(result['agency_name'], 'Department of Defense')
        self.assertEqual(result['overall_stats']['total_opportunities'], 3)
        self.assertIn('top_naics_codes', result)
        self.assertIn('set_aside_distribution', result)
        self.assertIn('posting_patterns', result)

    def test_agency_deep_dive_nonexistent(self):
        result = self.analytics.agency_deep_dive('Fake Agency')
        self.assertEqual(result['overall_stats']['total_opportunities'], 0)

    def test_small_business_opportunity_finder(self):
        opps = self.analytics.small_business_opportunity_finder()
        self.assertIsInstance(opps, list)
        # All results should be small business set-asides with future deadlines
        for opp in opps:
            self.assertIsNotNone(opp.get('solicitation_number'))

    def test_small_business_opportunity_finder_with_naics_filter(self):
        opps = self.analytics.small_business_opportunity_finder(naics_codes=['541512'])
        for opp in opps:
            self.assertEqual(opp['naics_code'], '541512')

    def test_competitive_landscape(self):
        result = self.analytics.competitive_landscape('541512')
        self.assertEqual(result['naics_code'], '541512')
        self.assertIn('top_agencies', result)
        self.assertIn('competition_level', result)
        self.assertIn('recent_30day_count', result)

    def test_generate_market_summary(self):
        summary = self.analytics.generate_market_summary()
        self.assertIn('database_stats', summary)
        self.assertEqual(summary['database_stats']['total_opportunities'], 6)
        self.assertIn('top_agencies', summary)
        self.assertIn('top_naics', summary)
        self.assertIn('set_aside_distribution', summary)

    def test_export_report_json(self):
        import json
        filename = '/tmp/test_report.json'
        data = {'test': 'data', 'count': 42}
        self.analytics.export_report_json(data, filename)
        with open(filename) as f:
            loaded = json.load(f)
        self.assertEqual(loaded, data)
        os.remove(filename)


if __name__ == '__main__':
    unittest.main()
