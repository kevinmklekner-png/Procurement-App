"""
Analytics module for federal procurement data.
Generate insights that can be sold as reports or subscriptions.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from database import ProcurementDatabase
from datetime import datetime, timedelta
import json


class ProcurementAnalytics:
    """Analytics engine for federal procurement data."""
    
    def __init__(self, db_path: str = None, db=None):
        """Initialize with database connection."""
        if db is not None:
            self.db = db
        elif db_path is not None:
            self.db = ProcurementDatabase(db_path)
        else:
            self.db = ProcurementDatabase()
    
    def agency_opportunity_report(self, limit: int = 20) -> list:
        """
        Generate report of agencies with most opportunities.
        
        Returns:
            List of dicts with agency stats
        """
        cursor = self.db.conn.cursor()
        
        cursor.execute('''
            SELECT 
                department,
                COUNT(*) as total_opportunities,
                COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) as small_biz_opportunities,
                ROUND(COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) * 100.0 / COUNT(*), 1) as small_biz_percentage,
                COUNT(DISTINCT naics_code) as unique_naics_codes,
                MIN(posted_date) as first_seen,
                MAX(posted_date) as last_activity
            FROM solicitations
            WHERE department IS NOT NULL AND department != ''
            GROUP BY department
            ORDER BY total_opportunities DESC
            LIMIT ?
        ''', (limit,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def naics_market_analysis(self, limit: int = 20) -> list:
        """
        Analyze opportunity distribution by NAICS code.
        
        Returns:
            List of dicts with NAICS stats
        """
        cursor = self.db.conn.cursor()
        
        cursor.execute('''
            SELECT 
                naics_code,
                naics_description,
                COUNT(*) as total_opportunities,
                COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) as small_biz_opportunities,
                ROUND(COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) * 100.0 / COUNT(*), 1) as small_biz_percentage,
                COUNT(DISTINCT department) as number_of_agencies
            FROM solicitations
            WHERE naics_code IS NOT NULL AND naics_code != ''
            GROUP BY naics_code
            ORDER BY total_opportunities DESC
            LIMIT ?
        ''', (limit,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def set_aside_trends(self) -> list:
        """
        Analyze distribution of set-aside types.
        
        Returns:
            List of dicts with set-aside stats
        """
        cursor = self.db.conn.cursor()
        
        cursor.execute('''
            SELECT 
                COALESCE(set_aside, 'None/Unrestricted') as set_aside_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM solicitations), 1) as percentage,
                COUNT(DISTINCT department) as agencies_using,
                COUNT(DISTINCT naics_code) as naics_codes
            FROM solicitations
            GROUP BY set_aside
            ORDER BY count DESC
        ''')
        
        return [dict(row) for row in cursor.fetchall()]
    
    def trending_opportunities(self, days: int = 30) -> dict:
        """
        Identify trending opportunity areas (growing vs shrinking).
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dict with growing and shrinking categories
        """
        cursor = self.db.conn.cursor()
        
        # Compare first half vs second half of period
        midpoint = days // 2
        
        cursor.execute('''
            WITH first_half AS (
                SELECT naics_code, COUNT(*) as count
                FROM solicitations
                WHERE posted_date >= date('now', '-' || ? || ' days')
                  AND posted_date < date('now', '-' || ? || ' days')
                GROUP BY naics_code
            ),
            second_half AS (
                SELECT naics_code, COUNT(*) as count
                FROM solicitations
                WHERE posted_date >= date('now', '-' || ? || ' days')
                GROUP BY naics_code
            )
            SELECT 
                COALESCE(f.naics_code, s.naics_code) as naics_code,
                COALESCE(f.count, 0) as first_half_count,
                COALESCE(s.count, 0) as second_half_count,
                COALESCE(s.count, 0) - COALESCE(f.count, 0) as change,
                CASE 
                    WHEN COALESCE(f.count, 0) > 0 
                    THEN ROUND((COALESCE(s.count, 0) - COALESCE(f.count, 0)) * 100.0 / f.count, 1)
                    ELSE NULL
                END as percent_change
            FROM first_half f
            FULL OUTER JOIN second_half s ON f.naics_code = s.naics_code
            WHERE COALESCE(f.naics_code, s.naics_code) IS NOT NULL
            ORDER BY ABS(change) DESC
            LIMIT 20
        ''', (days, midpoint, midpoint))
        
        trends = [dict(row) for row in cursor.fetchall()]
        
        return {
            'growing': [t for t in trends if t['change'] > 0],
            'shrinking': [t for t in trends if t['change'] < 0]
        }
    
    def agency_deep_dive(self, agency_name: str) -> dict:
        """
        Deep analysis of a specific agency.
        
        Args:
            agency_name: Name of the agency to analyze
            
        Returns:
            Dict with comprehensive agency stats
        """
        cursor = self.db.conn.cursor()
        
        # Overall stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_opportunities,
                COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) as small_biz_count,
                COUNT(DISTINCT naics_code) as unique_naics,
                MIN(posted_date) as first_opportunity,
                MAX(posted_date) as latest_opportunity
            FROM solicitations
            WHERE department = ?
        ''', (agency_name,))
        
        overall = dict(cursor.fetchone())
        
        # Top NAICS codes
        cursor.execute('''
            SELECT naics_code, naics_description, COUNT(*) as count
            FROM solicitations
            WHERE department = ?
            GROUP BY naics_code
            ORDER BY count DESC
            LIMIT 10
        ''', (agency_name,))
        
        top_naics = [dict(row) for row in cursor.fetchall()]
        
        # Set-aside distribution
        cursor.execute('''
            SELECT 
                COALESCE(set_aside, 'None') as set_aside,
                COUNT(*) as count
            FROM solicitations
            WHERE department = ?
            GROUP BY set_aside
            ORDER BY count DESC
        ''', (agency_name,))
        
        set_asides = [dict(row) for row in cursor.fetchall()]
        
        # Posting patterns (day of week)
        cursor.execute('''
            SELECT 
                CASE CAST(strftime('%w', posted_date) AS INTEGER)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END as day_of_week,
                COUNT(*) as count
            FROM solicitations
            WHERE department = ?
            GROUP BY strftime('%w', posted_date)
            ORDER BY count DESC
        ''', (agency_name,))
        
        posting_patterns = [dict(row) for row in cursor.fetchall()]
        
        return {
            'agency_name': agency_name,
            'overall_stats': overall,
            'top_naics_codes': top_naics,
            'set_aside_distribution': set_asides,
            'posting_patterns': posting_patterns
        }
    
    def small_business_opportunity_finder(
        self, 
        naics_codes: list = None,
        min_days_to_deadline: int = 7
    ) -> list:
        """
        Find opportunities ideal for small businesses.
        
        Args:
            naics_codes: Optional list of NAICS codes to filter
            min_days_to_deadline: Minimum days until deadline
            
        Returns:
            List of filtered opportunities
        """
        cursor = self.db.conn.cursor()
        
        query = '''
            SELECT 
                solicitation_number,
                title,
                department,
                naics_code,
                set_aside,
                response_deadline,
                url,
                julianday(response_deadline) - julianday('now') as days_to_deadline
            FROM solicitations
            WHERE is_small_business_setaside = 1
              AND response_deadline > datetime('now')
              AND julianday(response_deadline) - julianday('now') >= ?
        '''
        
        params = [min_days_to_deadline]
        
        if naics_codes:
            placeholders = ','.join(['?' for _ in naics_codes])
            query += f' AND naics_code IN ({placeholders})'
            params.extend(naics_codes)
        
        query += ' ORDER BY response_deadline ASC LIMIT 50'
        
        cursor.execute(query, params)
        
        return [dict(row) for row in cursor.fetchall()]
    
    def competitive_landscape(self, naics_code: str) -> dict:
        """
        Analyze competitive landscape for a NAICS code.
        
        Args:
            naics_code: NAICS code to analyze
            
        Returns:
            Dict with competitive intelligence
        """
        cursor = self.db.conn.cursor()
        
        # Which agencies post most in this NAICS
        cursor.execute('''
            SELECT department, COUNT(*) as count
            FROM solicitations
            WHERE naics_code = ?
            GROUP BY department
            ORDER BY count DESC
            LIMIT 10
        ''', (naics_code,))
        
        top_agencies = [dict(row) for row in cursor.fetchall()]
        
        # Set-aside vs unrestricted
        cursor.execute('''
            SELECT 
                CASE WHEN is_small_business_setaside = 1 THEN 'Small Business' ELSE 'Unrestricted' END as category,
                COUNT(*) as count
            FROM solicitations
            WHERE naics_code = ?
            GROUP BY category
        ''', (naics_code,))
        
        competition_level = [dict(row) for row in cursor.fetchall()]
        
        # Recent activity
        cursor.execute('''
            SELECT COUNT(*) as recent_count
            FROM solicitations
            WHERE naics_code = ?
              AND posted_date >= date('now', '-30 days')
        ''', (naics_code,))
        
        recent_activity = cursor.fetchone()[0]
        
        return {
            'naics_code': naics_code,
            'top_agencies': top_agencies,
            'competition_level': competition_level,
            'recent_30day_count': recent_activity
        }
    
    def generate_market_summary(self) -> dict:
        """
        Generate comprehensive market summary report.
        
        Returns:
            Dict with overall market insights
        """
        cursor = self.db.conn.cursor()
        
        # Total opportunities
        cursor.execute('SELECT COUNT(*) FROM solicitations')
        total_opps = cursor.fetchone()[0]
        
        # Date range
        cursor.execute('SELECT MIN(posted_date), MAX(posted_date) FROM solicitations')
        date_range = cursor.fetchone()
        
        # Active opportunities (future deadlines)
        cursor.execute('''
            SELECT COUNT(*) FROM solicitations
            WHERE response_deadline > datetime('now')
        ''')
        active_opps = cursor.fetchone()[0]
        
        # Recent trends (last 30 days)
        cursor.execute('''
            SELECT COUNT(*) FROM solicitations
            WHERE posted_date >= date('now', '-30 days')
        ''')
        recent_count = cursor.fetchone()[0]
        
        return {
            'generated_at': datetime.now().isoformat(),
            'database_stats': {
                'total_opportunities': total_opps,
                'active_opportunities': active_opps,
                'date_range': {
                    'earliest': date_range[0],
                    'latest': date_range[1]
                },
                'last_30_days': recent_count
            },
            'top_agencies': self.agency_opportunity_report(10),
            'top_naics': self.naics_market_analysis(10),
            'set_aside_distribution': self.set_aside_trends()
        }
    
    def export_report_json(self, report_data: dict, filename: str):
        """Export report to JSON file."""
        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2)
        print(f"Report exported to {filename}")
    
    def close(self):
        """Close database connection."""
        self.db.close()


if __name__ == '__main__':
    # Example usage
    analytics = ProcurementAnalytics()
    
    print("Generating Market Summary Report...")
    summary = analytics.generate_market_summary()
    analytics.export_report_json(summary, 'market_summary.json')
    
    print("\nTop Agencies by Opportunity Count:")
    for i, agency in enumerate(summary['top_agencies'][:10], 1):
        print(f"{i}. {agency['department']}: {agency['total_opportunities']} opportunities")
    
    analytics.close()
