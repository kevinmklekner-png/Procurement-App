"""
Database schema for comprehensive federal procurement data collection.
This supports building a data analytics business.
"""

import sqlite3
from datetime import datetime
from typing import Optional


class ProcurementDatabase:
    """Database for storing and analyzing federal procurement data."""
    
    def __init__(self, db_path: str = 'federal_procurement.db'):
        """Initialize database connection and create tables."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
    
    def create_tables(self):
        """Create database schema."""
        cursor = self.conn.cursor()
        
        # Main solicitations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS solicitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notice_id TEXT UNIQUE NOT NULL,
                solicitation_number TEXT,
                title TEXT,
                description TEXT,
                
                -- Agency information
                department TEXT,
                sub_tier TEXT,
                office TEXT,
                full_parent_path TEXT,
                
                -- Classification
                naics_code TEXT,
                naics_description TEXT,
                psc_code TEXT,
                set_aside TEXT,
                type_of_notice TEXT,
                
                -- Dates
                posted_date TEXT,
                response_deadline TEXT,
                archive_date TEXT,
                
                -- Contract details
                estimated_value_low REAL,
                estimated_value_high REAL,
                place_of_performance_city TEXT,
                place_of_performance_state TEXT,
                place_of_performance_zip TEXT,
                place_of_performance_country TEXT,
                
                -- Contact
                primary_contact_name TEXT,
                primary_contact_email TEXT,
                primary_contact_phone TEXT,
                
                -- Metadata
                url TEXT,
                data_source TEXT,
                collected_date TEXT,
                last_updated TEXT,
                
                -- Computed fields
                days_to_deadline INTEGER,
                is_small_agency BOOLEAN,
                is_small_business_setaside BOOLEAN
            )
        ''')
        
        # Agency master list
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agency_name TEXT UNIQUE,
                agency_type TEXT,
                is_small_agency BOOLEAN,
                total_opportunities INTEGER DEFAULT 0,
                total_small_biz_opportunities INTEGER DEFAULT 0,
                avg_contract_value REAL,
                most_common_naics TEXT,
                first_seen TEXT,
                last_activity TEXT
            )
        ''')
        
        # NAICS code master list
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS naics_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                naics_code TEXT UNIQUE,
                naics_description TEXT,
                total_opportunities INTEGER DEFAULT 0,
                avg_contract_value REAL,
                top_agency TEXT,
                small_biz_percentage REAL,
                first_seen TEXT,
                last_activity TEXT
            )
        ''')
        
        # Contract awards (when available)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contract_awards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                solicitation_id INTEGER,
                award_number TEXT,
                awarded_to TEXT,
                award_amount REAL,
                award_date TEXT,
                awardee_duns TEXT,
                awardee_cage_code TEXT,
                awardee_business_type TEXT,
                data_source TEXT,
                collected_date TEXT,
                FOREIGN KEY (solicitation_id) REFERENCES solicitations (id)
            )
        ''')
        
        # Set-aside tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS set_aside_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_aside_type TEXT,
                agency TEXT,
                naics_code TEXT,
                count INTEGER DEFAULT 0,
                avg_value REAL,
                period_start TEXT,
                period_end TEXT,
                last_updated TEXT
            )
        ''')
        
        # Daily collection log
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_date TEXT,
                opportunities_collected INTEGER,
                new_opportunities INTEGER,
                updated_opportunities INTEGER,
                errors_count INTEGER,
                notes TEXT
            )
        ''')
        
        # Create indexes for common queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_posted_date ON solicitations(posted_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_department ON solicitations(department)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_naics ON solicitations(naics_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_set_aside ON solicitations(set_aside)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_response_deadline ON solicitations(response_deadline)')
        
        self.conn.commit()
    
    def insert_solicitation(self, sol_data: dict) -> Optional[int]:
        """
        Insert or update a solicitation.
        
        Args:
            sol_data: Dictionary containing solicitation data
            
        Returns:
            Row ID if successful, None otherwise
        """
        cursor = self.conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO solicitations (
                    notice_id, solicitation_number, title, description,
                    department, sub_tier, office, full_parent_path,
                    naics_code, naics_description, psc_code, set_aside, type_of_notice,
                    posted_date, response_deadline, archive_date,
                    estimated_value_low, estimated_value_high,
                    place_of_performance_city, place_of_performance_state,
                    place_of_performance_zip, place_of_performance_country,
                    primary_contact_name, primary_contact_email, primary_contact_phone,
                    url, data_source, collected_date, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                sol_data.get('notice_id'),
                sol_data.get('solicitation_number'),
                sol_data.get('title'),
                sol_data.get('description'),
                sol_data.get('department'),
                sol_data.get('sub_tier'),
                sol_data.get('office'),
                sol_data.get('full_parent_path'),
                sol_data.get('naics_code'),
                sol_data.get('naics_description'),
                sol_data.get('psc_code'),
                sol_data.get('set_aside'),
                sol_data.get('type_of_notice'),
                sol_data.get('posted_date'),
                sol_data.get('response_deadline'),
                sol_data.get('archive_date'),
                sol_data.get('estimated_value_low'),
                sol_data.get('estimated_value_high'),
                sol_data.get('place_of_performance_city'),
                sol_data.get('place_of_performance_state'),
                sol_data.get('place_of_performance_zip'),
                sol_data.get('place_of_performance_country'),
                sol_data.get('primary_contact_name'),
                sol_data.get('primary_contact_email'),
                sol_data.get('primary_contact_phone'),
                sol_data.get('url'),
                sol_data.get('data_source', 'SAM.gov API'),
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            
            self.conn.commit()
            return cursor.lastrowid
            
        except Exception as e:
            print(f"Error inserting solicitation: {e}")
            self.conn.rollback()
            return None
    
    def get_agency_stats(self, agency_name: str) -> dict:
        """Get statistics for a specific agency."""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total_opps,
                COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) as small_biz_opps,
                AVG(estimated_value_high) as avg_value,
                MIN(posted_date) as first_seen,
                MAX(posted_date) as last_activity
            FROM solicitations
            WHERE department = ?
        ''', (agency_name,))
        
        return dict(cursor.fetchone())
    
    def get_naics_stats(self, naics_code: str) -> dict:
        """Get statistics for a specific NAICS code."""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total_opps,
                AVG(estimated_value_high) as avg_value,
                department as top_agency,
                COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) * 100.0 / COUNT(*) as small_biz_percentage
            FROM solicitations
            WHERE naics_code LIKE ?
            GROUP BY department
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ''', (f'{naics_code}%',))
        
        row = cursor.fetchone()
        return dict(row) if row else {}
    
    def get_trends(self, days: int = 30) -> list:
        """Get trend data for the past N days."""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                DATE(posted_date) as date,
                COUNT(*) as count,
                COUNT(CASE WHEN is_small_business_setaside = 1 THEN 1 END) as small_biz_count
            FROM solicitations
            WHERE posted_date >= date('now', '-' || ? || ' days')
            GROUP BY DATE(posted_date)
            ORDER BY date
        ''', (days,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def close(self):
        """Close database connection."""
        self.conn.close()
