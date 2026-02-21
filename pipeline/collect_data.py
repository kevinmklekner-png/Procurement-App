"""
Automated daily data collection for federal procurement analytics.
Run this daily to build historical database.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config
from sam_api import SAMApiClient
from database import ProcurementDatabase
from datetime import datetime, timedelta
import time


def collect_all_opportunities(days_back: int = 7):
    """
    Collect all solicitations from the past N days.
    
    Args:
        days_back: Number of days to look back
    """
    print("=" * 80)
    print(f"Federal Procurement Data Collection - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)
    print()
    
    # Initialize
    config.validate_config()
    api_client = SAMApiClient()
    db = ProcurementDatabase()
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    print(f"Collecting opportunities from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print()
    
    # Track statistics
    total_collected = 0
    new_records = 0
    updated_records = 0
    errors = 0
    
    # Notice types to collect
    notice_types = [
        'Solicitation',
        'Combined Synopsis/Solicitation',
        'Presolicitation',
        'Sources Sought',
        'Special Notice'
    ]
    
    for notice_type in notice_types:
        print(f"\nCollecting {notice_type}...")
        
        try:
            # Use pagination to get all results
            response = api_client.get_opportunities_paginated(
                max_results=5000,
                page_size=100,
                posted_from=start_date.strftime('%Y-%m-%d'),
                posted_to=end_date.strftime('%Y-%m-%d'),
                notice_type=notice_type
            )
            
            print(f"  Found {len(response)} {notice_type} opportunities")
            
            # Store each in database
            for sol in response:
                sol_data = {
                    'notice_id': sol.notice_id,
                    'solicitation_number': sol.solicitation_number,
                    'title': sol.title,
                    'description': sol.description,
                    'department': sol.department,
                    'sub_tier': sol.sub_tier,
                    'office': sol.office,
                    'naics_code': sol.naics_code,
                    'naics_description': sol.naics_description,
                    'set_aside': sol.set_aside,
                    'type_of_notice': notice_type,
                    'posted_date': sol.posted_date.isoformat() if sol.posted_date else None,
                    'response_deadline': sol.response_deadline.isoformat() if sol.response_deadline else None,
                    'place_of_performance_city': sol.place_of_performance,
                    'primary_contact_name': sol.primary_contact,
                    'primary_contact_email': sol.primary_contact_email,
                    'url': sol.url,
                }
                
                row_id = db.insert_solicitation(sol_data)
                if row_id:
                    new_records += 1
                else:
                    updated_records += 1
                
                total_collected += 1
            
            # Be nice to the API
            time.sleep(1)
            
        except Exception as e:
            print(f"  Error collecting {notice_type}: {e}")
            errors += 1
    
    # Log the collection
    cursor = db.conn.cursor()
    cursor.execute('''
        INSERT INTO collection_log (
            collection_date, opportunities_collected, 
            new_opportunities, updated_opportunities, errors_count
        ) VALUES (?, ?, ?, ?, ?)
    ''', (
        datetime.now().isoformat(),
        total_collected,
        new_records,
        updated_records,
        errors
    ))
    db.conn.commit()
    
    # Summary
    print()
    print("=" * 80)
    print("COLLECTION SUMMARY")
    print("=" * 80)
    print(f"Total opportunities collected: {total_collected}")
    print(f"New records: {new_records}")
    print(f"Updated records: {updated_records}")
    print(f"Errors: {errors}")
    print()
    
    # Quick stats
    cursor.execute('SELECT COUNT(*) as total FROM solicitations')
    total_in_db = cursor.fetchone()[0]
    print(f"Total opportunities in database: {total_in_db}")
    
    db.close()
    
    print()
    print("Collection complete!")
    print("=" * 80)


def daily_collection():
    """Run daily collection (last 2 days to catch updates)."""
    collect_all_opportunities(days_back=2)


def initial_backfill(days: int = 90):
    """
    Initial backfill to build historical database.
    
    Args:
        days: How many days of history to collect
    """
    print(f"Starting initial backfill for {days} days of data...")
    print("This will take a while - be patient!")
    print()
    
    # Collect in chunks to avoid overwhelming the API
    chunk_size = 30
    for i in range(0, days, chunk_size):
        chunk_days = min(chunk_size, days - i)
        print(f"\nCollecting days {i+1} to {i+chunk_days}...")
        collect_all_opportunities(days_back=chunk_days)
        
        # Longer pause between chunks
        if i + chunk_size < days:
            print("\nPausing 30 seconds before next chunk...")
            time.sleep(30)
    
    print()
    print("Initial backfill complete!")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'backfill':
            # Initial backfill
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
            initial_backfill(days)
        elif sys.argv[1] == 'daily':
            # Daily collection
            daily_collection()
        else:
            print("Usage:")
            print("  python collect_data.py daily        # Collect last 2 days")
            print("  python collect_data.py backfill     # Initial 90-day backfill")
            print("  python collect_data.py backfill 180 # Custom day backfill")
    else:
        # Default: daily collection
        daily_collection()
