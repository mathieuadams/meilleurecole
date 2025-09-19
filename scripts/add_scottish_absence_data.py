import pandas as pd
import psycopg2
from datetime import datetime

def add_scottish_absence_data():
    """Add Scottish schools absence data to uk_absence_data table"""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    CSV_FILE = "Schoollevelsummarystatistics2024.csv"  # This file has attendance data
    
    print("=" * 70)
    print("ADDING SCOTTISH SCHOOLS ABSENCE DATA")
    print("=" * 70)
    
    try:
        # Connect to database
        print("\n1. Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        cursor = conn.cursor()
        conn.rollback()  # Clear any pending transactions
        print("✅ Database connected")
        
        # Read CSV
        print(f"\n2. Reading {CSV_FILE}...")
        df = pd.read_csv(CSV_FILE)
        print(f"✅ Found {len(df)} rows in CSV")
        
        # Check columns related to absence
        absence_columns = [col for col in df.columns if 'absence' in col.lower() or 'attendance' in col.lower()]
        print(f"\nAbsence-related columns found: {absence_columns}")
        
        # Process and insert absence data
        print("\n3. Processing absence data for Scottish schools...")
        inserted = 0
        updated = 0
        skipped = 0
        
        def safe_float(value):
            """Convert to float, handling 'z', 'c' and other non-numeric values"""
            if pd.isna(value):
                return None
            if isinstance(value, str):
                value_str = value.strip().lower()
                if value_str in ['z', 'c', 'x', '*', '-', '', 'n/a', 'na']:
                    return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        for index, row in df.iterrows():
            try:
                if pd.isna(row.get('SeedCode')):
                    skipped += 1
                    continue
                
                seed_code = int(row['SeedCode'])
                
                # Get attendance and absence rates
                # Scottish data typically has: Attendance rate, Authorised absence rate, Unauthorised absence rate
                attendance_rate = safe_float(row.get('Attendance rate (%)[Note 8] [Note 9]'))
                auth_absence = safe_float(row.get('Authorised absence rate (%) [Note 8] [Note 9]'))
                unauth_absence = safe_float(row.get('Unauthorised absence rate (%)[Note 8] [Note 9]'))
                
                # Calculate overall absence rate from attendance rate
                overall_absence = None
                if attendance_rate is not None:
                    overall_absence = 100.0 - attendance_rate
                elif auth_absence is not None and unauth_absence is not None:
                    overall_absence = auth_absence + unauth_absence
                
                # Skip if no absence data
                if overall_absence is None:
                    continue
                
                # Check if absence data already exists for this school
                cursor.execute("""
                    SELECT id FROM uk_absence_data 
                    WHERE urn = %s AND academic_year = '2023/2024'
                """, (seed_code,))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing record
                    cursor.execute("""
                        UPDATE uk_absence_data 
                        SET overall_absence_rate = %s,
                            persistent_absence_rate = NULL,  -- We don't have this in Scottish data
                            la_code = NULL,
                            estab_number = %s
                        WHERE urn = %s AND academic_year = '2023/2024'
                    """, (overall_absence, seed_code, seed_code))
                    updated += 1
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO uk_absence_data (
                            urn, la_code, estab_number,
                            overall_absence_rate, persistent_absence_rate,
                            academic_year
                        ) VALUES (
                            %s, NULL, %s,
                            %s, NULL,
                            '2023/2024'
                        )
                    """, (seed_code, seed_code, overall_absence))
                    inserted += 1
                
                # Show progress
                if (inserted + updated) % 100 == 0 and (inserted + updated) > 0:
                    print(f"   Processed {inserted + updated} schools...")
                    conn.commit()
                    
            except Exception as e:
                print(f"   Error at row {index}: {e}")
                conn.rollback()
                skipped += 1
                continue
        
        # Final commit
        conn.commit()
        
        # Verify results
        print("\n4. Verifying absence data...")
        
        # Get statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                AVG(overall_absence_rate) as avg_absence_rate,
                MIN(overall_absence_rate) as min_absence_rate,
                MAX(overall_absence_rate) as max_absence_rate
            FROM uk_absence_data a
            INNER JOIN uk_schools s ON a.urn = s.urn
            WHERE s.country = 'Scotland'
            AND a.academic_year = '2023/2024'
        """)
        stats = cursor.fetchone()
        
        # Show sample
        cursor.execute("""
            SELECT 
                a.urn,
                s.name,
                a.overall_absence_rate
            FROM uk_absence_data a
            INNER JOIN uk_schools s ON a.urn = s.urn
            WHERE s.country = 'Scotland'
            AND a.academic_year = '2023/2024'
            ORDER BY a.urn
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        print("\n" + "=" * 70)
        print("ABSENCE DATA IMPORT SUMMARY:")
        print(f"   New records inserted: {inserted}")
        print(f"   Existing records updated: {updated}")
        print(f"   Rows skipped: {skipped}")
        
        if stats[0] > 0:
            print(f"\nSCOTTISH ABSENCE STATISTICS:")
            print(f"   Total schools with absence data: {stats[0]}")
            print(f"   Average absence rate: {stats[1]:.2f}%")
            print(f"   Minimum absence rate: {stats[2]:.2f}%")
            print(f"   Maximum absence rate: {stats[3]:.2f}%")
        
        if samples:
            print("\n📊 SAMPLE ABSENCE DATA:")
            print("-" * 60)
            print(f"{'URN':<8} {'School Name':<35} {'Absence %':<10}")
            print("-" * 60)
            for urn, name, absence_rate in samples:
                print(f"{urn:<8} {name[:35]:<35} {absence_rate:>8.2f}%")
        
        print("\n✅ SUCCESS! Absence data has been added for Scottish schools")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("\n✅ Database connection closed")
        print("=" * 70)


if __name__ == "__main__":
    add_scottish_absence_data()