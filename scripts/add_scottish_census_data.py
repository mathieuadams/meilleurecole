import pandas as pd
import psycopg2
from datetime import datetime

def add_scottish_census_data():
    """Add Scottish schools census data from Schoollevelsummarystatistics2024.csv"""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    CSV_FILE = "Schoollevelsummarystatistics2024.csv"
    
    print("=" * 70)
    print("ADDING SCOTTISH CENSUS DATA TO UK_CENSUS_DATA TABLE")
    print("=" * 70)
    
    def safe_int(value):
        """Convert to int, handling 'z', 'c' and other non-numeric values"""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            value_str = value.strip().lower()
            if value_str in ['z', 'c', 'x', '*', '-', '', 'n/a', 'na']:
                return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def safe_float(value):
        """Convert to float percentage"""
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
    
    try:
        # Connect to database
        print("\n1. Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        cursor = conn.cursor()
        conn.rollback()
        print("✅ Database connected")
        
        # Read CSV
        print(f"\n2. Reading {CSV_FILE}...")
        df = pd.read_csv(CSV_FILE)
        print(f"✅ Found {len(df)} rows in CSV")
        
        # Process census data
        print("\n3. Processing census data for Scottish schools...")
        inserted = 0
        updated = 0
        skipped = 0
        
        for index, row in df.iterrows():
            try:
                if pd.isna(row.get('SeedCode')):
                    skipped += 1
                    continue
                
                seed_code = int(row['SeedCode'])
                
                # Extract census data
                number_on_roll = safe_int(row.get('Pupil roll'))
                number_girls = safe_int(row.get('Female'))
                number_boys = safe_int(row.get('Male'))
                
                # Calculate percentages
                percentage_girls = None
                percentage_boys = None
                if number_on_roll and number_on_roll > 0:
                    if number_girls is not None:
                        percentage_girls = (number_girls / number_on_roll) * 100
                    if number_boys is not None:
                        percentage_boys = (number_boys / number_on_roll) * 100
                
                # SEN support (Additional Support Needs in Scotland)
                total_sen_support = safe_int(row.get('Pupils with an Additional Support Need recorded'))
                percentage_sen_support = None
                if number_on_roll and number_on_roll > 0 and total_sen_support is not None:
                    percentage_sen_support = (total_sen_support / number_on_roll) * 100
                
                # Special school pupils (could be EHCP equivalent)
                total_sen_ehcp = safe_int(row.get('Special school pupils'))
                percentage_sen_ehcp = None
                if number_on_roll and number_on_roll > 0 and total_sen_ehcp is not None:
                    percentage_sen_ehcp = (total_sen_ehcp / number_on_roll) * 100
                
                # EAL data
                number_eal = safe_int(row.get('Pupils with English as an Additional Language [Note 2]'))
                number_english_first = safe_int(row.get('Pupils without English as an Additional Language [Note 2]'))
                
                percentage_eal = None
                percentage_english_first = None
                if number_on_roll and number_on_roll > 0:
                    if number_eal is not None:
                        percentage_eal = (number_eal / number_on_roll) * 100
                    if number_english_first is not None:
                        percentage_english_first = (number_english_first / number_on_roll) * 100
                
                # FSM data - combine P1-P5 and P6-P7/S1-S6
                fsm_p1p5 = safe_int(row.get('P1-P5 pupils registered for free school meals [Note 5]'))
                fsm_other = safe_int(row.get('P6-P7/S1-S6/SP pupils registered for free school meals [Note 5]'))
                
                number_fsm = None
                if fsm_p1p5 is not None and fsm_other is not None:
                    number_fsm = fsm_p1p5 + fsm_other
                elif fsm_p1p5 is not None:
                    number_fsm = fsm_p1p5
                elif fsm_other is not None:
                    number_fsm = fsm_other
                
                # FSM Ever6 percentage (use average of P1-P5 and P6-P7/S1-S6 percentages)
                fsm_p1p5_pct = safe_float(row.get('Percentage of P1-P5 pupils registered for free school meals [Note 5]'))
                fsm_other_pct = safe_float(row.get('Percentage of P6-P7/S1-S6/SP pupils registered for free school meals [Note 5]'))
                
                percentage_fsm_ever6 = None
                if fsm_p1p5_pct is not None and fsm_other_pct is not None:
                    percentage_fsm_ever6 = (fsm_p1p5_pct + fsm_other_pct) / 2
                elif fsm_p1p5_pct is not None:
                    percentage_fsm_ever6 = fsm_p1p5_pct
                elif fsm_other_pct is not None:
                    percentage_fsm_ever6 = fsm_other_pct
                
                # School type
                school_type = str(row['School Type']).strip() if pd.notna(row.get('School Type')) else None
                
                # Check if census data already exists
                cursor.execute("""
                    SELECT id FROM uk_census_data 
                    WHERE urn = %s AND academic_year = '2023/2024'
                """, (seed_code,))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing record
                    cursor.execute("""
                        UPDATE uk_census_data 
                        SET la_code = NULL,
                            estab_number = %s,
                            school_type = %s,
                            number_on_roll = %s,
                            number_girls = %s,
                            number_boys = %s,
                            percentage_girls = %s,
                            percentage_boys = %s,
                            total_sen_support = %s,
                            percentage_sen_support = %s,
                            total_sen_ehcp = %s,
                            percentage_sen_ehcp = %s,
                            number_eal = %s,
                            number_english_first_language = %s,
                            percentage_eal = %s,
                            percentage_english_first_language = %s,
                            number_fsm = %s,
                            number_fsm_ever6 = %s,
                            total_fsm_ever6 = %s,
                            percentage_fsm_ever6 = %s
                        WHERE urn = %s AND academic_year = '2023/2024'
                    """, (seed_code, school_type, number_on_roll, number_girls, number_boys,
                          percentage_girls, percentage_boys, total_sen_support, percentage_sen_support,
                          total_sen_ehcp, percentage_sen_ehcp, number_eal, number_english_first,
                          percentage_eal, percentage_english_first, number_fsm, number_fsm, number_fsm,
                          percentage_fsm_ever6, seed_code))
                    updated += 1
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO uk_census_data (
                            urn, la_code, estab_number, school_type,
                            number_on_roll, number_girls, number_boys,
                            percentage_girls, percentage_boys,
                            total_sen_support, percentage_sen_support,
                            total_sen_ehcp, percentage_sen_ehcp,
                            number_eal, number_english_first_language, number_unclassified_language,
                            percentage_eal, percentage_english_first_language, percentage_unclassified_language,
                            number_fsm, number_fsm_ever6, total_fsm_ever6, percentage_fsm_ever6,
                            academic_year
                        ) VALUES (
                            %s, NULL, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s,
                            %s, %s,
                            %s, %s, NULL,
                            %s, %s, NULL,
                            %s, %s, %s, %s,
                            '2023/2024'
                        )
                    """, (seed_code, seed_code, school_type,
                          number_on_roll, number_girls, number_boys,
                          percentage_girls, percentage_boys,
                          total_sen_support, percentage_sen_support,
                          total_sen_ehcp, percentage_sen_ehcp,
                          number_eal, number_english_first,
                          percentage_eal, percentage_english_first,
                          number_fsm, number_fsm, number_fsm, percentage_fsm_ever6))
                    inserted += 1
                
                # Commit every 100 records
                if (inserted + updated) % 100 == 0:
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
        print("\n4. Verifying census data...")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                AVG(number_on_roll) as avg_roll,
                AVG(percentage_fsm_ever6) as avg_fsm,
                AVG(percentage_eal) as avg_eal,
                AVG(percentage_sen_support) as avg_sen
            FROM uk_census_data c
            INNER JOIN uk_schools s ON c.urn = s.urn
            WHERE s.country = 'Scotland'
            AND c.academic_year = '2023/2024'
        """)
        stats = cursor.fetchone()
        
        print("\n" + "=" * 70)
        print("CENSUS DATA IMPORT SUMMARY:")
        print(f"   New records inserted: {inserted}")
        print(f"   Existing records updated: {updated}")
        print(f"   Rows skipped: {skipped}")
        
        if stats and stats['total_records'] > 0:
            print(f"\nSCOTTISH CENSUS STATISTICS:")
            print(f"   Total schools with census data: {stats['total_records']}")
            if stats['avg_roll']:
                print(f"   Average school roll: {stats['avg_roll']:.0f}")
            if stats['avg_fsm']:
                print(f"   Average FSM %: {stats['avg_fsm']:.1f}%")
            if stats['avg_eal']:
                print(f"   Average EAL %: {stats['avg_eal']:.1f}%")
            if stats['avg_sen']:
                print(f"   Average SEN support %: {stats['avg_sen']:.1f}%")
        
        print("\n✅ SUCCESS! Census data has been added for Scottish schools")
        
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
    add_scottish_census_data()