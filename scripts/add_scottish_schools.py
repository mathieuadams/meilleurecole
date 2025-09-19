import pandas as pd
import psycopg2
from datetime import datetime

def add_scottish_schools():
    """Add Scottish schools from CSV to uk_schools table with comprehensive field mapping"""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    CSV_FILE = "Schoollevelsummarystatistics2024.csv"
    
    print("=" * 70)
    print("ADDING SCOTTISH SCHOOLS TO UK_SCHOOLS TABLE")
    print("=" * 70)
    
    try:
        # Connect to database
        print("\n1. Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Database connected")
        
        # Check current school count
        cursor.execute("SELECT COUNT(*) FROM uk_schools WHERE country = 'Scotland'")
        existing_scottish = cursor.fetchone()[0]
        print(f"   Existing Scottish schools: {existing_scottish}")
        
        # Read CSV
        print(f"\n2. Reading {CSV_FILE}...")
        df = pd.read_csv(CSV_FILE)
        print(f"✅ Found {len(df)} rows in CSV")
        
        # Process and insert data
        print("\n3. Processing Scottish schools with full field mapping...")
        inserted = 0
        updated = 0
        skipped = 0
        errors = 0
        
        for index, row in df.iterrows():
            try:
                # Check required fields
                if pd.isna(row.get('SeedCode')) or pd.isna(row.get('School Name')):
                    skipped += 1
                    continue
                
                # Core fields
                seed_code = int(row['SeedCode'])
                school_name = str(row['School Name']).strip()
                urn = seed_code  # Use seed directly as URN since no overlap
                
                # Create name variations
                name_lower = school_name.lower()
                slug = name_lower.replace(' ', '-').replace('/', '-').replace('&', 'and')
                slug = ''.join(c for c in slug if c.isalnum() or c == '-')
                slug = f"scotland-{slug}-{seed_code}"
                
                # Map fields from CSV
                local_authority = str(row['Local Authority']).strip() if pd.notna(row.get('Local Authority')) else None
                school_type = str(row['School Type']).strip() if pd.notna(row.get('School Type')) else None
                
                # Map school type to phase of education
                phase_mapping = {
                    'Primary': 'Primary',
                    'Secondary': 'Secondary',
                    'Special': 'Special',
                    'Primary/Secondary': 'All-through'
                }
                phase = phase_mapping.get(school_type, school_type)
                
                # Pupil counts - handle 'z', 'c' and other non-numeric values
                def safe_int(value):
                    """Convert to int, return None if not possible"""
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
                
                total_pupils = safe_int(row['Pupil roll'])
                boys_count = safe_int(row['Male'])
                girls_count = safe_int(row['Female'])
                
                # FSM percentage (P1-P5) - handle 'z', 'c' and other non-numeric values
                def safe_float(value):
                    """Convert to float, return None if not possible"""
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
                
                percentage_fsm = safe_float(row['Percentage of P1-P5 pupils registered for free school meals [Note 5]'])
                
                # Religious character
                denomination = str(row['Denomination of school']).strip() if pd.notna(row.get('Denomination of school')) else None
                religious_character = None if denomination == 'Non-denominational' else denomination
                
                # Urban/rural classification
                urban_rural = str(row["School's 6-fold urban/rural classification"]).strip() if pd.notna(row.get("School's 6-fold urban/rural classification")) else None
                
                # Determine if has sixth form (S5 or S6 > 0)
                has_sixth_form = False
                s5_count = safe_int(row.get('S5'))
                s6_count = safe_int(row.get('S6'))
                if (s5_count and s5_count > 0) or (s6_count and s6_count > 0):
                    has_sixth_form = True
                
                # Determine if has SEN provision
                has_sen_provision = False
                special_pupils = safe_int(row.get('Special school pupils'))
                if special_pupils and special_pupils > 0:
                    has_sen_provision = True
                
                # Set age ranges based on school type
                age_ranges = {
                    'Primary': (4, 11),
                    'Secondary': (11, 18),
                    'Special': (4, 18),
                    'Primary/Secondary': (4, 18)
                }
                age_range_lower, age_range_upper = age_ranges.get(school_type, (None, None))
                
                # Check if school already exists
                cursor.execute("""
                    SELECT id FROM uk_schools 
                    WHERE urn = %s OR seed = %s
                """, (urn, seed_code))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing record
                    cursor.execute("""
                        UPDATE uk_schools 
                        SET name = %s,
                            name_lower = %s,
                            slug = %s,
                            seed = %s,
                            country = 'Scotland',
                            local_authority = %s,
                            type_of_establishment = %s,
                            phase_of_education = %s,
                            total_pupils = %s,
                            boys_count = %s,
                            girls_count = %s,
                            percentage_fsm = %s,
                            religious_character = %s,
                            urban_rural = %s,
                            has_sixth_form = %s,
                            has_sen_provision = %s,
                            age_range_lower = %s,
                            age_range_upper = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE urn = %s OR seed = %s
                    """, (school_name, name_lower, slug, seed_code, local_authority,
                          school_type, phase, total_pupils, boys_count, girls_count,
                          percentage_fsm, religious_character, urban_rural,
                          has_sixth_form, has_sen_provision, age_range_lower, age_range_upper,
                          urn, seed_code))
                    updated += 1
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO uk_schools (
                            urn, seed, name, name_lower, slug, country,
                            local_authority, type_of_establishment, phase_of_education,
                            establishment_status, total_pupils, boys_count, girls_count,
                            percentage_fsm, religious_character, urban_rural,
                            has_sixth_form, has_sen_provision,
                            age_range_lower, age_range_upper
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s
                        )
                        ON CONFLICT (urn) DO UPDATE SET
                            seed = EXCLUDED.seed,
                            name = EXCLUDED.name,
                            name_lower = EXCLUDED.name_lower,
                            country = EXCLUDED.country,
                            updated_at = CURRENT_TIMESTAMP
                    """, (urn, seed_code, school_name, name_lower, slug, 'Scotland',
                          local_authority, school_type, phase,
                          'Open', total_pupils, boys_count, girls_count,
                          percentage_fsm, religious_character, urban_rural,
                          has_sixth_form, has_sen_provision,
                          age_range_lower, age_range_upper))
                    inserted += 1
                
                # Show progress
                if (inserted + updated) % 100 == 0:
                    print(f"   Processed {inserted + updated} schools...")
                    conn.commit()  # Commit in batches
                    
            except Exception as e:
                errors += 1
                print(f"   Error at row {index}: {e}")
                continue
        
        # Final commit
        conn.commit()
        
        # Verify results
        print("\n4. Verifying import...")
        cursor.execute("SELECT COUNT(*) FROM uk_schools WHERE country = 'Scotland'")
        scottish_after = cursor.fetchone()[0]
        
        # Show sample of imported schools
        cursor.execute("""
            SELECT urn, seed, name, type_of_establishment, total_pupils, percentage_fsm
            FROM uk_schools 
            WHERE country = 'Scotland'
            ORDER BY seed
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        print("\n" + "=" * 70)
        print("IMPORT SUMMARY:")
        print(f"   New schools inserted: {inserted}")
        print(f"   Existing schools updated: {updated}")
        print(f"   Rows skipped (missing data): {skipped}")
        print(f"   Errors: {errors}")
        print(f"\n   Total Scottish schools now: {scottish_after}")
        
        if samples:
            print("\n📚 SAMPLE SCOTTISH SCHOOLS:")
            print("-" * 80)
            print(f"{'URN':<8} {'Seed':<8} {'Name':<35} {'Type':<12} {'Pupils':<8} {'FSM%':<6}")
            print("-" * 80)
            for urn, seed, name, school_type, pupils, fsm in samples:
                fsm_str = f"{fsm:.1f}" if fsm else "N/A"
                pupils_str = str(pupils) if pupils else "N/A"
                print(f"{urn:<8} {seed:<8} {name[:35]:<35} {(school_type or 'N/A')[:12]:<12} {pupils_str:<8} {fsm_str:<6}")
        
        print("\n✅ SUCCESS! Scottish schools have been added to uk_schools table")
        
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
    add_scottish_schools()