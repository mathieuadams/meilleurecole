import pandas as pd
import psycopg2
from datetime import datetime

def add_scottish_achievement_scores():
    """Add Scottish achievement scores from ACEL data to uk_schools table"""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    CSV_FILE = "ACEL2122allschools.csv"
    
    print("=" * 70)
    print("ADDING SCOTTISH ACHIEVEMENT SCORES (ACEL DATA)")
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
        
        # Process the data - group by school and calculate average scores
        print("\n3. Processing achievement data...")
        
        # Group data by Seed Code and calculate scores
        school_scores = {}
        
        for index, row in df.iterrows():
            if pd.isna(row.get('Seed Code')):
                continue
            
            seed_code = int(row['Seed Code'])
            organiser = str(row.get('Organiser', '')).strip()
            
            # Handle 'z', 'c' and other suppressed values
            def safe_float(value):
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
            
            percentage = safe_float(row.get('Percentage Achieved expected level [note 1]'))
            
            if percentage is None:
                continue
            
            # Initialize school entry if not exists
            if seed_code not in school_scores:
                school_scores[seed_code] = {
                    'school_name': row.get('School Name', ''),
                    'la_name': row.get('LA Name', ''),
                    'english_components': [],
                    'math_scores': [],
                    'stages': set()
                }
            
            # Add stage info
            school_scores[seed_code]['stages'].add(row.get('Stage', ''))
            
            # Categorize scores
            if organiser in ['Listening and Talking', 'Reading', 'Writing']:
                school_scores[seed_code]['english_components'].append(percentage)
            elif organiser == 'Numeracy':
                school_scores[seed_code]['math_scores'].append(percentage)
        
        # Calculate combined scores for each school
        print("\n4. Calculating combined scores and updating database...")
        updated = 0
        skipped = 0
        
        for seed_code, data in school_scores.items():
            try:
                # Calculate English score (average of Listening/Talking, Reading, Writing)
                english_score = None
                if data['english_components']:
                    english_score = sum(data['english_components']) / len(data['english_components'])
                
                # Calculate Math score (average of all Numeracy scores)
                math_score = None
                if data['math_scores']:
                    math_score = sum(data['math_scores']) / len(data['math_scores'])
                
                # Skip if no scores to update
                if english_score is None and math_score is None:
                    skipped += 1
                    continue
                
                # Build update query
                update_fields = []
                update_values = []
                
                if english_score is not None:
                    update_fields.append("english_score = %s")
                    update_values.append(english_score)
                    update_fields.append("english_avg = %s")
                    update_values.append(english_score)  # Using same value for avg
                
                if math_score is not None:
                    update_fields.append("math_score = %s")
                    update_values.append(math_score)
                    update_fields.append("math_avg = %s")
                    update_values.append(math_score)  # Using same value for avg
                
                # Note: Science score not available in ACEL data
                # We'll leave science_score and science_avg as NULL
                
                # Add WHERE clause values
                update_values.extend([seed_code, seed_code])
                
                # Update the school record
                query = f"""
                    UPDATE uk_schools 
                    SET {', '.join(update_fields)},
                        updated_at = CURRENT_TIMESTAMP
                    WHERE (urn = %s OR seed = %s)
                    AND country = 'Scotland'
                """
                
                cursor.execute(query, update_values)
                
                if cursor.rowcount > 0:
                    updated += 1
                    if updated % 50 == 0:
                        print(f"   Updated {updated} schools...")
                        conn.commit()
                
            except Exception as e:
                print(f"   Error processing seed {seed_code}: {e}")
                conn.rollback()
                skipped += 1
                continue
        
        # Final commit
        conn.commit()
        
        # Verify results
        print("\n5. Verifying score data...")
        
        # Get statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(english_score) as with_english,
                COUNT(math_score) as with_math,
                AVG(english_score) as avg_english,
                AVG(math_score) as avg_math,
                MIN(english_score) as min_english,
                MAX(english_score) as max_english,
                MIN(math_score) as min_math,
                MAX(math_score) as max_math
            FROM uk_schools 
            WHERE country = 'Scotland'
        """)
        stats = cursor.fetchone()
        
        # Show sample
        cursor.execute("""
            SELECT 
                seed, name, 
                ROUND(english_score::numeric, 2) as english,
                ROUND(math_score::numeric, 2) as math
            FROM uk_schools 
            WHERE country = 'Scotland'
            AND english_score IS NOT NULL
            ORDER BY seed
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        print("\n" + "=" * 70)
        print("ACHIEVEMENT SCORE IMPORT SUMMARY:")
        print(f"   Schools updated: {updated}")
        print(f"   Schools skipped: {skipped}")
        print(f"   Total schools processed: {len(school_scores)}")
        
        if stats:
            print(f"\nSCOTTISH ACHIEVEMENT STATISTICS:")
            print(f"   Total Scottish schools: {stats['total']}")
            print(f"   Schools with English scores: {stats['with_english']}")
            print(f"   Schools with Math scores: {stats['with_math']}")
            if stats['avg_english']:
                print(f"\n   English scores:")
                print(f"      Average: {stats['avg_english']:.2f}%")
                print(f"      Range: {stats['min_english']:.2f}% - {stats['max_english']:.2f}%")
            if stats['avg_math']:
                print(f"   Math scores:")
                print(f"      Average: {stats['avg_math']:.2f}%")
                print(f"      Range: {stats['min_math']:.2f}% - {stats['max_math']:.2f}%")
        
        if samples:
            print("\n📊 SAMPLE ACHIEVEMENT SCORES:")
            print("-" * 70)
            print(f"{'Seed':<8} {'School Name':<35} {'English':<10} {'Math':<10}")
            print("-" * 70)
            for seed, name, english, math in samples:
                eng_str = f"{english}%" if english else "N/A"
                math_str = f"{math}%" if math else "N/A"
                print(f"{seed:<8} {name[:35]:<35} {eng_str:<10} {math_str:<10}")
        
        print("\n✅ SUCCESS! Achievement scores have been added for Scottish schools")
        print("\nNote: Science scores are not available in ACEL data")
        print("English score = Average of (Listening/Talking, Reading, Writing)")
        print("Math score = Numeracy percentage")
        
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
    add_scottish_achievement_scores()