import pandas as pd
import psycopg2
import re
from datetime import datetime

def parse_acel_percentages():
    """Parse ACEL data with percentage ranges and update Scottish school scores"""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    CSV_FILE = "ACEL2122allschools.csv"
    
    print("=" * 70)
    print("PARSING ACEL PERCENTAGE DATA FOR SCOTTISH SCHOOLS")
    print("=" * 70)
    
    def extract_percentage(value):
        """Extract numeric percentage from strings like '98-100%' or '67.53'"""
        if pd.isna(value):
            return None
        
        value_str = str(value).strip()
        
        # Check for suppressed values
        if value_str.lower() in ['z', 'c', 'x', '*', '-', '', 'n/a', 'na']:
            return None
        
        # Handle percentage ranges like "98-100%"
        if '-' in value_str and '%' in value_str:
            # Extract the first number (lower bound of range)
            match = re.match(r'(\d+(?:\.\d+)?)', value_str)
            if match:
                return float(match.group(1))
        
        # Handle simple percentages with or without %
        value_str = value_str.replace('%', '').strip()
        try:
            return float(value_str)
        except ValueError:
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
        
        # Sample the percentage format
        print("\n3. Checking percentage format...")
        sample_values = df['Percentage Achieved expected level [note 1]'].dropna().head(10)
        print("Sample percentage values:")
        for val in sample_values:
            parsed = extract_percentage(val)
            print(f"   '{val}' → {parsed}")
        
        # Process data by school
        print("\n4. Processing scores by school...")
        school_scores = {}
        
        for index, row in df.iterrows():
            if pd.isna(row.get('Seed Code')):
                continue
            
            try:
                seed_code = int(row['Seed Code'])
            except (ValueError, TypeError):
                continue
            
            organiser = str(row.get('Organiser', '')).strip()
            percentage = extract_percentage(row.get('Percentage Achieved expected level [note 1]'))
            
            if percentage is None:
                continue
            
            # Initialize school if not exists
            if seed_code not in school_scores:
                school_scores[seed_code] = {
                    'school_name': row.get('School Name', ''),
                    'la_name': row.get('LA Name', ''),
                    'listening_talking': [],
                    'reading': [],
                    'writing': [],
                    'numeracy': []
                }
            
            # Categorize scores
            if organiser == 'Listening and Talking':
                school_scores[seed_code]['listening_talking'].append(percentage)
            elif organiser == 'Reading':
                school_scores[seed_code]['reading'].append(percentage)
            elif organiser == 'Writing':
                school_scores[seed_code]['writing'].append(percentage)
            elif organiser == 'Numeracy':
                school_scores[seed_code]['numeracy'].append(percentage)
        
        # Calculate combined scores
        print("\n5. Calculating combined scores...")
        updated = 0
        skipped = 0
        
        for seed_code, data in school_scores.items():
            try:
                # Calculate English score (average of all three components)
                english_components = []
                if data['listening_talking']:
                    english_components.append(sum(data['listening_talking']) / len(data['listening_talking']))
                if data['reading']:
                    english_components.append(sum(data['reading']) / len(data['reading']))
                if data['writing']:
                    english_components.append(sum(data['writing']) / len(data['writing']))
                
                english_score = None
                if english_components:
                    english_score = sum(english_components) / len(english_components)
                
                # Calculate Math score
                math_score = None
                if data['numeracy']:
                    math_score = sum(data['numeracy']) / len(data['numeracy'])
                
                # Skip if no scores
                if english_score is None and math_score is None:
                    skipped += 1
                    continue
                
                # Update database
                update_parts = []
                values = []
                
                if english_score is not None:
                    update_parts.append("english_score = %s")
                    values.append(english_score)
                
                if math_score is not None:
                    update_parts.append("math_score = %s")
                    values.append(math_score)
                
                values.extend([seed_code, seed_code])
                
                query = f"""
                    UPDATE uk_schools 
                    SET {', '.join(update_parts)},
                        updated_at = CURRENT_TIMESTAMP
                    WHERE (urn = %s OR seed = %s)
                    AND country = 'Scotland'
                """
                
                cursor.execute(query, values)
                
                if cursor.rowcount > 0:
                    updated += 1
                    if updated % 50 == 0:
                        print(f"   Updated {updated} schools...")
                        conn.commit()
                
            except Exception as e:
                print(f"   Error for seed {seed_code}: {e}")
                conn.rollback()
                skipped += 1
        
        # Commit final batch
        conn.commit()
        
        # Calculate and update Scotland-wide averages
        print("\n6. Calculating Scotland-wide averages...")
        cursor.execute("""
            UPDATE uk_schools 
            SET english_avg = (
                    SELECT AVG(english_score) 
                    FROM uk_schools 
                    WHERE country = 'Scotland' 
                    AND english_score IS NOT NULL
                ),
                math_avg = (
                    SELECT AVG(math_score) 
                    FROM uk_schools 
                    WHERE country = 'Scotland'
                    AND math_score IS NOT NULL
                )
            WHERE country = 'Scotland'
        """)
        conn.commit()
        
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(english_score) as with_english,
                COUNT(math_score) as with_math,
                AVG(english_score) as avg_english,
                AVG(math_score) as avg_math
            FROM uk_schools 
            WHERE country = 'Scotland'
        """)
        stats = cursor.fetchone()
        
        print("\n" + "=" * 70)
        print("IMPORT SUMMARY:")
        print(f"   Schools updated: {updated}")
        print(f"   Schools skipped: {skipped}")
        print(f"   Schools processed: {len(school_scores)}")
        
        if stats:
            print(f"\nFINAL STATISTICS:")
            print(f"   Total Scottish schools: {stats['total']}")
            print(f"   With English scores: {stats['with_english']}")
            print(f"   With Math scores: {stats['with_math']}")
            if stats['avg_english']:
                print(f"   Average English: {stats['avg_english']:.2f}%")
            if stats['avg_math']:
                print(f"   Average Math: {stats['avg_math']:.2f}%")
        
        print("\n✅ SUCCESS! Scores have been updated with proper percentage parsing")
        
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
    parse_acel_percentages()