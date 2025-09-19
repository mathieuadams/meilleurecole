import pandas as pd
import psycopg2

def insert_scottish_schools():
    """Insert Scottish schools into the temporary table"""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    
    print("=" * 70)
    print("INSERTING SCOTTISH SCHOOLS INTO TEMP TABLE")
    print("=" * 70)
    
    try:
        # Connect to database
        print("\n1. Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Database connected successfully")
        
        # Read CSV
        print("\n2. Reading CSV file...")
        df = pd.read_csv('Schoollevelsummarystatistics2024.csv')
        print(f"✅ Found {len(df)} rows in CSV")
        
        # Clear existing data
        print("\n3. Clearing existing data in scottish_schools_temp...")
        cursor.execute("DELETE FROM scottish_schools_temp")
        conn.commit()
        print("✅ Table cleared")
        
        # Prepare and insert data
        print("\n4. Inserting data...")
        inserted = 0
        skipped = 0
        
        for index, row in df.iterrows():
            try:
                if pd.notna(row['SeedCode']) and pd.notna(row['School Name']):
                    seed = int(row['SeedCode'])
                    name = str(row['School Name']).strip()
                    
                    cursor.execute("""
                        INSERT INTO scottish_schools_temp (seed, name)
                        VALUES (%s, %s)
                        ON CONFLICT (seed) DO NOTHING
                    """, (seed, name))
                    
                    inserted += 1
                    
                    # Show progress every 100 records
                    if inserted % 100 == 0:
                        print(f"   Inserted {inserted} records...")
                        conn.commit()  # Commit in batches
                else:
                    skipped += 1
                    
            except Exception as e:
                print(f"   Error at row {index}: {e}")
                skipped += 1
                continue
        
        # Final commit
        conn.commit()
        print(f"✅ Inserted {inserted} records")
        print(f"   Skipped {skipped} records (missing data)")
        
        # Verify the data
        print("\n5. Verifying data...")
        cursor.execute("SELECT COUNT(*) FROM scottish_schools_temp")
        total = cursor.fetchone()[0]
        print(f"✅ Total records in scottish_schools_temp: {total}")
        
        # Show sample
        cursor.execute("""
            SELECT seed, name 
            FROM scottish_schools_temp 
            ORDER BY seed 
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        print("\n📚 SAMPLE DATA:")
        print("-" * 50)
        for seed, name in samples:
            print(f"Seed: {seed:5} | Name: {name}")
        print("-" * 50)
        
        print("\n✅ SUCCESS! Scottish schools data has been inserted.")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("\n✅ Database connection closed")
        print("=" * 70)


if __name__ == "__main__":
    insert_scottish_schools()