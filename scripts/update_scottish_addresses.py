import pandas as pd
import psycopg2
from datetime import datetime

def update_scottish_school_addresses():
    """Update Scottish schools with missing address, contact, and coordinate data from 2023 file"""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    CSV_FILE = "ScotlandGSchoolRoll2023.csv"
    
    print("=" * 70)
    print("UPDATING SCOTTISH SCHOOLS WITH ADDRESS & CONTACT DATA")
    print("=" * 70)
    
    try:
        # Connect to database
        print("\n1. Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False  # Explicitly control transactions
        cursor = conn.cursor()
        print("✅ Database connected")
        
        # Ensure clean transaction state
        conn.rollback()
        
        # Read CSV
        print(f"\n2. Reading {CSV_FILE}...")
        df = pd.read_csv(CSV_FILE)
        print(f"✅ Found {len(df)} rows in CSV")
        
        # Process updates
        print("\n3. Updating missing fields for Scottish schools...")
        updated = 0
        skipped = 0
        not_found = 0
        
        for index, row in df.iterrows():
            try:
                if pd.isna(row.get('SeedCode')):
                    skipped += 1
                    continue
                
                seed_code = int(row['SeedCode'])
                
                # Build update query - only update NULL fields
                update_fields = []
                update_values = []
                
                # Address fields
                if pd.notna(row.get('AddressLine1')):
                    update_fields.append("street = COALESCE(street, %s)")
                    update_values.append(str(row['AddressLine1']).strip())
                
                if pd.notna(row.get('AddressLine2')):
                    update_fields.append("locality = COALESCE(locality, %s)")
                    update_values.append(str(row['AddressLine2']).strip())
                
                if pd.notna(row.get('AddressLine3')):
                    update_fields.append("town = COALESCE(town, %s)")
                    update_values.append(str(row['AddressLine3']).strip())
                
                if pd.notna(row.get('PostCode')):
                    update_fields.append("postcode = COALESCE(postcode, %s)")
                    update_values.append(str(row['PostCode']).strip())
                
                # Coordinates
                if pd.notna(row.get('Latitude')):
                    update_fields.append("latitude = COALESCE(latitude, %s)")
                    update_values.append(float(row['Latitude']))
                
                if pd.notna(row.get('Longitude')):
                    update_fields.append("longitude = COALESCE(longitude, %s)")
                    update_values.append(float(row['Longitude']))
                
                # Contact information
                if pd.notna(row.get('Email')):
                    update_fields.append("email = COALESCE(email, %s)")
                    update_values.append(str(row['Email']).strip())
                
                if pd.notna(row.get('Phone')):
                    update_fields.append("telephone = COALESCE(telephone, %s)")
                    update_values.append(str(row['Phone']).strip())
                
                if pd.notna(row.get('Website')):
                    update_fields.append("website = COALESCE(website, %s)")
                    update_values.append(str(row['Website']).strip())
                
                # UPRN
                if pd.notna(row.get('UPRN')):
                    update_fields.append("uprn = COALESCE(uprn, %s)")
                    update_values.append(str(row['UPRN']).strip())
                
                # LA Code (if missing) - handle both numeric and string codes
                if pd.notna(row.get('LACode')):
                    la_code = str(row['LACode']).strip()
                    # Check if it's numeric or a string code like 'S12000029'
                    try:
                        # Try to convert to int if it's purely numeric
                        la_code_value = int(la_code)
                    except ValueError:
                        # Keep as string if it contains letters (Scottish LA codes)
                        la_code_value = None  # Skip LA code update for string codes
                    
                    if la_code_value is not None:
                        update_fields.append("la_code = COALESCE(la_code, %s)")
                        update_values.append(la_code_value)
                
                # Only update if we have fields to update
                if update_fields:
                    # Add the WHERE clause values
                    update_values.extend([seed_code, seed_code])
                    
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
                    else:
                        not_found += 1
                
                # Show progress
                if updated % 100 == 0 and updated > 0:
                    print(f"   Updated {updated} schools...")
                    conn.commit()  # Commit in batches
                    
            except Exception as e:
                print(f"   Error at row {index}: {e}")
                skipped += 1
                # Rollback the failed transaction and start a new one
                conn.rollback()
                continue
        
        # Final commit
        conn.commit()
        
        # Verify results
        print("\n4. Checking updated data...")
        
        # Count schools with addresses
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(postcode) as with_postcode,
                COUNT(latitude) as with_coords,
                COUNT(telephone) as with_phone,
                COUNT(email) as with_email,
                COUNT(website) as with_website
            FROM uk_schools 
            WHERE country = 'Scotland'
        """)
        stats = cursor.fetchone()
        
        # Show sample
        cursor.execute("""
            SELECT seed, name, postcode, latitude, longitude, telephone
            FROM uk_schools 
            WHERE country = 'Scotland' 
            AND postcode IS NOT NULL
            ORDER BY seed
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        print("\n" + "=" * 70)
        print("UPDATE SUMMARY:")
        print(f"   Schools updated: {updated}")
        print(f"   Schools not found: {not_found}")
        print(f"   Rows skipped: {skipped}")
        print(f"\nSCOTTISH SCHOOLS DATA COMPLETENESS:")
        print(f"   Total Scottish schools: {stats[0]}")
        print(f"   With postcode: {stats[1]} ({stats[1]*100/stats[0]:.1f}%)")
        print(f"   With coordinates: {stats[2]} ({stats[2]*100/stats[0]:.1f}%)")
        print(f"   With telephone: {stats[3]} ({stats[3]*100/stats[0]:.1f}%)")
        print(f"   With email: {stats[4]} ({stats[4]*100/stats[0]:.1f}%)")
        print(f"   With website: {stats[5]} ({stats[5]*100/stats[0]:.1f}%)")
        
        if samples:
            print("\n📍 SAMPLE SCHOOLS WITH ADDRESSES:")
            print("-" * 80)
            for seed, name, postcode, lat, lng, phone in samples:
                print(f"Seed: {seed} | {name[:30]:<30} | {postcode:<8} | {lat:.4f}, {lng:.4f}")
        
        print("\n✅ SUCCESS! Address and contact data has been added")
        
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
    update_scottish_school_addresses()