import psycopg2
import requests
import json
from typing import Optional, Tuple
from datetime import datetime

def test_single_school_geocoding():
    """Test geocoding with a single school (URN: 104736) to verify the setup works."""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    GOOGLE_API_KEY = "AIzaSyDGgHqBIdpFRcUTSHzwAh754dIvLT-ZBT8"  # Replace with your actual API key
    
    # Test school URN
    TEST_URN = 104736
    
    # Check API key
    if GOOGLE_API_KEY == "YOUR_GOOGLE_MAPS_API_KEY":
        print("❌ ERROR: Please set your Google Maps API key in the script")
        return
    
    print("=" * 70)
    print("UK SCHOOLS GEOCODING - SINGLE SCHOOL TEST")
    print(f"Testing with URN: {TEST_URN}")
    print("=" * 70)
    
    try:
        # Connect to database
        print("\n1. Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Database connected successfully")
        
        # Check if lat/long columns exist
        print("\n2. Checking database schema...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='uk_schools' 
            AND column_name IN ('latitude', 'longitude')
        """)
        existing_cols = [row[0] for row in cursor.fetchall()]
        
        need_schema_update = False
        if 'latitude' not in existing_cols:
            print("📝 Adding latitude column...")
            cursor.execute("ALTER TABLE uk_schools ADD COLUMN latitude DECIMAL(10, 8)")
            need_schema_update = True
            
        if 'longitude' not in existing_cols:
            print("📝 Adding longitude column...")
            cursor.execute("ALTER TABLE uk_schools ADD COLUMN longitude DECIMAL(11, 8)")
            need_schema_update = True
        
        if need_schema_update:
            # Add index for geographic queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_uk_schools_lat_long 
                ON uk_schools(latitude, longitude)
            """)
            conn.commit()
            print("✅ Database schema updated")
        else:
            print("✅ Latitude and longitude columns already exist")
        
        # Fetch the test school
        print(f"\n3. Fetching school with URN {TEST_URN}...")
        cursor.execute("""
            SELECT id, urn, name, street, locality, town, postcode, 
                   local_authority, latitude, longitude
            FROM uk_schools 
            WHERE urn = %s
        """, (TEST_URN,))
        
        school = cursor.fetchone()
        
        if not school:
            print(f"❌ ERROR: No school found with URN {TEST_URN}")
            return
        
        # Display school information
        print("\n📚 SCHOOL INFORMATION:")
        print("-" * 50)
        print(f"ID: {school[0]}")
        print(f"URN: {school[1]}")
        print(f"Name: {school[2]}")
        print(f"Street: {school[3] or 'N/A'}")
        print(f"Locality: {school[4] or 'N/A'}")
        print(f"Town: {school[5] or 'N/A'}")
        print(f"Postcode: {school[6] or 'N/A'}")
        print(f"Local Authority: {school[7] or 'N/A'}")
        print(f"Current Latitude: {school[8] or 'Not set'}")
        print(f"Current Longitude: {school[9] or 'Not set'}")
        print("-" * 50)
        
        if school[8] and school[9]:
            print("\n⚠️  This school already has coordinates. Testing anyway...")
        
        # Build different address formats
        print("\n4. Building address variations...")
        
        addresses = []
        
        # Format 1: School name + full address
        parts1 = []
        if school[2]:  # name
            parts1.append(school[2].replace('&', 'and'))
        if school[3]:  # street
            parts1.append(school[3])
        if school[4]:  # locality
            parts1.append(school[4])
        if school[5]:  # town
            parts1.append(school[5])
        if school[6]:  # postcode
            parts1.append(school[6])
        parts1.append("United Kingdom")
        address1 = ", ".join(filter(None, parts1))
        addresses.append(("Full with name", address1))
        
        # Format 2: Without school name
        parts2 = []
        if school[3]:  # street
            parts2.append(school[3])
        if school[4]:  # locality
            parts2.append(school[4])
        if school[5]:  # town
            parts2.append(school[5])
        if school[6]:  # postcode
            parts2.append(school[6])
        parts2.append("United Kingdom")
        address2 = ", ".join(filter(None, parts2))
        addresses.append(("Without name", address2))
        
        # Format 3: School name + postcode
        if school[2] and school[6]:
            address3 = f"{school[2]}, {school[6]}, United Kingdom"
            addresses.append(("Name + postcode", address3))
        
        # Format 4: Just postcode
        if school[6]:
            address4 = f"{school[6]}, United Kingdom"
            addresses.append(("Postcode only", address4))
        
        print("\n📍 ADDRESS VARIATIONS TO TRY:")
        for i, (label, addr) in enumerate(addresses, 1):
            print(f"{i}. {label}: {addr}")
        
        # Try geocoding each address
        print("\n5. Testing Google Geocoding API...")
        print("-" * 50)
        
        geocoding_url = "https://maps.googleapis.com/maps/api/geocode/json"
        successful_result = None
        
        for label, address in addresses:
            print(f"\n🔍 Trying: {label}")
            print(f"   Address: {address[:80]}...")
            
            params = {
                'address': address,
                'key': GOOGLE_API_KEY,
                'region': 'uk',
                'components': 'country:GB'
            }
            
            try:
                response = requests.get(geocoding_url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    print(f"   API Status: {data['status']}")
                    
                    if data['status'] == 'OK' and data['results']:
                        result = data['results'][0]
                        location = result['geometry']['location']
                        lat = location['lat']
                        lng = location['lng']
                        
                        # Get place types
                        types = result.get('types', [])
                        formatted_address = result.get('formatted_address', '')
                        
                        print(f"   ✅ FOUND: Lat: {lat:.6f}, Long: {lng:.6f}")
                        print(f"   Place types: {', '.join(types[:3])}")
                        print(f"   Formatted: {formatted_address}")
                        
                        # Check if it's in UK bounds
                        if 49.5 <= lat <= 61.0 and -8.0 <= lng <= 2.0:
                            print(f"   ✅ Location is within UK bounds")
                            
                            # Check if it's recognized as a school
                            if 'school' in types or 'secondary_school' in types or 'primary_school' in types:
                                print(f"   🏆 EXCELLENT: Recognized as a school!")
                            elif 'establishment' in types or 'point_of_interest' in types:
                                print(f"   👍 GOOD: Recognized as a point of interest")
                            else:
                                print(f"   📍 OK: General address match")
                            
                            if not successful_result:
                                successful_result = (lat, lng, label)
                            break
                        else:
                            print(f"   ⚠️  Location outside UK bounds!")
                    
                    elif data['status'] == 'ZERO_RESULTS':
                        print(f"   ❌ No results found")
                    
                    elif data['status'] == 'OVER_QUERY_LIMIT':
                        print(f"   ❌ API quota exceeded")
                        break
                    
                    elif data['status'] == 'REQUEST_DENIED':
                        print(f"   ❌ API key issue - check your key and permissions")
                        if 'error_message' in data:
                            print(f"   Error: {data['error_message']}")
                        break
                    
                    else:
                        print(f"   ❌ API Error: {data['status']}")
                        
                else:
                    print(f"   ❌ HTTP Error: {response.status_code}")
                    
            except Exception as e:
                print(f"   ❌ Exception: {str(e)}")
        
        # Update database if successful
        if successful_result:
            lat, lng, method = successful_result
            print("\n" + "=" * 50)
            print("✅ GEOCODING SUCCESSFUL!")
            print(f"   Method used: {method}")
            print(f"   Latitude: {lat}")
            print(f"   Longitude: {lng}")
            
            print("\n6. Updating database...")
            cursor.execute("""
                UPDATE uk_schools 
                SET latitude = %s, longitude = %s, updated_at = CURRENT_TIMESTAMP
                WHERE urn = %s
            """, (lat, lng, TEST_URN))
            conn.commit()
            print("✅ Database updated successfully!")
            
            # Verify the update
            cursor.execute("""
                SELECT latitude, longitude 
                FROM uk_schools 
                WHERE urn = %s
            """, (TEST_URN,))
            updated = cursor.fetchone()
            print(f"\n📊 VERIFICATION:")
            print(f"   Stored Latitude: {updated[0]}")
            print(f"   Stored Longitude: {updated[1]}")
            
            # Generate Google Maps link
            maps_url = f"https://www.google.com/maps?q={lat},{lng}"
            print(f"\n🗺️  View on Google Maps: {maps_url}")
            
        else:
            print("\n" + "=" * 50)
            print("❌ GEOCODING FAILED - Could not find coordinates")
            print("   Please check:")
            print("   1. Your Google API key is valid")
            print("   2. Geocoding API is enabled in Google Cloud Console")
            print("   3. The school address data is accurate")
        
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
    test_single_school_geocoding()