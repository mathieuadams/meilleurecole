import psycopg2
import requests
import time
from typing import Optional, Tuple
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'geocoding_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class SchoolGeocoder:
    def __init__(self, db_url: str, google_api_key: str):
        """
        Initialize the geocoder with database connection and Google API key.
        
        Args:
            db_url: PostgreSQL connection URL
            google_api_key: Google Maps API key
        """
        self.db_url = db_url
        self.google_api_key = google_api_key
        self.geocoding_url = "https://maps.googleapis.com/maps/api/geocode/json"
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        
    def connect_db(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.cursor = self.conn.cursor()
            logging.info("Database connection established")
            return True
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            return False
    
    def add_lat_long_columns(self):
        """Add latitude and longitude columns to uk_schools table if they don't exist."""
        try:
            # Check if columns already exist
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='uk_schools' 
                AND column_name IN ('latitude', 'longitude')
            """)
            existing_cols = [row[0] for row in self.cursor.fetchall()]
            
            # Add latitude column if it doesn't exist
            if 'latitude' not in existing_cols:
                self.cursor.execute("""
                    ALTER TABLE uk_schools 
                    ADD COLUMN latitude DECIMAL(10, 8)
                """)
                logging.info("Added latitude column to uk_schools table")
            
            # Add longitude column if it doesn't exist
            if 'longitude' not in existing_cols:
                self.cursor.execute("""
                    ALTER TABLE uk_schools 
                    ADD COLUMN longitude DECIMAL(11, 8)
                """)
                logging.info("Added longitude column to uk_schools table")
            
            # Add index for geographic queries
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_uk_schools_lat_long 
                ON uk_schools(latitude, longitude)
            """)
            
            self.conn.commit()
            logging.info("Database schema updated successfully")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Failed to add columns: {e}")
            return False
    
    def build_address(self, row: dict, include_name: bool = True) -> str:
        """
        Build a complete address string from school data.
        
        Args:
            row: Dictionary containing school data
            include_name: Whether to include school name in address
            
        Returns:
            Formatted address string
        """
        address_parts = []
        
        # Start with school name for better recognition
        if include_name and row.get('name'):
            # Clean the school name - remove any special characters that might confuse geocoding
            school_name = row['name'].replace('&', 'and')
            address_parts.append(school_name)
        
        # Add street if available (often without numbers in UK)
        if row.get('street'):
            address_parts.append(row['street'])
        
        # Add locality if available
        if row.get('locality'):
            address_parts.append(row['locality'])
        
        # Add town if available
        if row.get('town'):
            address_parts.append(row['town'])
        
        # Add postcode (most important for UK geocoding)
        if row.get('postcode'):
            address_parts.append(row['postcode'])
        
        # Add UK to improve accuracy
        address_parts.append('United Kingdom')
        
        return ', '.join(filter(None, address_parts))
    
    def geocode_address(self, address: str, school_name: str = "") -> Optional[Tuple[float, float]]:
        """
        Geocode an address using Google Maps API.
        
        Args:
            address: Address string to geocode
            school_name: School name for logging purposes
            
        Returns:
            Tuple of (latitude, longitude) or None if failed
        """
        try:
            params = {
                'address': address,
                'key': self.google_api_key,
                'region': 'uk',  # Bias results to UK
                'components': 'country:GB'  # Restrict to Great Britain
            }
            
            response = requests.get(self.geocoding_url, params=params)
            self.request_count += 1
            
            if response.status_code == 200:
                data = response.json()
                
                if data['status'] == 'OK' and data['results']:
                    # Get the first result
                    result = data['results'][0]
                    location = result['geometry']['location']
                    lat = location['lat']
                    lng = location['lng']
                    
                    # Check if it's actually a school or educational institution
                    types = result.get('types', [])
                    place_type = result.get('place_id', '')
                    
                    # Log the match quality
                    if 'school' in types or 'secondary_school' in types or 'primary_school' in types:
                        logging.info(f"Perfect match - School found: {school_name}")
                    elif 'establishment' in types or 'point_of_interest' in types:
                        logging.info(f"Good match - POI found: {school_name}")
                    else:
                        logging.info(f"Address match: {school_name} - Types: {types[:3]}")
                    
                    # Verify the result is in the UK (approximate bounds)
                    if 49.5 <= lat <= 61.0 and -8.0 <= lng <= 2.0:
                        return (lat, lng)
                    else:
                        logging.warning(f"Location outside UK bounds for {school_name}: {lat}, {lng}")
                        return None
                        
                elif data['status'] == 'ZERO_RESULTS':
                    logging.warning(f"No results found for {school_name}: {address[:50]}...")
                    return None
                    
                elif data['status'] == 'OVER_QUERY_LIMIT':
                    logging.error("API query limit reached. Pausing...")
                    time.sleep(60)  # Wait 1 minute before retrying
                    return None
                    
                else:
                    logging.error(f"Geocoding error for {school_name}: {data['status']}")
                    return None
                    
            else:
                logging.error(f"HTTP error {response.status_code} for {school_name}")
                return None
                
        except Exception as e:
            logging.error(f"Exception geocoding {school_name}: {e}")
            return None
    
    def process_schools(self, batch_size: int = 100, delay: float = 0.1):
        """
        Process all schools without coordinates.
        
        Args:
            batch_size: Number of schools to process before committing
            delay: Delay between API requests (in seconds)
        """
        try:
            # First, get count of schools without coordinates
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM uk_schools 
                WHERE latitude IS NULL OR longitude IS NULL
            """)
            total_count = self.cursor.fetchone()[0]
            logging.info(f"Found {total_count} schools without coordinates")
            
            if total_count == 0:
                logging.info("All schools already have coordinates")
                return
            
            # Process schools in batches
            offset = 0
            
            while offset < total_count:
                # Fetch batch of schools without coordinates
                self.cursor.execute("""
                    SELECT id, urn, name, street, locality, town, postcode, local_authority
                    FROM uk_schools 
                    WHERE latitude IS NULL OR longitude IS NULL
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
                
                schools = self.cursor.fetchall()
                
                if not schools:
                    break
                
                for school in schools:
                    school_dict = {
                        'id': school[0],
                        'urn': school[1],
                        'name': school[2],
                        'street': school[3],
                        'locality': school[4],
                        'town': school[5],
                        'postcode': school[6],
                        'local_authority': school[7]
                    }
                    
                    # Build address WITH school name first (better for UK schools)
                    address_with_name = self.build_address(school_dict, include_name=True)
                    
                    # Try geocoding with school name + full address first
                    coords = self.geocode_address(address_with_name, school_dict['name'])
                    
                    # If failed, try without school name
                    if not coords:
                        address_without_name = self.build_address(school_dict, include_name=False)
                        coords = self.geocode_address(address_without_name, school_dict['name'])
                    
                    # If still failed and we have a postcode, try different combinations
                    if not coords and school_dict.get('postcode'):
                        # Try school name + postcode (often works well for UK schools)
                        name_postcode_address = f"{school_dict['name']}, {school_dict['postcode']}, UK"
                        coords = self.geocode_address(name_postcode_address, school_dict['name'])
                        
                        # Last resort: just postcode
                        if not coords:
                            simple_address = f"{school_dict['postcode']}, UK"
                            coords = self.geocode_address(simple_address, school_dict['name'])
                    
                    if coords:
                        # Update database with coordinates
                        self.cursor.execute("""
                            UPDATE uk_schools 
                            SET latitude = %s, longitude = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (coords[0], coords[1], school_dict['id']))
                        self.success_count += 1
                    else:
                        self.error_count += 1
                    
                    # Rate limiting
                    time.sleep(delay)
                    
                    # Progress update
                    if (self.success_count + self.error_count) % 10 == 0:
                        logging.info(f"Progress: {self.success_count + self.error_count}/{total_count} processed")
                
                # Commit batch
                self.conn.commit()
                logging.info(f"Committed batch. Total processed: {self.success_count + self.error_count}")
                
                offset += batch_size
                
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error processing schools: {e}")
            raise
    
    def generate_stats(self):
        """Generate and log statistics about the geocoding process."""
        try:
            # Get statistics
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_schools,
                    COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as geocoded,
                    COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as not_geocoded
                FROM uk_schools
            """)
            
            stats = self.cursor.fetchone()
            
            logging.info("=" * 50)
            logging.info("GEOCODING STATISTICS")
            logging.info("=" * 50)
            logging.info(f"Total schools in database: {stats[0]}")
            logging.info(f"Schools with coordinates: {stats[1]}")
            logging.info(f"Schools without coordinates: {stats[2]}")
            logging.info(f"Coverage: {(stats[1]/stats[0]*100):.2f}%")
            logging.info(f"API requests made: {self.request_count}")
            logging.info(f"Successful geocodes this run: {self.success_count}")
            logging.info(f"Failed geocodes this run: {self.error_count}")
            logging.info("=" * 50)
            
        except Exception as e:
            logging.error(f"Error generating statistics: {e}")
    
    def close(self):
        """Close database connection."""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()
        logging.info("Database connection closed")
    
    def run(self):
        """Main execution method."""
        try:
            # Connect to database
            if not self.connect_db():
                return
            
            # Add lat/long columns if needed
            if not self.add_lat_long_columns():
                return
            
            # Process schools
            logging.info("Starting geocoding process...")
            self.process_schools(batch_size=50, delay=0.15)  # Adjust delay to stay within rate limits
            
            # Generate statistics
            self.generate_stats()
            
        finally:
            self.close()


def main():
    """Main function to run the geocoding process."""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    GOOGLE_API_KEY = "AIzaSyDGgHqBIdpFRcUTSHzwAh754dIvLT-ZBT8"  # Replace with your actual API key
    
    # Validate API key
    if GOOGLE_API_KEY == "YOUR_GOOGLE_MAPS_API_KEY":
        logging.error("Please set your Google Maps API key in the script")
        return
    
    # Create and run geocoder
    geocoder = SchoolGeocoder(DATABASE_URL, GOOGLE_API_KEY)
    geocoder.run()


if __name__ == "__main__":
    main()