import csv
import psycopg2
import requests
import time
import logging
from datetime import datetime
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'geocoding_wales_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class WalesSchoolGeocoder:
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
        self.insert_count = 0
        
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
    
    def build_address(self, row: dict) -> str:
        """
        Build a complete address string from school data.
        
        Args:
            row: Dictionary containing school data
            
        Returns:
            Formatted address string
        """
        address_parts = []
        
        # Start with school name for better recognition
        if row.get('School Name'):
            # Clean the school name
            school_name = row['School Name'].replace('&', 'and')
            address_parts.append(school_name)
        
        # Add address fields
        for field in ['Address 1', 'Address 2', 'Address 3', 'Address 4']:
            if row.get(field) and row[field].strip():
                address_parts.append(row[field].strip())
        
        # Add postcode (most important for UK geocoding)
        if row.get('Postcode') and row['Postcode'].strip():
            address_parts.append(row['Postcode'].strip())
        
        # Add Wales, UK to improve accuracy
        address_parts.append('Wales')
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
                    logging.warning(f"No results found for {school_name}")
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
    
    def insert_or_update_school(self, school_data: dict, coords: Tuple[float, float]):
        """
        Insert or update a school in the database with coordinates.
        
        Args:
            school_data: Dictionary containing school information
            coords: Tuple of (latitude, longitude)
        """
        try:
            # First, check if school already exists (by name and postcode)
            self.cursor.execute("""
                SELECT id FROM uk_schools 
                WHERE name = %s AND postcode = %s
            """, (school_data['School Name'], school_data.get('Postcode', '')))
            
            existing = self.cursor.fetchone()
            
            if existing:
                # Update existing school with coordinates
                self.cursor.execute("""
                    UPDATE uk_schools 
                    SET latitude = %s, longitude = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (coords[0], coords[1], existing[0]))
                logging.info(f"Updated existing school: {school_data['School Name']}")
            else:
                # Insert new school with all available data
                # Map CSV fields to database columns
                street = school_data.get('Address 1', '')
                locality = school_data.get('Address 2', '')
                town = school_data.get('Address 3', '') or school_data.get('Address 4', '')
                
                self.cursor.execute("""
                    INSERT INTO uk_schools (
                        name, street, locality, town, postcode, 
                        local_authority, latitude, longitude, region
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    school_data['School Name'],
                    street,
                    locality,
                    town,
                    school_data.get('Postcode', ''),
                    school_data.get('Local Authority', ''),
                    coords[0],
                    coords[1],
                    'Wales'
                ))
                self.insert_count += 1
                logging.info(f"Inserted new school: {school_data['School Name']}")
                
        except Exception as e:
            logging.error(f"Error inserting/updating school {school_data['School Name']}: {e}")
            self.conn.rollback()
    
    def process_wales_schools(self, csv_file: str, batch_size: int = 50, delay: float = 0.15):
        """
        Process Wales schools from CSV and update database.
        
        Args:
            csv_file: Path to the Wales schools CSV file
            batch_size: Number of schools to process before committing
            delay: Delay between API requests (in seconds)
        """
        try:
            # Read CSV file
            with open(csv_file, 'r', encoding='utf-8-sig') as file:
                csv_reader = csv.DictReader(file)
                schools = list(csv_reader)
            
            total_count = len(schools)
            logging.info(f"Found {total_count} schools in CSV file")
            
            # Process schools in batches
            for idx, school in enumerate(schools, 1):
                school_name = school.get('School Name', 'Unknown')
                
                # Build address
                address = self.build_address(school)
                
                # Try geocoding with full address
                coords = self.geocode_address(address, school_name)
                
                # If failed and we have a postcode, try simpler combinations
                if not coords and school.get('Postcode'):
                    # Try school name + postcode
                    name_postcode_address = f"{school_name}, {school['Postcode']}, Wales, UK"
                    coords = self.geocode_address(name_postcode_address, school_name)
                    
                    # Last resort: just postcode
                    if not coords:
                        simple_address = f"{school['Postcode']}, Wales, UK"
                        coords = self.geocode_address(simple_address, school_name)
                
                if coords:
                    # Insert or update in database
                    self.insert_or_update_school(school, coords)
                    self.success_count += 1
                else:
                    self.error_count += 1
                    logging.warning(f"Failed to geocode: {school_name}")
                
                # Commit batch
                if idx % batch_size == 0:
                    self.conn.commit()
                    logging.info(f"Committed batch. Progress: {idx}/{total_count}")
                
                # Rate limiting
                time.sleep(delay)
                
                # Progress update
                if idx % 10 == 0:
                    logging.info(f"Progress: {idx}/{total_count} processed")
            
            # Final commit
            self.conn.commit()
            logging.info("Final commit completed")
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error processing schools: {e}")
            raise
    
    def generate_stats(self):
        """Generate and log statistics about the geocoding process."""
        try:
            # Get statistics from database
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_schools,
                    COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as geocoded,
                    COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as not_geocoded,
                    COUNT(CASE WHEN region = 'Wales' THEN 1 END) as wales_schools
                FROM uk_schools
            """)
            
            stats = self.cursor.fetchone()
            
            logging.info("=" * 50)
            logging.info("GEOCODING STATISTICS")
            logging.info("=" * 50)
            logging.info(f"Total schools in database: {stats[0]}")
            logging.info(f"Schools with coordinates: {stats[1]}")
            logging.info(f"Schools without coordinates: {stats[2]}")
            logging.info(f"Wales schools in database: {stats[3]}")
            logging.info(f"Coverage: {(stats[1]/stats[0]*100):.2f}%")
            logging.info(f"API requests made: {self.request_count}")
            logging.info(f"Successful geocodes this run: {self.success_count}")
            logging.info(f"Failed geocodes this run: {self.error_count}")
            logging.info(f"New schools inserted: {self.insert_count}")
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
    
    def run(self, csv_file: str):
        """
        Main execution method.
        
        Args:
            csv_file: Path to Wales schools CSV file
        """
        try:
            # Connect to database
            if not self.connect_db():
                return
            
            # Add lat/long columns if needed
            if not self.add_lat_long_columns():
                return
            
            # Process Wales schools from CSV
            logging.info("Starting geocoding process for Wales schools...")
            self.process_wales_schools(csv_file, batch_size=50, delay=0.15)
            
            # Generate statistics
            self.generate_stats()
            
        finally:
            self.close()


def main():
    """Main function to run the geocoding process."""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    GOOGLE_API_KEY = "AIzaSyDGgHqBIdpFRcUTSHzwAh754dIvLT-ZBT8"
    CSV_FILE = "wales_schools.csv"  # Your Wales schools CSV file
    
    # Create and run geocoder
    geocoder = WalesSchoolGeocoder(DATABASE_URL, GOOGLE_API_KEY)
    geocoder.run(CSV_FILE)
    
    print("\nGeocoding complete! Wales schools have been added/updated in the database.")
    print("Check the log file for detailed information about the process.")


if __name__ == "__main__":
    main()