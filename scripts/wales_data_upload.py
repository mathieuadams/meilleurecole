import csv
import psycopg2
import logging
from datetime import datetime
from typing import Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'wales_data_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class WalesSchoolDataUploader:
    def __init__(self, db_url: str):
        """
        Initialize the uploader with database connection.
        
        Args:
            db_url: PostgreSQL connection URL
        """
        self.db_url = db_url
        self.schools_processed = 0
        self.schools_updated = 0
        self.schools_not_found = 0
        self.attendance_records_inserted = 0
        self.schools_with_scores = 0
        
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
    
    def safe_float(self, value: str) -> Optional[float]:
        """Safely convert string to float, handling empty values."""
        if not value or value.strip() == '' or value.strip().upper() == 'NULL':
            return None
        try:
            return float(value.strip())
        except (ValueError, AttributeError):
            return None
    
    def safe_int(self, value: str) -> Optional[int]:
        """Safely convert string to integer, handling empty values."""
        if not value or value.strip() == '' or value.strip().upper() == 'NULL':
            return None
        try:
            # Remove any decimal points if present
            return int(float(value.strip()))
        except (ValueError, AttributeError):
            return None
    
    def ensure_absence_table_exists(self):
        """Check if uk_absence_data table exists and create if needed."""
        try:
            # Check if table exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'uk_absence_data'
                )
            """)
            
            table_exists = self.cursor.fetchone()[0]
            
            if not table_exists:
                # Create the table if it doesn't exist
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS uk_absence_data (
                        id SERIAL PRIMARY KEY,
                        school_id INTEGER REFERENCES uk_schools(id),
                        academic_year VARCHAR(10),
                        overall_absence_rate DECIMAL(5, 2),
                        persistent_absence_rate DECIMAL(5, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP,
                        UNIQUE(school_id, academic_year)
                    )
                """)
                logging.info("Created uk_absence_data table")
            else:
                logging.info("uk_absence_data table already exists")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error checking/creating absence table: {e}")
            return False
    
    def update_school_performance(self, csv_file: str):
        """
        Update schools with performance data from CSV.
        
        Args:
            csv_file: Path to the Wales schools CSV file
        """
        try:
            # Read CSV file
            with open(csv_file, 'r', encoding='utf-8-sig') as file:
                csv_reader = csv.DictReader(file)
                schools = list(csv_reader)
            
            total_count = len(schools)
            logging.info(f"Found {total_count} schools in CSV file")
            
            # Debug: Check first few schools with data
            for i, school in enumerate(schools[:5]):
                logging.debug(f"Sample row {i}: School={school.get('School Name')}, "
                            f"URN={school.get('School Number')}, "
                            f"literacy={school.get('literacy_points')}, "
                            f"numeracy={school.get('numeracy_points')}, "
                            f"science={school.get('science_points')}")
            
            for idx, school in enumerate(schools, 1):
                school_name = school.get('School Name', '').strip()
                urn = school.get('School Number', '').strip()  # This is the URN
                postcode = school.get('Postcode', '').strip()
                
                # Get performance scores - using exact column names from CSV
                literacy_score = self.safe_float(school.get('literacy_points'))
                numeracy_score = self.safe_float(school.get('numeracy_points'))
                science_score = self.safe_float(school.get('science_points'))
                capped9_score = self.safe_float(school.get('capped9_points'))
                welsh_bacc_score = self.safe_float(school.get('welsh_bacc_points'))
                
                # Get other metrics
                pupil_count = self.safe_int(school.get('Pupils - see notes'))
                attendance_pct = self.safe_float(school.get('attendance_pct'))
                fsm_pct = self.safe_float(school.get('fsm_3yr_pct'))
                pupil_teacher_ratio = self.safe_float(school.get('pupil_teacher_ratio'))
                school_budget = self.safe_float(school.get('school_budget_per_pupil'))
                
                if not urn:
                    logging.warning(f"Row {idx}: Missing school number (URN), skipping - School: {school_name}")
                    continue
                
                # Track if this school has any scores
                has_scores = any([literacy_score, numeracy_score, science_score])
                
                # Try to find school by URN (School Number) - this is the most reliable
                self.cursor.execute("""
                    SELECT id, name FROM uk_schools 
                    WHERE urn = %s
                """, (urn,))
                
                result = self.cursor.fetchone()
                
                # If not found by URN, try by name and postcode as fallback
                if not result and school_name:
                    self.cursor.execute("""
                        SELECT id, name FROM uk_schools 
                        WHERE UPPER(name) = UPPER(%s) AND postcode = %s
                    """, (school_name, postcode))
                    result = self.cursor.fetchone()
                    
                    if result:
                        logging.info(f"Found school by name/postcode (URN {urn} not found): {school_name}")
                
                if result:
                    school_id = result[0]
                    db_school_name = result[1] if len(result) > 1 else school_name
                    
                    # Update the school with performance data
                    # Using COALESCE to only update if new value is not NULL
                    self.cursor.execute("""
                        UPDATE uk_schools 
                        SET english_score = COALESCE(%s, english_score),
                            math_score = COALESCE(%s, math_score),
                            science_score = COALESCE(%s, science_score),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (literacy_score, numeracy_score, science_score, school_id))
                    
                    self.schools_updated += 1
                    if has_scores:
                        self.schools_with_scores += 1
                        logging.info(f"[{idx}/{total_count}] Updated with scores - URN: {urn}, Name: {db_school_name} - Lit: {literacy_score}, Math: {numeracy_score}, Sci: {science_score}")
                    else:
                        logging.debug(f"[{idx}/{total_count}] Updated (no scores) - URN: {urn}, Name: {db_school_name}")
                    
                    # Insert attendance data if available
                    if attendance_pct is not None and attendance_pct > 0:
                        self.insert_attendance_record(school_id, db_school_name, attendance_pct, school.get('Local Authority'))
                    
                else:
                    self.schools_not_found += 1
                    if has_scores:
                        # Only log as warning if the school has scores we're missing
                        logging.warning(f"[{idx}/{total_count}] School with scores not found - URN: {urn}, Name: {school_name} - Lit: {literacy_score}, Math: {numeracy_score}, Sci: {science_score}")
                    else:
                        logging.debug(f"[{idx}/{total_count}] School not found (no scores) - URN: {urn}, Name: {school_name}")
                
                self.schools_processed += 1
                
                # Commit every 50 schools
                if idx % 50 == 0:
                    self.conn.commit()
                    logging.info(f"Committed batch. Progress: {idx}/{total_count} (Schools with scores: {self.schools_with_scores})")
            
            # Final commit
            self.conn.commit()
            logging.info("Final commit completed")
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error updating schools: {e}")
            raise
    
    def insert_attendance_record(self, school_id: int, school_name: str, attendance_pct: float, local_authority: str):
        """
        Insert attendance record into uk_absence_data table.
        
        Args:
            school_id: School ID from uk_schools table
            school_name: Name of the school
            attendance_pct: Attendance percentage (from CSV)
            local_authority: Local authority name
        """
        try:
            # Calculate absence rate from attendance percentage
            # CSV has attendance_pct, we need to convert to absence rate
            if attendance_pct <= 0 or attendance_pct > 100:
                logging.debug(f"Invalid attendance percentage for {school_name}: {attendance_pct}")
                return
            
            absence_rate = 100.0 - attendance_pct
            
            # Insert or update the absence record
            self.cursor.execute("""
                INSERT INTO uk_absence_data (
                    school_id, academic_year, overall_absence_rate, created_at
                ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (school_id, academic_year) 
                DO UPDATE SET 
                    overall_absence_rate = EXCLUDED.overall_absence_rate,
                    updated_at = CURRENT_TIMESTAMP
            """, (school_id, '2023/24', absence_rate))
            
            self.attendance_records_inserted += 1
            logging.debug(f"Inserted absence record for {school_name}: {absence_rate:.2f}%")
            
        except Exception as e:
            # Log the error but continue processing
            logging.warning(f"Could not insert absence record for {school_name}: {e}")
    
    def calculate_local_authority_averages(self):
        """Calculate average scores per local authority for Wales."""
        try:
            logging.info("Calculating local authority averages for Wales...")
            
            # Calculate averages for each local authority in Wales
            self.cursor.execute("""
                WITH la_averages AS (
                    SELECT 
                        local_authority,
                        AVG(english_score) FILTER (WHERE english_score IS NOT NULL) as avg_english,
                        AVG(math_score) FILTER (WHERE math_score IS NOT NULL) as avg_math,
                        AVG(science_score) FILTER (WHERE science_score IS NOT NULL) as avg_science,
                        COUNT(*) FILTER (WHERE english_score IS NOT NULL OR math_score IS NOT NULL OR science_score IS NOT NULL) as school_count
                    FROM uk_schools
                    WHERE region = 'Wales' AND local_authority IS NOT NULL
                    GROUP BY local_authority
                )
                UPDATE uk_schools s
                SET english_avg = la.avg_english,
                    math_avg = la.avg_math,
                    science_avg = la.avg_science
                FROM la_averages la
                WHERE s.local_authority = la.local_authority
                AND s.region = 'Wales'
                AND la.school_count > 0
            """)
            
            affected_rows = self.cursor.rowcount
            self.conn.commit()
            logging.info(f"Updated {affected_rows} Wales schools with local authority averages")
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error calculating local authority averages: {e}")
    
    def calculate_national_averages(self):
        """Calculate national (Wales) average scores."""
        try:
            logging.info("Calculating Wales national averages...")
            
            # Calculate Wales averages
            self.cursor.execute("""
                SELECT 
                    AVG(english_score) FILTER (WHERE english_score IS NOT NULL) as avg_english,
                    AVG(math_score) FILTER (WHERE math_score IS NOT NULL) as avg_math,
                    AVG(science_score) FILTER (WHERE science_score IS NOT NULL) as avg_science,
                    COUNT(*) FILTER (WHERE english_score IS NOT NULL) as schools_with_english,
                    COUNT(*) FILTER (WHERE math_score IS NOT NULL) as schools_with_math,
                    COUNT(*) FILTER (WHERE science_score IS NOT NULL) as schools_with_science
                FROM uk_schools
                WHERE region = 'Wales'
            """)
            
            result = self.cursor.fetchone()
            if result and result[0] is not None:
                avg_english, avg_math, avg_science, eng_count, math_count, sci_count = result
                
                # Log the national averages
                logging.info(f"Wales averages:")
                logging.info(f"  English: {avg_english:.2f} (from {eng_count} schools)")
                logging.info(f"  Math: {avg_math:.2f} (from {math_count} schools)")
                logging.info(f"  Science: {avg_science:.2f} (from {sci_count} schools)")
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error calculating national averages: {e}")
    
    def generate_stats(self):
        """Generate and log statistics about the upload process."""
        try:
            # Get statistics from database
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_wales_schools,
                    COUNT(CASE WHEN english_score IS NOT NULL THEN 1 END) as with_english,
                    COUNT(CASE WHEN math_score IS NOT NULL THEN 1 END) as with_math,
                    COUNT(CASE WHEN science_score IS NOT NULL THEN 1 END) as with_science,
                    AVG(english_score) as avg_english,
                    AVG(math_score) as avg_math,
                    AVG(science_score) as avg_science
                FROM uk_schools
                WHERE region = 'Wales'
            """)
            
            stats = self.cursor.fetchone()
            
            # Get absence data stats
            self.cursor.execute("""
                SELECT COUNT(DISTINCT school_id) 
                FROM uk_absence_data a
                JOIN uk_schools s ON a.school_id = s.id
                WHERE s.region = 'Wales' AND a.academic_year = '2023/24'
            """)
            
            absence_stats = self.cursor.fetchone()
            
            logging.info("=" * 60)
            logging.info("FINAL UPLOAD STATISTICS")
            logging.info("=" * 60)
            logging.info("Processing Summary:")
            logging.info(f"  Schools processed from CSV: {self.schools_processed}")
            logging.info(f"  Schools found and updated: {self.schools_updated}")
            logging.info(f"  Schools with score data: {self.schools_with_scores}")
            logging.info(f"  Schools not found in database: {self.schools_not_found}")
            logging.info(f"  Attendance records inserted: {self.attendance_records_inserted}")
            logging.info("-" * 60)
            logging.info("Wales Schools in Database:")
            logging.info(f"  Total Wales schools: {stats[0]}")
            logging.info(f"  Schools with English scores: {stats[1]}")
            logging.info(f"  Schools with Math scores: {stats[2]}")
            logging.info(f"  Schools with Science scores: {stats[3]}")
            if stats[4]:
                logging.info(f"  Average English score: {stats[4]:.2f}")
                logging.info(f"  Average Math score: {stats[5]:.2f}")
                logging.info(f"  Average Science score: {stats[6]:.2f}")
            if absence_stats and absence_stats[0]:
                logging.info(f"  Schools with absence data: {absence_stats[0]}")
            logging.info("=" * 60)
            
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
            
            # Ensure uk_absence_data table exists
            self.ensure_absence_table_exists()
            
            # Update schools with performance data
            logging.info("Starting data upload for Wales schools...")
            logging.info("Column mapping: literacy_points → english_score, numeracy_points → math_score, science_points → science_score")
            self.update_school_performance(csv_file)
            
            # Calculate local authority averages
            self.calculate_local_authority_averages()
            
            # Calculate national averages
            self.calculate_national_averages()
            
            # Generate statistics
            self.generate_stats()
            
        finally:
            self.close()


def main():
    """Main function to run the data upload process."""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    CSV_FILE = "wales_schools.csv"  # Your Wales schools CSV file
    
    # Create and run uploader
    uploader = WalesSchoolDataUploader(DATABASE_URL)
    uploader.run(CSV_FILE)
    
    print("\nData upload complete!")
    print("Check the log file for detailed statistics.")
    print("\nVerify with SQL:")
    print("  SELECT urn, name, english_score, math_score, science_score")
    print("  FROM uk_schools")
    print("  WHERE region = 'Wales' AND english_score IS NOT NULL")
    print("  LIMIT 10;")


if __name__ == "__main__":
    main()