import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'edubase_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class EdubaseUpdater:
    def __init__(self, db_url: str, csv_file_path: str):
        """
        Initialize the updater with database connection and CSV file path.
        
        Args:
            db_url: PostgreSQL connection URL
            csv_file_path: Path to the edubase CSV file
        """
        self.db_url = db_url
        self.csv_file_path = csv_file_path
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_records': 0,
            'matched_schools': 0,
            'updated_records': 0,
            'failed_updates': 0,
            'schools_with_phone': 0,
            'schools_with_website': 0,
            'schools_with_head': 0
        }
    
    def connect_db(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.cursor = self.conn.cursor()
            logging.info("✅ Database connection established")
            return True
        except Exception as e:
            logging.error(f"❌ Database connection failed: {e}")
            return False
    
    def load_csv_data(self):
        """Load and process the CSV file."""
        try:
            logging.info(f"Loading CSV file: {self.csv_file_path}")
            
            # Read CSV with proper encoding
            df = pd.read_csv(
                self.csv_file_path, 
                encoding='utf-8',
                dtype={'URN': int, 'TelephoneNum': str}
            )
            
            logging.info(f"✅ Loaded {len(df)} records from CSV")
            
            # Clean column names (remove any spaces)
            df.columns = df.columns.str.strip()
            
            # Display first few column names for verification
            logging.info(f"Sample columns: {list(df.columns[:10])}")
            
            return df
            
        except UnicodeDecodeError:
            # Try with latin-1 encoding if utf-8 fails
            logging.info("Trying latin-1 encoding...")
            df = pd.read_csv(
                self.csv_file_path, 
                encoding='latin-1',
                dtype={'URN': int, 'TelephoneNum': str}
            )
            df.columns = df.columns.str.strip()
            return df
            
        except Exception as e:
            logging.error(f"❌ Failed to load CSV: {e}")
            return None
    
    def clean_phone_number(self, phone):
        """Clean and standardize phone numbers."""
        if pd.isna(phone) or phone == '' or phone == 'Not applicable':
            return None
        
        # Convert to string and strip whitespace
        phone = str(phone).strip()
        
        # Remove any 'Tel:' prefix
        phone = phone.replace('Tel:', '').replace('tel:', '').strip()
        
        return phone if phone else None
    
    def clean_website(self, website):
        """Clean and standardize website URLs."""
        if pd.isna(website) or website == '' or website == 'Not applicable':
            return None
        
        website = str(website).strip()
        
        # Add http:// if no protocol specified
        if website and not website.startswith(('http://', 'https://')):
            website = 'http://' + website
        
        # Truncate if longer than 500 characters (database limit)
        if website and len(website) > 500:
            website = website[:500]
        
        return website if website else None
    
    def clean_text_field(self, text, max_length=None):
        """Clean text fields."""
        if pd.isna(text) or text == '' or text == 'Not applicable':
            return None
        
        text = str(text).strip()
        
        # Truncate if max_length specified
        if text and max_length and len(text) > max_length:
            text = text[:max_length]
        
        return text if text else None
    
    def update_schools(self, df):
        """Update schools with contact information."""
        try:
            logging.info("Starting school updates...")
            
            # Process in batches
            batch_size = 100
            total_processed = 0
            
            for start_idx in range(0, len(df), batch_size):
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]
                
                for idx, row in batch_df.iterrows():
                    urn = row.get('URN')
                    if pd.isna(urn):
                        continue
                    
                    # Extract and clean data
                    phone = self.clean_phone_number(row.get('TelephoneNum'))
                    website = self.clean_website(row.get('SchoolWebsite'))
                    head_title = self.clean_text_field(row.get('HeadTitle', ''), 20)
                    head_first = self.clean_text_field(row.get('HeadFirstName', ''), 100)
                    head_last = self.clean_text_field(row.get('HeadLastName', ''), 100)
                    head_job = self.clean_text_field(row.get('HeadPreferredJobTitle', ''), 100)
                    
                    # Track statistics
                    if phone:
                        self.stats['schools_with_phone'] += 1
                    if website:
                        self.stats['schools_with_website'] += 1
                    if head_first or head_last:
                        self.stats['schools_with_head'] += 1
                    
                    # Update the database
                    try:
                        self.cursor.execute("""
                            UPDATE uk_schools
                            SET 
                                telephone = COALESCE(%s, telephone),
                                website = COALESCE(%s, website),
                                head_title = COALESCE(%s, head_title),
                                head_first_name = COALESCE(%s, head_first_name),
                                head_last_name = COALESCE(%s, head_last_name),
                                head_job_title = COALESCE(%s, head_job_title),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE urn = %s
                        """, (phone, website, head_title, head_first, head_last, head_job, int(urn)))
                        
                        if self.cursor.rowcount > 0:
                            self.stats['matched_schools'] += 1
                            self.stats['updated_records'] += 1
                            
                    except Exception as e:
                        logging.error(f"Error updating URN {urn}: {e}")
                        self.stats['failed_updates'] += 1
                
                # Commit batch
                self.conn.commit()
                total_processed += len(batch_df)
                logging.info(f"Processed {total_processed}/{len(df)} records...")
            
            logging.info(f"✅ Update complete. {self.stats['updated_records']} schools updated")
            
        except Exception as e:
            logging.error(f"❌ Error during update: {e}")
            self.conn.rollback()
    
    def verify_updates(self):
        """Verify the updates by checking a sample of schools."""
        try:
            logging.info("\nVerifying updates...")
            
            # Check schools with websites
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM uk_schools 
                WHERE website IS NOT NULL AND website != ''
            """)
            website_count = self.cursor.fetchone()[0]
            
            # Check schools with phone numbers
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM uk_schools 
                WHERE telephone IS NOT NULL AND telephone != ''
            """)
            phone_count = self.cursor.fetchone()[0]
            
            # Check schools with head teacher info
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM uk_schools 
                WHERE head_first_name IS NOT NULL OR head_last_name IS NOT NULL
            """)
            head_count = self.cursor.fetchone()[0]
            
            # Sample some updated records
            self.cursor.execute("""
                SELECT urn, name, telephone, website, 
                       head_title, head_first_name, head_last_name, head_job_title
                FROM uk_schools
                WHERE telephone IS NOT NULL OR website IS NOT NULL
                LIMIT 5
            """)
            
            samples = self.cursor.fetchall()
            
            logging.info(f"\n📊 Database Statistics:")
            logging.info(f"  Schools with websites: {website_count}")
            logging.info(f"  Schools with phone numbers: {phone_count}")
            logging.info(f"  Schools with head teacher info: {head_count}")
            
            logging.info(f"\n📝 Sample Updated Records:")
            for sample in samples:
                logging.info(f"  URN {sample[0]}: {sample[1]}")
                if sample[2]:
                    logging.info(f"    Phone: {sample[2]}")
                if sample[3]:
                    logging.info(f"    Website: {sample[3][:50]}...")
                if sample[4] or sample[5] or sample[6]:
                    head_name = ' '.join(filter(None, [sample[4], sample[5], sample[6]]))
                    logging.info(f"    Head: {head_name}")
                if sample[7]:
                    logging.info(f"    Title: {sample[7]}")
                logging.info("")
            
        except Exception as e:
            logging.error(f"❌ Error during verification: {e}")
    
    def generate_summary_report(self):
        """Generate a summary of the update process."""
        logging.info("\n" + "=" * 60)
        logging.info("UPDATE SUMMARY REPORT")
        logging.info("=" * 60)
        logging.info(f"Total records in CSV: {self.stats['total_records']}")
        logging.info(f"Schools matched in database: {self.stats['matched_schools']}")
        logging.info(f"Schools updated: {self.stats['updated_records']}")
        logging.info(f"Failed updates: {self.stats['failed_updates']}")
        logging.info(f"Schools with phone in CSV: {self.stats['schools_with_phone']}")
        logging.info(f"Schools with website in CSV: {self.stats['schools_with_website']}")
        logging.info(f"Schools with head info in CSV: {self.stats['schools_with_head']}")
        logging.info("=" * 60)
    
    def run(self):
        """Main execution method."""
        try:
            # Connect to database
            if not self.connect_db():
                return
            
            # Load CSV data
            df = self.load_csv_data()
            if df is None:
                return
            
            self.stats['total_records'] = len(df)
            
            # Update schools
            self.update_schools(df)
            
            # Verify updates
            self.verify_updates()
            
            # Generate summary report
            self.generate_summary_report()
            
        except Exception as e:
            logging.error(f"❌ Process failed: {e}")
            
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logging.info("\n✅ Database connection closed")


def main():
    """Main function to run the update process."""
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    
    # CSV file path - update this to your actual file path
    CSV_FILE = "edubaseallstatefunded20250825.csv"
    
    # Check if file exists
    if not Path(CSV_FILE).exists():
        logging.error(f"❌ CSV file not found: {CSV_FILE}")
        logging.info("Please ensure the file is in the same directory as this script")
        logging.info("Or provide the full path to the file")
        return
    
    logging.info("=" * 60)
    logging.info("UK SCHOOLS CONTACT INFORMATION UPDATER")
    logging.info("=" * 60)
    logging.info(f"Database: schoolsdb")
    logging.info(f"CSV File: {CSV_FILE}")
    logging.info("=" * 60)
    
    # Create and run updater
    updater = EdubaseUpdater(DATABASE_URL, CSV_FILE)
    updater.run()


if __name__ == "__main__":
    main()