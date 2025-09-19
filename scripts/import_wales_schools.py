#!/usr/bin/env python3
"""
Wales Schools Data Importer
Updates uk_schools table with Wales schools data from CSV

Usage:
  python import_wales_schools.py --csv wales_schools.csv
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
import sys
from pathlib import Path
import json
import argparse
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'wales_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class WalesSchoolsUpdater:
    def __init__(self, db_url: str, csv_file_path: str, update_only: bool = False):
        """
        Initialize the updater with database connection and CSV file path.
        
        Args:
            db_url: PostgreSQL connection URL
            csv_file_path: Path to the Wales schools CSV file
            update_only: If True, only update existing records
        """
        self.db_url = db_url
        self.csv_file_path = csv_file_path
        self.update_only = update_only
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_records': 0,
            'matched_schools': 0,
            'inserted_schools': 0,
            'updated_records': 0,
            'failed_updates': 0,
            'schools_with_fsm': 0,
            'schools_with_scores': 0,
            'schools_with_wales_metrics': 0
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
                dtype={'School Number': str}  # Keep as string initially
            )
            
            logging.info(f"✅ Loaded {len(df)} records from CSV")
            
            # Clean column names (remove any spaces)
            df.columns = df.columns.str.strip()
            
            # Display available columns
            logging.info(f"Available columns: {list(df.columns)}")
            
            return df
            
        except UnicodeDecodeError:
            # Try with latin-1 encoding if utf-8 fails
            logging.info("Trying latin-1 encoding...")
            df = pd.read_csv(
                self.csv_file_path, 
                encoding='latin-1',
                dtype={'School Number': str}
            )
            df.columns = df.columns.str.strip()
            return df
            
        except Exception as e:
            logging.error(f"❌ Failed to load CSV: {e}")
            return None
    
    def clean_numeric(self, value):
        """Clean numeric values."""
        if pd.isna(value) or value == '' or value == 'Not applicable':
            return None
        try:
            return float(value)
        except:
            return None
    
    def clean_integer(self, value):
        """Clean integer values."""
        if pd.isna(value) or value == '' or value == 'Not applicable':
            return None
        try:
            return int(float(value))
        except:
            return None
    
    def clean_text_field(self, text, max_length=None):
        """Clean text fields."""
        if pd.isna(text) or text == '' or text == 'Not applicable':
            return None
        
        text = str(text).strip()
        
        # Truncate if max_length specified
        if text and max_length and len(text) > max_length:
            text = text[:max_length]
        
        return text if text else None
    
    def generate_slug(self, name):
        """Generate a URL-friendly slug from school name."""
        if not name:
            return None
        # Convert to lowercase
        slug = name.lower()
        # Replace spaces and special characters with hyphens
        slug = re.sub(r'[^a-z0-9]+', '-', slug)
        # Remove leading/trailing hyphens
        slug = slug.strip('-')
        # Limit length to 255 characters
        if len(slug) > 255:
            slug = slug[:255].rsplit('-', 1)[0]
        return slug
    
    def process_schools(self, df):
        """Process and update/insert schools."""
        try:
            logging.info("Starting school processing...")
            
            # Process in batches
            batch_size = 100
            total_processed = 0
            
            for start_idx in range(0, len(df), batch_size):
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]
                
                for idx, row in batch_df.iterrows():
                    try:
                        # Get URN (School Number)
                        urn = self.clean_integer(row.get('School Number'))
                        if not urn:
                            logging.warning(f"Row {idx}: No valid School Number/URN")
                            continue
                        
                        # Prepare school data - INCLUDING establishment_status
                        school_data = {
                            'urn': urn,
                            'la_code': self.clean_integer(row.get('LA Code')),
                            'name': self.clean_text_field(row.get('School Name'), 255),
                            'local_authority': self.clean_text_field(row.get('Local Authority'), 150),
                            'establishment_status': 'Open',  # Set default status for all Wales schools
                            'type_of_establishment': self.clean_text_field(row.get('School Type'), 100),
                            'religious_character': self.clean_text_field(row.get('Religious Character'), 100),
                            'street': self.clean_text_field(row.get('Address 1'), 255),
                            'locality': self.clean_text_field(row.get('Address 2'), 255),
                            'town': self.clean_text_field(row.get('Address 3'), 100),
                            'county': self.clean_text_field(row.get('Address 4'), 100),
                            'postcode': self.clean_text_field(row.get('Postcode'), 10),
                            'telephone': self.clean_text_field(row.get('Phone Number'), 50),
                            'total_pupils': self.clean_integer(row.get('Pupils')),
                            'percentage_fsm': self.clean_numeric(row.get('fsm_3yr_pct')),
                            'english_score': self.clean_numeric(row.get('literacy_points')),
                            'math_score': self.clean_numeric(row.get('numeracy_points')),
                            'science_score': self.clean_numeric(row.get('science_points'))
                        }
                        
                        # Track statistics
                        if school_data['percentage_fsm']:
                            self.stats['schools_with_fsm'] += 1
                        if any([school_data['english_score'], school_data['math_score'], school_data['science_score']]):
                            self.stats['schools_with_scores'] += 1
                        
                        # Collect Wales-specific metrics
                        wales_metrics = {}
                        if 'pupil_teacher_ratio' in row and pd.notna(row['pupil_teacher_ratio']):
                            wales_metrics['pupil_teacher_ratio'] = self.clean_numeric(row['pupil_teacher_ratio'])
                        if 'attendance_pct' in row and pd.notna(row['attendance_pct']):
                            wales_metrics['attendance_pct'] = self.clean_numeric(row['attendance_pct'])
                        if 'school_budget_per_pupil' in row and pd.notna(row['school_budget_per_pupil']):
                            wales_metrics['school_budget_per_pupil'] = self.clean_numeric(row['school_budget_per_pupil'])
                        if 'capped9_points' in row and pd.notna(row['capped9_points']):
                            wales_metrics['capped9_points'] = self.clean_numeric(row['capped9_points'])
                        if 'welsh_bacc_points' in row and pd.notna(row['welsh_bacc_points']):
                            wales_metrics['welsh_bacc_points'] = self.clean_numeric(row['welsh_bacc_points'])
                        if 'estyn_report_url' in row and pd.notna(row['estyn_report_url']):
                            wales_metrics['estyn_report_url'] = self.clean_text_field(row['estyn_report_url'])
                        
                        if wales_metrics:
                            self.stats['schools_with_wales_metrics'] += 1
                        
                        # Check if school exists
                        self.cursor.execute("SELECT id FROM uk_schools WHERE urn = %s", (urn,))
                        existing = self.cursor.fetchone()
                        
                        if existing:
                            # Update existing record
                            self.update_school(urn, school_data, wales_metrics)
                        elif not self.update_only:
                            # Insert new record
                            self.insert_school(school_data, wales_metrics)
                        
                        # Commit after each successful operation
                        self.conn.commit()
                        
                    except Exception as e:
                        # Rollback only the failed transaction
                        self.conn.rollback()
                        logging.error(f"Error processing URN {row.get('School Number')}: {e}")
                        self.stats['failed_updates'] += 1
                        continue
                
                total_processed += len(batch_df)
                logging.info(f"Processed {total_processed}/{len(df)} records...")
            
            logging.info(f"✅ Processing complete")
            
        except Exception as e:
            logging.error(f"❌ Error during processing: {e}")
            self.conn.rollback()
    
    def update_school(self, urn, school_data, wales_metrics):
        """Update an existing school record."""
        try:
            # Build update query dynamically
            update_fields = []
            values = []
            
            # Add non-null fields to update, INCLUDING establishment_status
            for field, value in school_data.items():
                if field != 'urn' and value is not None:
                    update_fields.append(f"{field} = COALESCE(%s, {field})")
                    values.append(value)
            
            # Ensure establishment_status is updated
            if 'establishment_status' not in [f.split(' = ')[0] for f in update_fields]:
                update_fields.append("establishment_status = %s")
                values.append('Open')
            
            # Add name_lower if name exists
            if school_data.get('name'):
                update_fields.append("name_lower = %s")
                values.append(school_data['name'].lower())
            
            # Add country
            update_fields.append("country = %s")
            values.append('wales')
            
            # Handle wales_metrics in rating_components
            if wales_metrics:
                update_fields.append("""
                    rating_components = 
                    CASE 
                        WHEN rating_components IS NULL THEN %s::jsonb
                        ELSE rating_components || %s::jsonb
                    END
                """)
                wales_json = json.dumps({'wales_metrics': wales_metrics})
                values.extend([wales_json, wales_json])
            
            # Add timestamp
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            
            # Add URN at the end for WHERE clause
            values.append(urn)
            
            query = f"""
                UPDATE uk_schools 
                SET {', '.join(update_fields)}
                WHERE urn = %s
            """
            
            self.cursor.execute(query, values)
            
            if self.cursor.rowcount > 0:
                self.stats['updated_records'] += 1
                self.stats['matched_schools'] += 1
                
        except Exception as e:
            logging.error(f"Error updating URN {urn}: {e}")
            self.stats['failed_updates'] += 1
    
    def insert_school(self, school_data, wales_metrics):
        """Insert a new school record."""
        try:
            # Start with the school_data which already includes establishment_status
            insert_data = {k: v for k, v in school_data.items() if v is not None}
            
            # ENSURE establishment_status is set (double-check)
            if 'establishment_status' not in insert_data or insert_data['establishment_status'] is None:
                insert_data['establishment_status'] = 'Open'
            
            # Add country
            insert_data['country'] = 'wales'
            
            # Add name_lower and slug if name exists
            if insert_data.get('name'):
                insert_data['name_lower'] = insert_data['name'].lower()
                slug = self.generate_slug(insert_data['name'])
                if slug:
                    insert_data['slug'] = slug
                else:
                    insert_data['slug'] = f"school-{insert_data['urn']}"
            else:
                insert_data['slug'] = f"school-{insert_data.get('urn', 'unknown')}"
            
            # Add wales_metrics if any
            if wales_metrics:
                insert_data['rating_components'] = json.dumps({'wales_metrics': wales_metrics})
            
            # Final check for required fields
            if 'slug' not in insert_data:
                insert_data['slug'] = f"school-{insert_data.get('urn', 'unknown')}"
            
            # Log what we're about to insert for debugging
            logging.debug(f"Inserting URN {insert_data.get('urn')} with establishment_status: {insert_data.get('establishment_status')}")
            
            # Build insert query
            columns = list(insert_data.keys())
            placeholders = ['%s'] * len(columns)
            values = list(insert_data.values())
            
            query = f"""
                INSERT INTO uk_schools ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (urn) DO UPDATE SET
                    name = EXCLUDED.name,
                    local_authority = EXCLUDED.local_authority,
                    type_of_establishment = EXCLUDED.type_of_establishment,
                    establishment_status = EXCLUDED.establishment_status,
                    percentage_fsm = EXCLUDED.percentage_fsm,
                    english_score = EXCLUDED.english_score,
                    math_score = EXCLUDED.math_score,
                    science_score = EXCLUDED.science_score,
                    country = EXCLUDED.country,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            self.cursor.execute(query, values)
            if self.cursor.rowcount > 0:
                self.stats['inserted_schools'] += 1
            
        except Exception as e:
            logging.error(f"Error inserting school URN {school_data.get('urn')}: {e}")
            if 'insert_data' in locals():
                logging.debug(f"Insert data had establishment_status: {insert_data.get('establishment_status')}")
            self.stats['failed_updates'] += 1
    
    def verify_updates(self):
        """Verify the updates by checking database statistics."""
        try:
            logging.info("\nVerifying updates...")
            
            # Get Wales schools statistics
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(percentage_fsm) as with_fsm,
                    COUNT(english_score) as with_english,
                    COUNT(math_score) as with_math,
                    COUNT(science_score) as with_science,
                    COUNT(total_pupils) as with_pupils,
                    AVG(percentage_fsm) as avg_fsm,
                    AVG(english_score) as avg_english,
                    AVG(math_score) as avg_math,
                    AVG(science_score) as avg_science
                FROM uk_schools
                WHERE country = 'wales'
            """)
            
            stats = self.cursor.fetchone()
            
            logging.info(f"\n📊 Wales Schools Database Statistics:")
            logging.info(f"  Total Wales schools: {stats[0]}")
            logging.info(f"  Schools with FSM data: {stats[1]}")
            logging.info(f"  Schools with English scores: {stats[2]}")
            logging.info(f"  Schools with Math scores: {stats[3]}")
            logging.info(f"  Schools with Science scores: {stats[4]}")
            logging.info(f"  Schools with pupil counts: {stats[5]}")
            
            if stats[6]:
                logging.info(f"\n📈 Average Scores:")
                logging.info(f"  FSM %: {stats[6]:.2f}")
                logging.info(f"  English: {stats[7]:.2f}" if stats[7] else "  English: N/A")
                logging.info(f"  Math: {stats[8]:.2f}" if stats[8] else "  Math: N/A")
                logging.info(f"  Science: {stats[9]:.2f}" if stats[9] else "  Science: N/A")
            
            # Sample some records
            self.cursor.execute("""
                SELECT urn, name, local_authority, percentage_fsm, 
                       english_score, math_score, science_score
                FROM uk_schools
                WHERE country = 'wales' 
                  AND (english_score IS NOT NULL OR math_score IS NOT NULL)
                LIMIT 5
            """)
            
            samples = self.cursor.fetchall()
            
            logging.info(f"\n📝 Sample Wales Schools:")
            for sample in samples:
                logging.info(f"  URN {sample[0]}: {sample[1]} ({sample[2]})")
                if sample[3]:
                    logging.info(f"    FSM: {sample[3]:.1f}%")
                scores = []
                if sample[4]:
                    scores.append(f"Eng: {sample[4]:.1f}")
                if sample[5]:
                    scores.append(f"Math: {sample[5]:.1f}")
                if sample[6]:
                    scores.append(f"Sci: {sample[6]:.1f}")
                if scores:
                    logging.info(f"    Scores: {', '.join(scores)}")
                logging.info("")
            
        except Exception as e:
            logging.error(f"❌ Error during verification: {e}")
    
    def generate_summary_report(self):
        """Generate a summary of the update process."""
        logging.info("\n" + "=" * 60)
        logging.info("WALES SCHOOLS UPDATE SUMMARY REPORT")
        logging.info("=" * 60)
        logging.info(f"Total records in CSV: {self.stats['total_records']}")
        logging.info(f"Schools matched in database: {self.stats['matched_schools']}")
        logging.info(f"Schools inserted: {self.stats['inserted_schools']}")
        logging.info(f"Schools updated: {self.stats['updated_records']}")
        logging.info(f"Failed operations: {self.stats['failed_updates']}")
        logging.info(f"Schools with FSM data: {self.stats['schools_with_fsm']}")
        logging.info(f"Schools with academic scores: {self.stats['schools_with_scores']}")
        logging.info(f"Schools with Wales metrics: {self.stats['schools_with_wales_metrics']}")
        
        success_rate = ((self.stats['updated_records'] + self.stats['inserted_schools']) / 
                       self.stats['total_records'] * 100) if self.stats['total_records'] > 0 else 0
        logging.info(f"Success rate: {success_rate:.1f}%")
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
            
            # Process schools
            self.process_schools(df)
            
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
    
    parser = argparse.ArgumentParser(description="Import Wales schools data to uk_schools table")
    parser.add_argument("--csv", required=True, help="Path to CSV file with Wales schools data")
    parser.add_argument("--update-only", action="store_true", 
                       help="Only update existing records, don't insert new ones")
    
    args = parser.parse_args()
    
    # Configuration
    DATABASE_URL = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
    
    # Check if file exists
    if not Path(args.csv).exists():
        logging.error(f"❌ CSV file not found: {args.csv}")
        return
    
    logging.info("=" * 60)
    logging.info("WALES SCHOOLS DATA IMPORTER")
    logging.info("=" * 60)
    logging.info(f"Database: schoolsdb")
    logging.info(f"CSV File: {args.csv}")
    logging.info(f"Mode: {'Update only' if args.update_only else 'Update and insert'}")
    logging.info("=" * 60)
    
    # Create and run updater
    updater = WalesSchoolsUpdater(DATABASE_URL, args.csv, args.update_only)
    updater.run()


if __name__ == "__main__":
    main()