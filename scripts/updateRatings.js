// updateAllRatings.js
// Run this script to update all school ratings in the database
// Usage: node updateAllRatings.js

const { query } = require('./src/config/database');

async function updateAllRatings() {
  console.log('Starting comprehensive rating update...');
  
  try {
    // Step 1: Create or replace the calculation function
    console.log('Creating rating calculation function...');
    await query(`
      CREATE OR REPLACE FUNCTION calculate_all_school_ratings()
      RETURNS TABLE (
        urn INTEGER,
        rating INTEGER,
        components JSONB,
        schools_processed INTEGER
      ) AS $$
      DECLARE
        school_record RECORD;
        la_averages RECORD;
        total_weight INTEGER;
        weighted_sum DECIMAL(5,2);
        final_rating INTEGER;
        rating_components JSONB;
        processed_count INTEGER := 0;
      BEGIN
        FOR school_record IN 
          SELECT s.urn, s.local_authority, s.phase_of_education,
                 s.english_score, s.math_score, s.science_score,
                 COALESCE(o.overall_effectiveness, o.quality_of_education) as ofsted_rating,
                 a.overall_absence_rate
          FROM uk_schools s
          LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
          LEFT JOIN uk_absence_data a ON s.urn = a.urn
        LOOP
          -- Get LA averages
          SELECT 
            AVG(s2.english_score) as avg_english,
            AVG(s2.math_score) as avg_math,
            AVG(s2.science_score) as avg_science
          INTO la_averages
          FROM uk_schools s2
          WHERE s2.local_authority = school_record.local_authority 
            AND s2.phase_of_education = school_record.phase_of_education
            AND s2.urn != school_record.urn;
          
          -- Initialize
          rating_components = '[]'::jsonb;
          total_weight = 0;
          weighted_sum = 0;
          
          -- Ofsted component (40%)
          IF school_record.ofsted_rating IS NOT NULL THEN
            DECLARE
              ofsted_score DECIMAL(3,1);
            BEGIN
              ofsted_score = CASE school_record.ofsted_rating
                WHEN 1 THEN 9.5
                WHEN 2 THEN 7.5
                WHEN 3 THEN 4.5
                WHEN 4 THEN 2.5
                ELSE 5.0
              END;
              
              rating_components = rating_components || jsonb_build_object(
                'name', 'ofsted',
                'score', ofsted_score,
                'weight', 40
              );
              
              weighted_sum = weighted_sum + (ofsted_score * 40);
              total_weight = total_weight + 40;
            END;
          END IF;
          
          -- Academic component (40%)
          DECLARE
            academic_sum DECIMAL(5,2) := 0;
            academic_count INTEGER := 0;
          BEGIN
            IF school_record.english_score IS NOT NULL AND la_averages.avg_english IS NOT NULL THEN
              academic_sum = academic_sum + LEAST(10, GREATEST(1, 5 + (school_record.english_score - la_averages.avg_english) / 4));
              academic_count = academic_count + 1;
            END IF;
            
            IF school_record.math_score IS NOT NULL AND la_averages.avg_math IS NOT NULL THEN
              academic_sum = academic_sum + LEAST(10, GREATEST(1, 5 + (school_record.math_score - la_averages.avg_math) / 4));
              academic_count = academic_count + 1;
            END IF;
            
            IF school_record.science_score IS NOT NULL AND la_averages.avg_science IS NOT NULL THEN
              academic_sum = academic_sum + LEAST(10, GREATEST(1, 5 + (school_record.science_score - la_averages.avg_science) / 4));
              academic_count = academic_count + 1;
            END IF;
            
            IF academic_count > 0 THEN
              DECLARE
                academic_avg DECIMAL(3,1);
              BEGIN
                academic_avg = academic_sum / academic_count;
                rating_components = rating_components || jsonb_build_object(
                  'name', 'academic',
                  'score', academic_avg,
                  'weight', 40
                );
                weighted_sum = weighted_sum + (academic_avg * 40);
                total_weight = total_weight + 40;
              END;
            END IF;
          END;
          
          -- Attendance component (20%)
          IF school_record.overall_absence_rate IS NOT NULL THEN
            DECLARE
              attendance_rate DECIMAL(5,2);
              attendance_score DECIMAL(3,1);
            BEGIN
              attendance_rate = 100 - school_record.overall_absence_rate;
              
              attendance_score = CASE
                WHEN attendance_rate >= 97 THEN 9.5
                WHEN attendance_rate >= 96 THEN 8.5
                WHEN attendance_rate >= 95 THEN 7.5
                WHEN attendance_rate >= 93 THEN 6.0
                WHEN attendance_rate >= 90 THEN 4.5
                WHEN attendance_rate >= 85 THEN 3.0
                ELSE 2.0
              END;
              
              rating_components = rating_components || jsonb_build_object(
                'name', 'attendance',
                'score', attendance_score,
                'weight', 20
              );
              
              weighted_sum = weighted_sum + (attendance_score * 20);
              total_weight = total_weight + 20;
            END;
          END IF;
          
          -- Calculate final rating
          IF total_weight >= 40 THEN
            -- Use standard rounding to get integer
            final_rating = ROUND(weighted_sum / total_weight);
            
            UPDATE uk_schools 
            SET overall_rating = final_rating,
                rating_components = rating_components,
                rating_updated_at = NOW()
            WHERE uk_schools.urn = school_record.urn;
            
            processed_count = processed_count + 1;
          END IF;
        END LOOP;
        
        RETURN QUERY SELECT 0, 0, '{}'::jsonb, processed_count;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // Step 2: Execute the rating calculation
    console.log('Calculating ratings for all schools...');
    const result = await query('SELECT * FROM calculate_all_school_ratings()');
    console.log(`Processed ${result.rows[0].schools_processed} schools`);

    // Step 3: Update percentiles
    console.log('Updating percentile rankings...');
    await query(`
      WITH percentile_calc AS (
        SELECT 
          urn,
          overall_rating,
          PERCENT_RANK() OVER (
            PARTITION BY local_authority, phase_of_education 
            ORDER BY overall_rating ASC NULLS FIRST
          ) * 100 as percentile_rank
        FROM uk_schools
        WHERE overall_rating IS NOT NULL
      )
      UPDATE uk_schools s
      SET rating_percentile = GREATEST(1, ROUND(pc.percentile_rank))
      FROM percentile_calc pc
      WHERE s.urn = pc.urn
    `);

    // Step 4: Get statistics
    console.log('Generating statistics...');
    const stats = await query(`
      SELECT 
        COUNT(*) as total_schools,
        COUNT(overall_rating) as schools_with_rating,
        ROUND(AVG(overall_rating), 1) as avg_rating,
        MIN(overall_rating) as min_rating,
        MAX(overall_rating) as max_rating,
        COUNT(*) FILTER (WHERE overall_rating >= 8) as excellent_schools,
        COUNT(*) FILTER (WHERE overall_rating >= 6 AND overall_rating < 8) as good_schools,
        COUNT(*) FILTER (WHERE overall_rating >= 4 AND overall_rating < 6) as average_schools,
        COUNT(*) FILTER (WHERE overall_rating < 4) as below_average_schools
      FROM uk_schools
    `);

    console.log('\n=== Update Complete ===');
    console.log('Statistics:');
    console.log(`Total schools: ${stats.rows[0].total_schools}`);
    console.log(`Schools with ratings: ${stats.rows[0].schools_with_rating}`);
    console.log(`Average rating: ${stats.rows[0].avg_rating}`);
    console.log(`Rating range: ${stats.rows[0].min_rating} - ${stats.rows[0].max_rating}`);
    console.log(`\nDistribution:`);
    console.log(`Excellent (8-10): ${stats.rows[0].excellent_schools} schools`);
    console.log(`Good (6-7): ${stats.rows[0].good_schools} schools`);
    console.log(`Average (4-5): ${stats.rows[0].average_schools} schools`);
    console.log(`Below Average (1-3): ${stats.rows[0].below_average_schools} schools`);

    // Step 5: Clean up
    await query('DROP FUNCTION IF EXISTS calculate_all_school_ratings()');
    console.log('\nCleanup complete');

  } catch (error) {
    console.error('Error updating ratings:', error);
    process.exit(1);
  }

  process.exit(0);
}

// Run the update
updateAllRatings();