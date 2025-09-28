const express = require('express');
const router = express.Router();
const { query } = require('../config/database');

/* ---------------- helpers ---------------- */
function getOfstedLabel(rating) {
  const labels = { 1: 'Outstanding', 2: 'Good', 3: 'Requires Improvement', 4: 'Inadequate' };
  return labels[rating] || 'Not Inspected';
}

const toNum = v => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

function calculateRatingWithFallbacks(school, laAverages) {
  let components = [];
  let totalWeight = 0;
  
  // Check country (case-insensitive)
  const country = (school.country || 'england').toLowerCase();
  const isWales = country === 'wales';
  const isScotland = country === 'scotland';
  
  if (isWales) {
    // Wales rating: No Ofsted, Academic (80%) and Attendance (20%)
    
    // Academic component (80% weight for Wales)
    let academicScores = [];
    let academicDetails = {};
    
    if (school.english_score !== null && laAverages.avg_english !== null) {
      const engScore = calculateAcademicScore(school.english_score, laAverages.avg_english);
      academicScores.push(engScore);
      academicDetails.english = {
        school: school.english_score,
        la_avg: laAverages.avg_english,
        score: engScore
      };
    }
    
    if (school.math_score !== null && laAverages.avg_math !== null) {
      const mathScore = calculateAcademicScore(school.math_score, laAverages.avg_math);
      academicScores.push(mathScore);
      academicDetails.math = {
        school: school.math_score,
        la_avg: laAverages.avg_math,
        score: mathScore
      };
    }
    
    if (school.science_score !== null && laAverages.avg_science !== null) {
      const sciScore = calculateAcademicScore(school.science_score, laAverages.avg_science);
      academicScores.push(sciScore);
      academicDetails.science = {
        school: school.science_score,
        la_avg: laAverages.avg_science,
        score: sciScore
      };
    }
    
    if (academicScores.length > 0) {
      const avgAcademicScore = academicScores.reduce((a, b) => a + b, 0) / academicScores.length;
      components.push({
        name: 'academic',
        score: avgAcademicScore,
        weight: 80,
        details: academicDetails,
        subjects_available: academicScores.length
      });
      totalWeight += 80;
    }
    
    // Attendance component (20% weight for Wales)
    if (school.attendance_rate !== null) {
      const attendanceScore = calculateAttendanceScore(school.attendance_rate);
      components.push({
        name: 'attendance',
        score: attendanceScore,
        weight: 20,
        school_rate: school.attendance_rate,
        la_avg: laAverages.avg_attendance
      });
      totalWeight += 20;
    }
    
  } else if (isScotland) {
    // Scotland rating: No Ofsted, Academic (60%) and Attendance (40%)
    
    // Academic component (60% weight for Scotland)
    let academicScores = [];
    let academicDetails = {};
    
    // Only English and Math for Scotland
    if (school.english_score !== null && laAverages.avg_english !== null) {
      const engScore = calculateAcademicScore(school.english_score, laAverages.avg_english);
      academicScores.push(engScore);
      academicDetails.english = {
        school: school.english_score,
        la_avg: laAverages.avg_english,
        score: engScore
      };
    }
    
    if (school.math_score !== null && laAverages.avg_math !== null) {
      const mathScore = calculateAcademicScore(school.math_score, laAverages.avg_math);
      academicScores.push(mathScore);
      academicDetails.math = {
        school: school.math_score,
        la_avg: laAverages.avg_math,
        score: mathScore
      };
    }
    
    if (academicScores.length > 0) {
      const avgAcademicScore = academicScores.reduce((a, b) => a + b, 0) / academicScores.length;
      components.push({
        name: 'academic',
        score: avgAcademicScore,
        weight: 60,
        details: academicDetails,
        subjects_available: academicScores.length
      });
      totalWeight += 60;
    }
    
    // Attendance component (40% weight for Scotland)
    if (school.attendance_rate !== null) {
      const attendanceScore = calculateAttendanceScore(school.attendance_rate);
      components.push({
        name: 'attendance',
        score: attendanceScore,
        weight: 40,
        school_rate: school.attendance_rate,
        la_avg: laAverages.avg_attendance
      });
      totalWeight += 40;
    }
    
  } else {
    // England/Wales/NI rating: Original system with Ofsted
    
    // 1. Ofsted component (40% weight)
    if (school.ofsted_overall_effectiveness) {
      const ofstedMap = { 
        1: 9.5,  // Outstanding
        2: 7.5,  // Good
        3: 4.5,  // Requires Improvement
        4: 2.5   // Inadequate
      };
      const ofstedScore = ofstedMap[school.ofsted_overall_effectiveness] || 5;
      components.push({
        name: 'ofsted',
        score: ofstedScore,
        weight: 40,
        label: getOfstedLabel(school.ofsted_overall_effectiveness)
      });
      totalWeight += 40;
    }
    
    // 2. Academic component (40% weight)
    let academicScores = [];
    let academicDetails = {};
    
    if (school.english_score !== null && laAverages.avg_english !== null) {
      const engScore = calculateAcademicScore(school.english_score, laAverages.avg_english);
      academicScores.push(engScore);
      academicDetails.english = {
        school: school.english_score,
        la_avg: laAverages.avg_english,
        score: engScore
      };
    }
    
    if (school.math_score !== null && laAverages.avg_math !== null) {
      const mathScore = calculateAcademicScore(school.math_score, laAverages.avg_math);
      academicScores.push(mathScore);
      academicDetails.math = {
        school: school.math_score,
        la_avg: laAverages.avg_math,
        score: mathScore
      };
    }
    
    // Include Science for non-Scotland schools
    if (school.science_score !== null && laAverages.avg_science !== null) {
      const sciScore = calculateAcademicScore(school.science_score, laAverages.avg_science);
      academicScores.push(sciScore);
      academicDetails.science = {
        school: school.science_score,
        la_avg: laAverages.avg_science,
        score: sciScore
      };
    }
    
    if (academicScores.length > 0) {
      const avgAcademicScore = academicScores.reduce((a, b) => a + b, 0) / academicScores.length;
      components.push({
        name: 'academic',
        score: avgAcademicScore,
        weight: 40,
        details: academicDetails,
        subjects_available: academicScores.length
      });
      totalWeight += 40;
    }
    
    // 3. Attendance component (20% weight)
    if (school.attendance_rate !== null) {
      const attendanceScore = calculateAttendanceScore(school.attendance_rate);
      components.push({
        name: 'attendance',
        score: attendanceScore,
        weight: 20,
        school_rate: school.attendance_rate,
        la_avg: laAverages.avg_attendance
      });
      totalWeight += 20;
    }
  }
  
  // Check minimum data threshold
  const minThreshold = isWales ? 20 : (isScotland ? 50 : 40);
  if (totalWeight < minThreshold) {
    return {
      rating: null,
      message: "Insufficient data for rating",
      available_components: components,
      data_completeness: totalWeight,
      is_wales: isWales,
      is_scotland: isScotland
    };
  }
  
  // Calculate normalized score
  const normalizedScore = components.reduce((sum, c) => 
    sum + (c.score * (c.weight / totalWeight)), 0
  );
  
  // Calculate percentile
  const percentile = calculatePercentile(normalizedScore, laAverages.all_ratings || []);
  
  // Standard rounding
  const roundedRating = Math.round(normalizedScore);
  
  return {
    rating: roundedRating,
    components: components,
    data_completeness: totalWeight,
    percentile: percentile,
    la_comparison: totalWeight === 100 ? 'Complete data' : 'Partial data',
    is_wales: isWales,
    is_scotland: isScotland
  };
}

function calculateAcademicScore(schoolValue, laAverage) {
  if (schoolValue === null || schoolValue === undefined) return 5;
  if (laAverage === null || laAverage === undefined) return 5;
  
  // Calculate percentage point difference
  const difference = schoolValue - laAverage;
  
  // Scale: Each 4 percentage points difference = 1 rating point
  // +20 points above LA = 10/10, -20 below = 0/10
  let score = 5 + (difference / 4);
  
  // Cap between 1 and 10
  return Math.max(1, Math.min(10, score));
}

function calculateAttendanceScore(attendanceRate) {
  if (!attendanceRate) return 5;
  
  // Linear progression from 80% to 100%
  // 80% or below = 1
  // 100% = 10
  // Linear scale in between
  
  if (attendanceRate <= 80) {
    return 1;
  }
  
  if (attendanceRate >= 100) {
    return 10;
  }
  
  // Linear calculation:
  // For every 1% increase from 80%, add 0.45 points
  // Formula: score = 1 + ((attendanceRate - 80) * 9 / 20)
  // This gives us: 80%=1, 85%=3.25, 90%=5.5, 95%=7.75, 100%=10
  
  const score = 1 + ((attendanceRate - 80) * 9 / 20);
  
  // Round to 1 decimal place for consistency
  return Math.round(score * 10) / 10;
}

function calculatePercentile(score, allScores) {
  if (!allScores || allScores.length === 0) return null;
  if (allScores.length === 1) return 50; // Default to 50th percentile if only one school
  
  // Filter out null values and count how many schools have lower scores
  const validScores = allScores.filter(s => s !== null);
  const below = validScores.filter(s => s < score).length;
  const percentile = Math.round((below / validScores.length) * 100);
  
  // Return null if percentile is 0 (might indicate data issue)
  return percentile || null;
}

/* =======================================================================
 * GET /api/schools/:urn
 * Returns a robust object with calculated rating
 * ======================================================================= */
router.get('/:urn', async (req, res) => {
  try {
    const { urn } = req.params;
    const debug = req.query.debug === '1';

    // -------------------------------------------------------------------
    // FR branch: handle French UAI identifiers (alphanumeric like 0341711A)
    // -------------------------------------------------------------------
    if (!/^\d+$/.test(String(urn))) {
      const uai = String(urn).trim();
      if (!uai) return res.status(400).json({ error: 'Invalid UAI provided' });

      // Fetch base school data from French table
      const frSql = `
        SELECT 
          e.identifiant_de_l_etablissement AS uai,
          e.nom_etablissement AS name,
          e.nom_commune AS commune,
          e.code_postal,
          e.type_etablissement,
          e.statut_public_prive,
          NULL::text AS type_contrat_prive,
          e.libelle_departement AS departement,
          e.libelle_academie AS academie,
          e.libelle_region AS region,
          e.adresse_1,
          e.adresse_2,
          e.adresse_3,
          e.telephone,
          e.web,
          e.mail,
          e.nombre_d_eleves AS nombre_d_eleves,
          e.latitude AS latitude,
          e.longitude AS longitude,
          e.students_total,
          e.lycee_students_total,
          e.lycee_effectifs_seconde,
          e.lycee_effectifs_premiere,
          e.lycee_effectifs_terminale,
          e.lycee_bac_candidates,
          e.lycee_bac_success_rate,
          e.lycee_mentions_rate,
          e.college_dnb_candidates,
          e.college_dnb_success_rate
        FROM fr_ecoles e
        WHERE e.identifiant_de_l_etablissement = $1
        LIMIT 1
      `;
      const statsSql = `
        SELECT avg_overall_rating, total_reviews, recommendation_percentage
        FROM fr_school_review_stats
        WHERE uai = $1
        LIMIT 1
      `;

      let frR;
      try {
        frR = await query(frSql, [uai]);
      } catch (e) {
        if (e.code === '42703') {
          // Columns not present yet; retry with a minimal set
          const fallbackSql = `
            SELECT 
              e.identifiant_de_l_etablissement AS uai,
              e.nom_etablissement AS name,
              e.nom_commune AS commune,
              e.code_postal,
              e.type_etablissement,
              e.statut_public_prive,
              NULL::text AS type_contrat_prive,
              e.libelle_departement AS departement,
              e.libelle_academie AS academie,
              e.libelle_region AS region,
              e.adresse_1,
              e.adresse_2,
              e.adresse_3,
              e.telephone,
              e.web,
              e.mail,
              e.nombre_d_eleves AS nombre_d_eleves,
              e.latitude AS latitude,
              e.longitude AS longitude
            FROM fr_ecoles e
            WHERE e.identifiant_de_l_etablissement = $1
            LIMIT 1`;
          frR = await query(fallbackSql, [uai]);
        } else {
          throw e;
        }
      }
      let stR = { rows: [] };
      try { stR = await query(statsSql, [uai]); } catch (e) { if (e.code !== '42P01') console.warn('FR stats table missing/error:', e.message); }

      if (!frR.rows.length) {
        return res.status(404).json({ error: 'School not found' });
      }
      const row = frR.rows[0];
      const stats = stR.rows[0] || {};

      const avgReview = stats.avg_overall_rating == null ? null : Number(stats.avg_overall_rating);
      // Pull aggregates directly from fr_ecoles row when available
      const overallOn10 = avgReview == null ? null : Math.round(avgReview * 20) / 10; // 0-5 -> 0-10

      const payload = {
        success: true,
        school: {
          urn: row.uai,
          name: row.name,
          country: 'France',

          type: row.type_etablissement,
          phase: row.statut_public_prive,
          status: row.type_contrat_prive,

          telephone: row.telephone || null,
          website: row.web || null,
          email: row.mail || null,
          headteacher_name: null,
          headteacher_job_title: null,
          latitude: row.latitude,
          longitude: row.longitude,

          address: {
            street: [row.adresse_1, row.adresse_2, row.adresse_3].filter(Boolean).join(', ') || null,
            locality: null,
            town: row.commune,
            postcode: row.code_postal,
            local_authority: row.departement,
            county: row.departement,
            region: row.region
          },

          characteristics: {
            gender: null,
            age_range: 'N/A',
            religious_character: null,
            admissions_policy: null,
            has_nursery: null,
            has_sixth_form: null,
            is_boarding_school: null,
            has_sen_provision: null
          },

          demographics: {
            total_students: (
              (row.students_total != null ? parseInt(row.students_total, 10) : null) ??
              (row.lycee_students_total != null ? parseInt(row.lycee_students_total, 10) : null) ??
              (row.nombre_d_eleves != null ? parseInt(row.nombre_d_eleves, 10) : null)
            ),
            boys: (row.boys_total != null ? parseInt(row.boys_total, 10) : null),
            girls: (row.girls_total != null ? parseInt(row.girls_total, 10) : null),
            fsm_percentage: null,
            eal_percentage: null,
            sen_support_percentage: null,
            sen_ehcp_percentage: null
          },

          attendance: {
            overall_absence_rate: null,
            persistent_absence_rate: null
          },

          test_scores: null,
          ofsted: null,

          // Enrolment (FR)
          enrolment_stats: {
            total: (row.nombre_d_eleves != null ? parseInt(row.nombre_d_eleves, 10) : null)
          },

          overall_rating: overallOn10,
          rating_components: null,
          rating_percentile: null,
          rating_data_completeness: null,
          la_comparison: null,

          fr_performance: {
            lycee_bac_candidates: row.lycee_bac_candidates ? parseInt(row.lycee_bac_candidates) : null,
            lycee_bac_success_rate: row.lycee_bac_success_rate ? Number(row.lycee_bac_success_rate) : null,
            lycee_mentions_rate: row.lycee_mentions_rate ? Number(row.lycee_mentions_rate) : null,
            college_dnb_candidates: row.college_dnb_candidates ? parseInt(row.college_dnb_candidates) : null,
            college_dnb_success_rate: row.college_dnb_success_rate ? Number(row.college_dnb_success_rate) : null
          },

          reviews: {
            avg_overall_rating: avgReview,
            total_reviews: stats.total_reviews || 0,
            recommendation_percentage: stats.recommendation_percentage == null ? null : Number(stats.recommendation_percentage)
          }
        }
      };

      // Add departement average (same type) for comparison if possible
      try {
        if (row.departement && row.type_etablissement) {
          const depAvgSql = `
            SELECT AVG(nombre_d_eleves)::numeric(10,2) AS avg_dep
            FROM fr_ecoles
            WHERE libelle_departement = $1
              AND type_etablissement = $2
              AND nombre_d_eleves IS NOT NULL`;
          const depAvgR = await query(depAvgSql, [row.departement, row.type_etablissement]);
          const avgDep = depAvgR.rows && depAvgR.rows[0] ? depAvgR.rows[0].avg_dep : null;
          if (avgDep != null) {
            payload.school.enrolment_stats.departement_average = Number(avgDep);
          }
        }
      } catch (e) {
        console.warn('FR departement average error:', e.message);
      }

      return res.json(payload);
    }

    // -------------------------------------------------------------------
    // UK branch: numeric URN
    // -------------------------------------------------------------------
    if (!urn || isNaN(urn)) {
      return res.status(400).json({ error: 'Invalid URN provided' });
    }

    // 1) Base row from uk_schools - includes new rating columns
    const baseSql = `
      SELECT
        s.id, s.urn, s.la_code, s.establishment_number, s.name, s.name_lower, s.slug,
        s.establishment_status, s.type_of_establishment, s.establishment_group, s.phase_of_education,
        s.street, s.locality, s.town, s.county, s.postcode,
        s.latitude, s.longitude,
        s.local_authority, s.region, s.parliamentary_constituency, s.ward, s.urban_rural,
        s.website, s.telephone,
        s.head_title, s.head_first_name, s.head_last_name, s.head_job_title,
        s.gender, s.age_range_lower, s.age_range_upper,
        s.school_capacity, s.total_pupils, s.boys_count, s.girls_count,
        s.has_nursery, s.has_sixth_form, s.is_boarding_school, s.has_sen_provision,
        s.religious_character, s.religious_ethos, s.diocese, s.percentage_fsm,
        s.is_part_of_trust, s.trust_name, s.ukprn, s.uprn,
        s.date_opened, s.last_changed_date, s.created_at, s.updated_at,

        /* test scores (school-level) */
        s.english_score, s.math_score, s.science_score,

        /* NEW: national averages (renamed columns) */
        s.english_avg_national, s.math_avg_national, s.science_avg_national,

        /* NEW: local authority averages already cached on the school row (optional) */
        s.english_avg_la, s.math_avg_la, s.science_avg_la,

        /* rating fields on the school row */
        s.overall_rating, s.rating_components, s.rating_percentile, s.rating_updated_at,
        s.country,

        /* latest ofsted (if any) */
        o.overall_effectiveness       AS ofsted_overall_effectiveness,
        o.inspection_date             AS ofsted_inspection_date,
        o.publication_date            AS ofsted_publication_date,

        /* latest absence snapshot */
        a.overall_absence_rate,
        a.persistent_absence_rate,

        /* convenience: computed attendance rate */
        CASE
          WHEN a.overall_absence_rate IS NOT NULL THEN 100 - a.overall_absence_rate
          ELSE NULL
        END AS attendance_rate
      FROM uk_schools s
      /* latest ofsted */
      LEFT JOIN LATERAL (
        SELECT overall_effectiveness, inspection_date, publication_date
        FROM uk_ofsted_inspections oi
        WHERE oi.urn = s.urn
        ORDER BY COALESCE(inspection_date, publication_date) DESC NULLS LAST
        LIMIT 1
      ) o ON TRUE
      /* latest attendance */
      LEFT JOIN LATERAL (
        SELECT overall_absence_rate, persistent_absence_rate
        FROM uk_absence_data ua
        WHERE ua.urn = s.urn
        ORDER BY academic_year DESC NULLS LAST
        LIMIT 1
      ) a ON TRUE
      WHERE s.urn = $1
      LIMIT 1
    `;
    const baseR = await query(baseSql, [urn]);
    if (baseR.rows.length === 0) {
      return res.status(404).json({ error: 'School not found' });
    }
    const s = baseR.rows[0];



    // Normalize: test_scores structure for the HTML components
    s.test_scores = {
      english: {
        score: toNum(s.english_score),
        average: toNum(s.english_avg_national),  // marker text says “National Avg”
        la_average: toNum(s.english_avg_la)      // keep both if you want to show LA too
      },
      math: {
        score: toNum(s.math_score),
        average: toNum(s.math_avg_national),
        la_average: toNum(s.math_avg_la)
      },
      science: {
        score: toNum(s.science_score),
        average: toNum(s.science_avg_national),
        la_average: toNum(s.science_avg_la)
      }
    };

    // One canonical field for calculators:
    s.attendance_rate = toNum(s.attendance_rate);

    // If you pass Ofsted into the header & rating box:
    s.ofsted = s.ofsted_overall_effectiveness ? {
      overall_effectiveness: s.ofsted_overall_effectiveness,
      inspection_date: s.ofsted_inspection_date,
      overall_label: getOfstedLabel(s.ofsted_overall_effectiveness)
    } : null;


    // 2) Get latest Ofsted and other data
    const ofstedSql = `
      SELECT
        overall_effectiveness,
        inspection_date,
        publication_date,
        quality_of_education,
        behaviour_and_attitudes,
        personal_development,
        leadership_and_management,
        safeguarding_effective,
        sixth_form_provision,
        early_years_provision,
        previous_inspection_date,
        previous_overall_effectiveness,
        web_link
      FROM uk_ofsted_inspections
      WHERE urn = $1
      ORDER BY COALESCE(inspection_date, publication_date) DESC
      LIMIT 1
    `;
    const censusSql = `
      SELECT number_on_roll, number_girls, number_boys,
             percentage_fsm_ever6, percentage_eal, percentage_sen_support, percentage_sen_ehcp
      FROM uk_census_data
      WHERE urn = $1
      ORDER BY academic_year DESC NULLS LAST
      LIMIT 1
    `;
    const attendSql = `
      SELECT overall_absence_rate, persistent_absence_rate
      FROM uk_absence_data
      WHERE urn = $1
      ORDER BY academic_year DESC NULLS LAST
      LIMIT 1
    `;
    
    const [oR, cR, aR] = await Promise.all([
      query(ofstedSql, [urn]),
      query(censusSql, [urn]),
      query(attendSql, [urn]),
    ]);

    const o = oR.rows[0] || {};
    const c = cR.rows[0] || {};
    const a = aR.rows[0] || {};

    // 3) Calculate LA averages for comparison
    const laAvgSql = `
      SELECT
        AVG(s2.english_score) AS avg_english,
        AVG(s2.math_score)    AS avg_math,
        AVG(s2.science_score) AS avg_science,
        AVG(CASE WHEN a2.overall_absence_rate IS NOT NULL
                THEN 100 - a2.overall_absence_rate END) AS avg_attendance,
        ARRAY_AGG(s2.overall_rating) FILTER (WHERE s2.overall_rating IS NOT NULL) AS all_ratings
      FROM uk_schools s2
      LEFT JOIN LATERAL (
        SELECT overall_absence_rate
        FROM uk_absence_data ua2
        WHERE ua2.urn = s2.urn
        ORDER BY academic_year DESC NULLS LAST
        LIMIT 1
      ) a2 ON TRUE
      WHERE s2.local_authority = $1
        AND s2.phase_of_education = $2
        AND s2.urn <> $3
    `;
    const laAvgR = await query(laAvgSql, [s.local_authority, s.phase_of_education, urn]);
    const laAverages = laAvgR.rows[0] || {};

    // 4) Always calculate an up-to-date rating for response payload.
    //    We still only persist to DB when stale or forced.
    const forceRecalculate = req.query.recalculate === '1';
    const needsRatingUpdate = forceRecalculate || !s.rating_updated_at ||
      !s.rating_components ||
      (Date.now() - new Date(s.rating_updated_at) > 30 * 24 * 60 * 60 * 1000);

    // Prepare data for rating calculation
    const isWales = s.country && s.country.toLowerCase() === 'wales';
    
    // If overall_effectiveness is null but other Ofsted scores exist, try to infer it
    let ofstedRating = o.overall_effectiveness;
    if (!ofstedRating && o.quality_of_education && !isWales) {
      // Use quality_of_education as a proxy if overall is missing (but not for Wales)
      ofstedRating = o.quality_of_education;
    }
    
    // For Wales schools, explicitly set Ofsted to null
    if (isWales) {
      ofstedRating = null;
    }
    
    const schoolForRating = {
      country: s.country || 'england',
      ofsted_overall_effectiveness: ofstedRating,
      english_score: toNum(s.english_score),
      math_score: toNum(s.math_score),
      science_score: toNum(s.science_score),
      attendance_rate: a.overall_absence_rate ? (100 - a.overall_absence_rate) : null
    };
    
    // Compute latest rating (for response)
    const calculatedRating = calculateRatingWithFallbacks(schoolForRating, laAverages);

    // Persist to DB only if stale/forced and we have a rating
    if ((needsRatingUpdate || debug) && calculatedRating.rating !== null && !debug) {
      await query(`
        UPDATE uk_schools 
        SET overall_rating = $1,
            rating_components = $2,
            rating_percentile = $3,
            rating_updated_at = NOW()
        WHERE urn = $4
      `, [
        calculatedRating.rating, 
        JSON.stringify(calculatedRating.components),
        calculatedRating.percentile,
        urn
      ]);
      
      // Update local object
      s.overall_rating = calculatedRating.rating;
      s.rating_components = calculatedRating.components;
      s.rating_percentile = calculatedRating.percentile;
    }

    // Normalize leader name and contact
    const headteacher_name = [s.head_title, s.head_first_name, s.head_last_name]
      .filter(Boolean)
      .join(' ')
      .trim() || null;

    // Coalesce demographics
    const total_students = s.total_pupils ?? c.number_on_roll ?? null;
    const boys = s.boys_count ?? c.number_boys ?? null;
    const girls = s.girls_count ?? c.number_girls ?? null;

    // Use database rating or calculated rating
    const final_rating = s.overall_rating || (calculatedRating && calculatedRating.rating) || null;
    
    // Determine country flags (reuse earlier isWales for consistency)
    const country = (s.country || 'england').toLowerCase();
    const isScotland = country === 'scotland';

    const payload = {
      success: true,
      school: {
        // Basic
        urn: s.urn,
        name: s.name,
        country: s.country || 'England',
        is_wales: isWales,
        is_scotland: isScotland,
        type: s.type_of_establishment,
        phase: s.phase_of_education,
        status: s.establishment_status,
        // Trust / Academy
        is_part_of_trust: !!s.is_part_of_trust,
        trust_name: s.trust_name || null,
        ukprn: s.ukprn || null,

        // Contact / map
        telephone: s.telephone || null,
        website: s.website || null,
        headteacher_name,
        headteacher_job_title: s.head_job_title || null,
        latitude: toNum(s.latitude),
        longitude: toNum(s.longitude),

        // Address
        address: {
          street: s.street,
          locality: s.locality,
          town: s.town,
          postcode: s.postcode,
          local_authority: s.local_authority,
          county: s.county || null,
          region: s.region || null,
        },

        // Characteristics
        characteristics: {
          gender: s.gender,
          age_range: `${s.age_range_lower ?? 'N/A'} - ${s.age_range_upper ?? 'N/A'}`,
          religious_character: s.religious_character,
          admissions_policy: s.admissions_policy || null,
          has_nursery: !!s.has_nursery,
          has_sixth_form: !!s.has_sixth_form,
          is_boarding_school: !!s.is_boarding_school,
          has_sen_provision: !!s.has_sen_provision,
        },

        // Demographics
        demographics: {
          total_students,
          boys,
          girls,
          fsm_percentage: s.percentage_fsm ?? c.percentage_fsm_ever6 ?? null,
          eal_percentage: c.percentage_eal ?? null,
          sen_support_percentage: c.percentage_sen_support ?? null,
          sen_ehcp_percentage: c.percentage_sen_ehcp ?? null,
        },

        // Attendance
        attendance: {
          overall_absence_rate: a.overall_absence_rate ?? null,
          persistent_absence_rate: a.persistent_absence_rate ?? null,
        },

        // Test Scores
        test_scores: {
          english: {
            score: toNum(s.english_score),
            average: toNum(s.english_avg),
            la_average: toNum(laAverages.avg_english)
          },
          math: {
            score: toNum(s.math_score),
            average: toNum(s.math_avg),
            la_average: toNum(laAverages.avg_math)
          },
          science: {
            score: toNum(s.science_score),
            average: toNum(s.science_avg),
            la_average: toNum(laAverages.avg_science)
          }
        },

        // Ofsted - set to null for Wales schools
        ofsted: isWales ? {
          overall_effectiveness: null,
          overall_label: 'Not Applicable (Wales)',
          inspection_date: null,
          publication_date: null,
          quality_of_education: null,
          behaviour_and_attitudes: null,
          personal_development: null,
          leadership_and_management: null,
          safeguarding_effective: null,
          sixth_form_provision: null,
          early_years_provision: null,
          previous_inspection_date: null,
          previous_overall_effectiveness: null,
          web_link: null,
        } : {
          overall_effectiveness: o.overall_effectiveness ?? null,
          overall_label: getOfstedLabel(o.overall_effectiveness),
          inspection_date: o.inspection_date ?? null,
          publication_date: o.publication_date ?? null,
          quality_of_education: o.quality_of_education ?? null,
          behaviour_and_attitudes: o.behaviour_and_attitudes ?? null,
          personal_development: o.personal_development ?? null,
          leadership_and_management: o.leadership_and_management ?? null,
          safeguarding_effective: o.safeguarding_effective ?? null,
          sixth_form_provision: o.sixth_form_provision ?? null,
          early_years_provision: o.early_years_provision ?? null,
          previous_inspection_date: o.previous_inspection_date ?? null,
          previous_overall_effectiveness: o.previous_overall_effectiveness ?? null,
          web_link: o.web_link || null,
        },

        // Overall rating (new)
        overall_rating: final_rating,
        // Prefer freshly calculated components for accuracy, fall back to DB copy
        rating_components: (calculatedRating && calculatedRating.components) || s.rating_components || null,
        rating_percentile: s.rating_percentile || (calculatedRating && calculatedRating.percentile) || null,
        rating_data_completeness: calculatedRating ? calculatedRating.data_completeness : null,
        
        // LA comparison data
        la_comparison: {
          local_authority: s.local_authority,
          school_count: laAverages.school_count,
          averages: {
            english: toNum(laAverages.avg_english),
            math: toNum(laAverages.avg_math),
            science: toNum(laAverages.avg_science),
            attendance: toNum(laAverages.avg_attendance)
          }
        }
      },
    };

    if (debug) {
      payload.__debug = {
        base_row: true,
        ofsted_row: !!oR.rows[0],
        census_row: !!cR.rows[0],
        attendance_row: !!aR.rows[0],
        rating_calculated: !!calculatedRating,
        rating_from_db: !!s.overall_rating,
        la_averages: laAverages,
        country: s.country,
        is_wales: isWales,
        is_scotland: isScotland
      };
    }

    return res.json(payload);
  } catch (err) {
    console.error('School fetch error:', err);
    return res.status(500).json({ error: 'Failed to fetch school information', message: err.message });
  }
});

/* =======================================================================
 * GET /api/schools/:urn/performance
 * ======================================================================= */
router.get('/:urn/performance', async (req, res) => {
  try {
    const { urn } = req.params;

    const ks2Sql = `
      SELECT * FROM uk_ks2_performance
      WHERE urn = $1
      ORDER BY academic_year DESC
      LIMIT 1
    `;
    const ks4Sql = `
      SELECT * FROM uk_ks4_performance
      WHERE urn = $1
      ORDER BY academic_year DESC
      LIMIT 1
    `;
    const ks5Sql = `
      SELECT * FROM uk_ks5_performance
      WHERE urn = $1
      ORDER BY academic_year DESC
      LIMIT 1
    `;
    const ks4DestSql = `
      SELECT * FROM uk_ks4_destinations
      WHERE urn = $1
      ORDER BY academic_year DESC
      LIMIT 1
    `;
    const ks5DestSql = `
      SELECT * FROM uk_ks5_destinations
      WHERE urn = $1
      ORDER BY academic_year DESC
      LIMIT 1
    `;

    const [ks2R, ks4R, ks5R, ks4DestR, ks5DestR] = await Promise.all([
      query(ks2Sql, [urn]),
      query(ks4Sql, [urn]),
      query(ks5Sql, [urn]),
      query(ks4DestSql, [urn]),
      query(ks5DestSql, [urn]),
    ]);

    return res.json({
      success: true,
      performance: {
        ks2: ks2R.rows[0] || null,
        ks4: ks4R.rows[0] || null,
        ks5: ks5R.rows[0] || null,
        destinations: {
          ks4: ks4DestR.rows[0] || null,
          ks5: ks5DestR.rows[0] || null,
        },
      },
    });
  } catch (err) {
    console.error('Performance fetch error:', err);
    return res.status(500).json({ error: 'Failed to fetch performance data', message: err.message });
  }
});

/* =======================================================================
 * GET /api/schools/:urn/performance/detailed
 * ======================================================================= */
router.get('/:urn/performance/detailed', async (req, res) => {
  try {
    const { urn } = req.params;

    const subjectVASql = `
      SELECT subject_name, qualification_type, value_added_score,
             lower_confidence_limit, upper_confidence_limit,
             number_of_entries, cohort_size
      FROM uk_subject_value_added
      WHERE urn = $1
      ORDER BY number_of_entries DESC
    `;
    const qualVASql = `
      SELECT qualification_type, value_added_score,
             lower_confidence_limit, upper_confidence_limit,
             number_of_entries, cohort_size
      FROM uk_qualification_value_added
      WHERE urn = $1
      ORDER BY number_of_entries DESC
    `;
    const stemSql = `
      SELECT * FROM uk_ks5_stem_participation
      WHERE urn = $1
      LIMIT 1
    `;

    const [subjectVA, qualVA, stem] = await Promise.all([
      query(subjectVASql, [urn]),
      query(qualVASql, [urn]),
      query(stemSql, [urn]),
    ]);

    return res.json({
      success: true,
      detailed_performance: {
        subject_value_added: subjectVA.rows,
        qualification_value_added: qualVA.rows,
        stem_participation: stem.rows[0] || null,
      },
    });
  } catch (err) {
    console.error('Detailed performance fetch error:', err);
    return res.status(500).json({ error: 'Failed to fetch detailed performance data', message: err.message });
  }
});

/* =======================================================================
 * GET /api/schools/:urn/nearby
 * (same LA + same phase for now)
 * ======================================================================= */
router.get('/:urn/nearby', async (req, res) => {
  try {
    const { urn } = req.params;
    const { limit = 10 } = req.query;

    const currentSql = `
      SELECT local_authority, phase_of_education
      FROM uk_schools
      WHERE urn = $1
      LIMIT 1
    `;
    const curR = await query(currentSql, [urn]);
    if (curR.rows.length === 0) {
      return res.status(404).json({ error: 'School not found' });
    }
    const cur = curR.rows[0];

    const nearbySql = `
      SELECT
        s.urn,
        s.name,
        s.type_of_establishment,
        s.postcode,
        s.town,
        o.overall_effectiveness AS ofsted_rating,
        COALESCE(c.number_on_roll, s.total_pupils) AS number_on_roll,
        COALESCE(
          s.overall_rating,
          CASE
            WHEN o.overall_effectiveness = 1 THEN 9
            WHEN o.overall_effectiveness = 2 THEN 7
            WHEN o.overall_effectiveness = 3 THEN 5
            WHEN o.overall_effectiveness = 4 THEN 3
            ELSE NULL
          END
        ) AS overall_rating
      FROM uk_schools s
      LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
      LEFT JOIN uk_census_data c ON s.urn = c.urn
      WHERE s.local_authority = $1
        AND s.urn <> $2
        AND s.phase_of_education = $3
      ORDER BY s.name
      LIMIT $4
    `;
    const nearR = await query(nearbySql, [
      cur.local_authority,
      urn,
      cur.phase_of_education,
      parseInt(limit, 10),
    ]);

    return res.json({
      success: true,
      current_school: {
        urn,
        local_authority: cur.local_authority,
        phase: cur.phase_of_education,
      },
      nearby_schools: nearR.rows.map(row => ({
        ...row,
        ofsted_label: getOfstedLabel(row.ofsted_rating),
      })),
    });
  } catch (err) {
    console.error('Nearby schools fetch error:', err);
    return res.status(500).json({ error: 'Failed to fetch nearby schools', message: err.message });
  }
});

/* =======================================================================
 * GET /api/schools/:urn/comparison
 * ======================================================================= */
router.get('/:urn/comparison', async (req, res) => {
  try {
    const { urn } = req.params;

    const compSql = `
      SELECT
        s.urn,
        s.name,
        s.phase_of_education,
        s.local_authority,
        o.overall_effectiveness as ofsted_rating,
        c.percentage_fsm_ever6,
        COALESCE(c.number_on_roll, s.total_pupils) AS number_on_roll,
        ks4.progress_8_score,
        ks4.attainment_8_score,
        ks4.basics_9_5_percentage,
        ks2.rwm_expected_percentage,
        ks2.reading_progress,
        ks2.maths_progress,
        s.english_score,
        s.math_score,
        s.science_score
      FROM uk_schools s
      LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
      LEFT JOIN uk_census_data c ON s.urn = c.urn
      LEFT JOIN uk_ks4_performance ks4 ON s.urn = ks4.urn
      LEFT JOIN uk_ks2_performance ks2 ON s.urn = ks2.urn
      WHERE s.urn = $1
      LIMIT 1
    `;
    const compR = await query(compSql, [urn]);
    if (compR.rows.length === 0) {
      return res.status(404).json({ error: 'School not found' });
    }
    const school = compR.rows[0];

    const laAvgSql = `
      SELECT
        AVG(c.percentage_fsm_ever6) as avg_fsm,
        AVG(ks4.progress_8_score) as avg_progress_8,
        AVG(ks4.attainment_8_score) as avg_attainment_8,
        AVG(ks2.rwm_expected_percentage) as avg_ks2_expected,
        AVG(s.english_score) as avg_english_score,
        AVG(s.math_score) as avg_math_score,
        AVG(s.science_score) as avg_science_score,
        COUNT(DISTINCT s.urn) as school_count
      FROM uk_schools s
      LEFT JOIN uk_census_data c ON s.urn = c.urn
      LEFT JOIN uk_ks4_performance ks4 ON s.urn = ks4.urn
      LEFT JOIN uk_ks2_performance ks2 ON s.urn = ks2.urn
      WHERE s.local_authority = $1
        AND s.phase_of_education = $2
    `;
    const natAvgSql = `
      SELECT
        AVG(c.percentage_fsm_ever6) as avg_fsm,
        AVG(ks4.progress_8_score) as avg_progress_8,
        AVG(ks4.attainment_8_score) as avg_attainment_8,
        AVG(ks2.rwm_expected_percentage) as avg_ks2_expected,
        AVG(s.english_score) as avg_english_score,
        AVG(s.math_score) as avg_math_score,
        AVG(s.science_score) as avg_science_score,
        COUNT(DISTINCT s.urn) as school_count
      FROM uk_schools s
      LEFT JOIN uk_census_data c ON s.urn = c.urn
      LEFT JOIN uk_ks4_performance ks4 ON s.urn = ks4.urn
      LEFT JOIN uk_ks2_performance ks2 ON s.urn = ks2.urn
      WHERE s.phase_of_education = $1
    `;

    const [laR, natR] = await Promise.all([
      query(laAvgSql, [school.local_authority, school.phase_of_education]),
      query(natAvgSql, [school.phase_of_education]),
    ]);

    return res.json({
      success: true,
      comparison: {
        school,
        local_authority_average: laR.rows[0],
        national_average: natR.rows[0],
      },
    });
  } catch (err) {
    console.error('Comparison fetch error:', err);
    return res.status(500).json({ error: 'Failed to fetch comparison data', message: err.message });
  }
});

module.exports = router;
