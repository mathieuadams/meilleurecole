const express = require('express');
const router = express.Router();
const { query } = require('../config/database');

/**
 * @route   GET /api/search
 * @desc    Search schools by name, postcode, or location
 * @query   q (search term), type (name|postcode|location), limit, offset
 * @example /api/search?q=Westminster&type=name&limit=10
 */
// --- /api/search/suggest ---
router.get('/suggest', async (req, res) => {
  const q = String(req.query.q || '').trim();
  const limit = Math.min(parseInt(req.query.limit || '8', 10), 20);
  if (q.length < 2) return res.json({ schools: [], cities: [], authorities: [], postcodes: [] });

  const likePrefix = q.replace(/[%_]/g, '') + '%';

  try {
    const schoolsSql = `
      SELECT s.urn, s.name, s.town, s.postcode,
             COALESCE(s.overall_rating,
                      CASE o.overall_effectiveness
                        WHEN 1 THEN 9 WHEN 2 THEN 7 WHEN 3 THEN 5 WHEN 4 THEN 3 ELSE NULL END
             ) AS overall_rating
      FROM uk_schools s
      LEFT JOIN uk_ofsted_inspections o ON o.urn = s.urn
      WHERE s.name ILIKE $1 OR s.postcode ILIKE $1 OR s.town ILIKE $1
      ORDER BY s.overall_rating DESC NULLS LAST, s.name ASC
      LIMIT $2;`;

    const citiesSql = `
      SELECT s.town, MAX(s.country) AS country
      FROM uk_schools s
      WHERE s.town ILIKE $1
      GROUP BY s.town
      ORDER BY COUNT(*) DESC, s.town ASC
      LIMIT $2;`;

    const laSql = `
      SELECT s.local_authority, MAX(s.region) AS region
      FROM uk_schools s
      WHERE s.local_authority ILIKE $1
      GROUP BY s.local_authority
      ORDER BY COUNT(*) DESC, s.local_authority ASC
      LIMIT $2;`;

    const pcSql = `
      SELECT DISTINCT s.postcode
      FROM uk_schools s
      WHERE s.postcode ILIKE $1
      ORDER BY s.postcode
      LIMIT $2;`;

    const [schools, cities, authorities, postcodes] = await Promise.all([
      query(schoolsSql, [likePrefix, limit]),
      query(citiesSql,   [likePrefix, Math.max(5, Math.floor(limit/2))]),
      query(laSql,       [likePrefix, Math.max(5, Math.floor(limit/2))]),
      query(pcSql,       [likePrefix, 6]),
    ]).then(rs => rs.map(r => r.rows));

    res.json({
      schools: schools.map(r => ({ type:'school', urn:r.urn, name:r.name, town:r.town, postcode:r.postcode, overall_rating:r.overall_rating })),
      cities:  cities.map(r => ({ type:'city', town:r.town, country:r.country })),
      authorities: authorities.map(r => ({ type:'la', local_authority:r.local_authority, region:r.region })),
      postcodes: postcodes.map(r => ({ type:'pc', postcode:r.postcode })),
    });
  } catch (e) {
    console.error('suggest error', e);
    res.json({ schools: [], cities: [], authorities: [], postcodes: [] });
  }
});

// --- /api/search/school-autocomplete ---
router.get('/school-autocomplete', async (req, res) => {
  const qRaw = String(req.query.q || '').trim();
  const limit = Math.min(parseInt(req.query.limit || '8', 10), 20);
  if (qRaw.length < 2) return res.json({ schools: [] });

  const q = qRaw.replace(/[%_]/g, '');
  const like = q + '%';
  const tokens = q.split(/\s+/).filter(Boolean);
  const tokenConds = tokens.map((_, i) => `LOWER(name) LIKE LOWER($${i + 3})`).join(' AND ');
  const tokenParams = tokens.map(t => `%${t}%`);

  const sql = `
    SELECT urn, name, town, postcode, overall_rating
    FROM uk_schools
    WHERE name ILIKE $1
       OR postcode ILIKE $1
       OR town ILIKE $1
       OR (${tokens.length ? tokenConds : 'FALSE'})
    ORDER BY overall_rating DESC NULLS LAST, name ASC
    LIMIT $2`;

  try {
    const { rows } = await query(sql, [like, limit, ...tokenParams]);
    res.json({ schools: rows });
  } catch (e) {
    console.error('school-autocomplete error', e);
    res.json({ schools: [] });
  }
});



router.get('/', async (req, res) => {
  try {
    const { 
      q, 
      type = 'all',
      limit = 20, 
      offset = 0,
      phase,
      ofsted,
      la,
      phases: phasesStr,
      minRating
    } = req.query;

    // Validate search query
    if (!q || q.trim().length < 2) {
      return res.status(400).json({ 
        error: 'Search query must be at least 2 characters long' 
      });
    }

    // Build the SQL query - now using stored overall_rating
    let sqlQuery = `
      SELECT 
        s.urn,
        s.name,
        s.postcode,
        s.town,
        s.latitude,
        s.longitude,
        s.local_authority,
        s.phase_of_education,
        s.type_of_establishment,
        s.street,
        s.religious_character,
        s.gender,
        s.overall_rating,
        s.rating_percentile,
        o.overall_effectiveness as ofsted_rating,
        o.inspection_date,
        COALESCE(c.number_on_roll, s.total_pupils) AS number_on_roll,
        c.percentage_fsm_ever6 as fsm_percentage
      FROM uk_schools s
      LEFT JOIN LATERAL (
        SELECT overall_effectiveness, inspection_date
        FROM uk_ofsted_inspections o
        WHERE o.urn = s.urn
        ORDER BY COALESCE(inspection_date, publication_date) DESC NULLS LAST
        LIMIT 1
      ) o ON TRUE
      LEFT JOIN LATERAL (
        SELECT number_on_roll, percentage_fsm_ever6
        FROM uk_census_data c2
        WHERE c2.urn = s.urn
        ORDER BY academic_year DESC NULLS LAST
        LIMIT 1
      ) c ON TRUE
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 0;

    // Add search conditions based on type
    const searchTerm = `%${q.trim()}%`;
    const qExact = q.trim();
    
    if (type === 'name') {
      paramCount++;
      sqlQuery += ` AND LOWER(s.name) LIKE LOWER($${paramCount})`;
      params.push(searchTerm);
    } else if (type === 'postcode') {
      paramCount++;
      sqlQuery += ` AND LOWER(s.postcode) LIKE LOWER($${paramCount})`;
      params.push(searchTerm);
    } else if (type === 'location') {
      // Prefer exact city/LA matches to avoid false positives like 'Londonderry'
      const laPrefix = `${qExact}%`;
      paramCount += 3;
      sqlQuery += ` AND (
        LOWER(s.town) = LOWER($${paramCount-2}) OR
        LOWER(s.local_authority) = LOWER($${paramCount-1}) OR
        LOWER(s.local_authority) LIKE LOWER($${paramCount})
      )`;
      params.push(qExact, qExact, laPrefix);
    } else {
      // Search all fields
      paramCount++;
      paramCount++;
      paramCount++;
      paramCount++;
      sqlQuery += ` AND (
        LOWER(s.name) LIKE LOWER($${paramCount-3}) OR 
        LOWER(s.postcode) LIKE LOWER($${paramCount-2}) OR 
        LOWER(s.town) LIKE LOWER($${paramCount-1}) OR 
        LOWER(s.local_authority) LIKE LOWER($${paramCount})
      )`;
      params.push(searchTerm);
      params.push(searchTerm);
      params.push(searchTerm);
      params.push(searchTerm);
    }

    // Add filters if provided
    if (phase) {
      paramCount++;
      sqlQuery += ` AND s.phase_of_education = $${paramCount}`;
      params.push(phase);
    }

    // Multi-ofsted support (comma-separated)
    if (ofsted) {
      const ratings = String(ofsted)
        .split(',')
        .map(v => parseInt(v.trim(), 10))
        .filter(v => [1,2,3,4].includes(v));
      if (ratings.length > 0) {
        const placeholders = ratings.map(() => `$${++paramCount}`).join(',');
        sqlQuery += ` AND o.overall_effectiveness IN (${placeholders})`;
        params.push(...ratings);
      }
    }

    if (la) {
      paramCount++;
      sqlQuery += ` AND LOWER(s.local_authority) = LOWER($${paramCount})`;
      params.push(la);
    }

    // School type filters (from checkboxes): phases=Primary,Secondary,Sixth Form,Special,Independent,Academy
    if (phasesStr) {
      const selected = String(phasesStr)
        .split(',')
        .map(s => s.trim().toLowerCase())
        .filter(Boolean);
      if (selected.length > 0) {
        const orClauses = [];
        // Primary
        if (selected.includes('primary')) {
          paramCount++;
          orClauses.push(`LOWER(s.phase_of_education) LIKE LOWER($${paramCount})`);
          params.push('%primary%');
        }
        // Secondary
        if (selected.includes('secondary')) {
          paramCount += 2;
          orClauses.push(`(LOWER(s.phase_of_education) LIKE LOWER($${paramCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${paramCount}))`);
          params.push('%secondary%','%secondary%');
        }
        // Sixth Form
        if (selected.includes('sixth form') || selected.includes('sixth-form')) {
          paramCount += 2;
          orClauses.push(`(LOWER(s.phase_of_education) LIKE LOWER($${paramCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${paramCount}) OR s.has_sixth_form = TRUE)`);
          params.push('%sixth%','%sixth%');
        }
        // Special
        if (selected.includes('special')) {
          paramCount += 2;
          orClauses.push(`(LOWER(s.phase_of_education) LIKE LOWER($${paramCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${paramCount}))`);
          params.push('%special%','%special%');
        }
        // Independent
        if (selected.includes('independent')) {
          paramCount += 2;
          orClauses.push(`(LOWER(s.establishment_group) LIKE LOWER($${paramCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${paramCount}))`);
          params.push('%independent%','%independent%');
        }
        // Academy
        if (selected.includes('academy')) {
          paramCount += 2;
          orClauses.push(`(LOWER(s.establishment_group) LIKE LOWER($${paramCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${paramCount}))`);
          params.push('%academy%','%academy%');
        }
        if (orClauses.length > 0) {
          sqlQuery += ` AND (${orClauses.join(' OR ')})`;
        }
      }
    }

    // Minimum overall rating filter
    if (minRating) {
      const mr = parseFloat(minRating);
      if (!isNaN(mr)) {
        paramCount++;
        sqlQuery += ` AND s.overall_rating >= $${paramCount}`;
        params.push(mr);
      }
    }

    // Order by overall_rating if available, otherwise by name
    sqlQuery += ` ORDER BY s.overall_rating DESC NULLS LAST, s.name ASC`;
    
    paramCount++;
    sqlQuery += ` LIMIT $${paramCount}`;
    params.push(parseInt(limit));
    
    paramCount++;
    sqlQuery += ` OFFSET $${paramCount}`;
    params.push(parseInt(offset));

    // Execute search query
    console.log('Executing search for:', q, 'Type:', type);
    
    const result = await query(sqlQuery, params);

    // Get total count for pagination
    let countQuery = `
      SELECT COUNT(*) as total
      FROM uk_schools s
      LEFT JOIN LATERAL (
        SELECT overall_effectiveness, inspection_date
        FROM uk_ofsted_inspections o
        WHERE o.urn = s.urn
        ORDER BY COALESCE(inspection_date, publication_date) DESC NULLS LAST
        LIMIT 1
      ) o ON TRUE
      WHERE 1=1
    `;
    
    // Add the same WHERE conditions for count (without LIMIT/OFFSET)
    // Rebuild count params to mirror filters (without LIMIT/OFFSET)
    const countParams = [];
    let countParamCount = 0;

    // replicate search conditions (reuse existing searchTerm)
    if (type === 'name') {
      countParamCount++;
      countQuery += ` AND LOWER(s.name) LIKE LOWER($${countParamCount})`;
      countParams.push(searchTerm);
    } else if (type === 'postcode') {
      countParamCount++;
      countQuery += ` AND LOWER(s.postcode) LIKE LOWER($${countParamCount})`;
      countParams.push(searchTerm);
    } else if (type === 'location') {
      const laPrefix = `${qExact}%`;
      countParamCount += 3;
      countQuery += ` AND (
        LOWER(s.town) = LOWER($${countParamCount-2}) OR
        LOWER(s.local_authority) = LOWER($${countParamCount-1}) OR
        LOWER(s.local_authority) LIKE LOWER($${countParamCount})
      )`;
      countParams.push(qExact, qExact, laPrefix);
    } else {
      countParamCount += 4;
      countQuery += ` AND (
        LOWER(s.name) LIKE LOWER($${countParamCount-3}) OR 
        LOWER(s.postcode) LIKE LOWER($${countParamCount-2}) OR 
        LOWER(s.town) LIKE LOWER($${countParamCount-1}) OR 
        LOWER(s.local_authority) LIKE LOWER($${countParamCount})
      )`;
      countParams.push(searchTerm, searchTerm, searchTerm, searchTerm);
    }

    // Same filters for count
    if (phase) {
      countParamCount++;
      countQuery += ` AND s.phase_of_education = $${countParamCount}`;
      countParams.push(phase);
    }
    if (ofsted) {
      const ratings = String(ofsted)
        .split(',')
        .map(v => parseInt(v.trim(), 10))
        .filter(v => [1,2,3,4].includes(v));
      if (ratings.length > 0) {
        const placeholders = ratings.map(() => `$${++countParamCount}`).join(',');
        countQuery += ` AND o.overall_effectiveness IN (${placeholders})`;
        countParams.push(...ratings);
      }
    }
    if (la) {
      countParamCount++;
      countQuery += ` AND LOWER(s.local_authority) = LOWER($${countParamCount})`;
      countParams.push(la);
    }
    if (phasesStr) {
      const selected = String(phasesStr)
        .split(',')
        .map(s => s.trim().toLowerCase())
        .filter(Boolean);
      if (selected.length > 0) {
        const orClauses = [];
        if (selected.includes('primary')) {
          countParamCount++;
          orClauses.push(`LOWER(s.phase_of_education) LIKE LOWER($${countParamCount})`);
          countParams.push('%primary%');
        }
        if (selected.includes('secondary')) {
          countParamCount += 2;
          orClauses.push(`(LOWER(s.phase_of_education) LIKE LOWER($${countParamCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${countParamCount}))`);
          countParams.push('%secondary%','%secondary%');
        }
        if (selected.includes('sixth form') || selected.includes('sixth-form')) {
          countParamCount += 2;
          orClauses.push(`(LOWER(s.phase_of_education) LIKE LOWER($${countParamCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${countParamCount}) OR s.has_sixth_form = TRUE)`);
          countParams.push('%sixth%','%sixth%');
        }
        if (selected.includes('special')) {
          countParamCount += 2;
          orClauses.push(`(LOWER(s.phase_of_education) LIKE LOWER($${countParamCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${countParamCount}))`);
          countParams.push('%special%','%special%');
        }
        if (selected.includes('independent')) {
          countParamCount += 2;
          orClauses.push(`(LOWER(s.establishment_group) LIKE LOWER($${countParamCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${countParamCount}))`);
          countParams.push('%independent%','%independent%');
        }
        if (selected.includes('academy')) {
          countParamCount += 2;
          orClauses.push(`(LOWER(s.establishment_group) LIKE LOWER($${countParamCount-1}) OR LOWER(s.type_of_establishment) LIKE LOWER($${countParamCount}))`);
          countParams.push('%academy%','%academy%');
        }
        if (orClauses.length > 0) {
          countQuery += ` AND (${orClauses.join(' OR ')})`;
        }
      }
    }
    if (minRating) {
      const mr = parseFloat(minRating);
      if (!isNaN(mr)) {
        countParamCount++;
        countQuery += ` AND s.overall_rating >= $${countParamCount}`;
        countParams.push(mr);
      }
    }

    const countResult = await query(countQuery, countParams);

    // Format response
    res.json({
      success: true,
      query: q,
      type: type,
      total: parseInt(countResult.rows[0]?.total || 0),
      limit: parseInt(limit),
      offset: parseInt(offset),
      schools: result.rows.map(school => ({
        ...school,
        ofsted_label: getOfstedLabel(school.ofsted_rating),
        overall_rating: school.overall_rating ? parseFloat(school.overall_rating) : null,
        rating_display: school.overall_rating ? `${parseFloat(school.overall_rating).toFixed(1)}/10` : 'N/A',
        percentile_text: school.rating_percentile ? `Top ${100 - school.rating_percentile}%` : null
      }))
    });

  } catch (error) {
    console.error('Search error:', error);
    res.status(500).json({ 
      error: 'Failed to search schools',
      message: error.message 
    });
  }
});

/**
 * @route   GET /api/search/nearby
 * @desc    Search schools near specific coordinates
 * @query   lat, lng, radius (in km)
 * @example /api/search/nearby?lat=51.5074&lng=-0.1278&radius=5
 */
router.get('/nearby', async (req, res) => {
  try {
    const { lat, lng, radius = 5, limit = 100 } = req.query;
    
    if (!lat || !lng) {
      return res.status(400).json({ 
        error: 'Latitude and longitude are required' 
      });
    }
    
    const latitude = parseFloat(lat);
    const longitude = parseFloat(lng);
    const searchRadius = Math.min(parseFloat(radius) || 5, 50); // Max 50km
    const resultLimit = Math.min(parseInt(limit) || 100, 500); // Max 500 results
    
    if (isNaN(latitude) || isNaN(longitude)) {
      return res.status(400).json({ 
        error: 'Invalid latitude or longitude values' 
      });
    }
    
    // PostgreSQL version with proper distance calculation
    // Using a subquery to calculate distance, then filtering in WHERE clause
    const sqlQuery = `
      WITH school_distances AS (
        SELECT 
          s.urn,
          s.name,
          s.postcode,
          s.town,
          s.local_authority,
          s.phase_of_education,
          s.type_of_establishment,
          s.street,
          s.religious_character,
          s.gender,
          s.overall_rating,
          s.rating_percentile,
          s.latitude,
          s.longitude,
          o.overall_effectiveness as ofsted_rating,
          o.inspection_date,
          COALESCE(c.number_on_roll, s.total_pupils) AS number_on_roll,
          c.percentage_fsm_ever6 as fsm_percentage,
          (
            6371 * acos(
              LEAST(1.0, 
                cos(radians($1)) * cos(radians(s.latitude)) * 
                cos(radians(s.longitude) - radians($2)) + 
                sin(radians($1)) * sin(radians(s.latitude))
              )
            )
          ) AS distance_km
        FROM uk_schools s
        LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
        LEFT JOIN uk_census_data c ON s.urn = c.urn
        WHERE 
          s.latitude IS NOT NULL 
          AND s.longitude IS NOT NULL
          AND s.latitude BETWEEN $1 - ($3 / 111.0) AND $1 + ($3 / 111.0)
          AND s.longitude BETWEEN $2 - ($3 / (111.0 * cos(radians($1)))) AND $2 + ($3 / (111.0 * cos(radians($1))))
      )
      SELECT * FROM school_distances
      WHERE distance_km <= $3
      ORDER BY overall_rating DESC NULLS LAST, distance_km ASC
      LIMIT $4
    `;
    
    console.log('Searching for schools near:', { latitude, longitude, searchRadius, resultLimit });
    
    const result = await query(sqlQuery, [latitude, longitude, searchRadius, resultLimit]);
    
    console.log(`Found ${result.rows.length} schools within ${searchRadius}km`);
    
    // Format response
    res.json({
      success: true,
      center: { lat: latitude, lng: longitude },
      radius: searchRadius,
      total: result.rows.length,
      schools: result.rows.map(school => ({
        ...school,
        latitude: school.latitude,
        longitude: school.longitude,
        distance: school.distance_km ? `${school.distance_km.toFixed(1)}km` : null,
        ofsted_label: getOfstedLabel(school.ofsted_rating),
        overall_rating: school.overall_rating ? parseFloat(school.overall_rating) : null,
        rating_display: school.overall_rating ? 
          (parseFloat(school.overall_rating) >= 10 ? '10' : `${parseFloat(school.overall_rating).toFixed(1)}`) + '/10' 
          : 'N/A'
      }))
    });
    
  } catch (error) {
    console.error('Nearby search error:', error);
    res.status(500).json({ 
      error: 'Failed to search nearby schools',
      message: error.message 
    });
  }
});



/**
 * @route   GET /api/search/postcode/:postcode
 * @desc    Search schools by specific postcode
 * @example /api/search/postcode/SW1A%201AA
 */
router.get('/postcode/:postcode', async (req, res) => {
  try {
    const { postcode } = req.params;
    const { radius = 3 } = req.query;

    // Now using stored overall_rating
    const sqlQuery = `
      SELECT 
        s.*,
        o.overall_effectiveness as ofsted_rating,
        COALESCE(c.number_on_roll, s.total_pupils) AS number_on_roll
      FROM uk_schools s
      LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
      LEFT JOIN uk_census_data c ON s.urn = c.urn
      WHERE UPPER(SUBSTRING(s.postcode, 1, 4)) = UPPER(SUBSTRING($1, 1, 4))
      ORDER BY s.overall_rating DESC NULLS LAST, s.name
      LIMIT 50
    `;
    
    const result = await query(sqlQuery, [postcode]);
    
    res.json({
      success: true,
      postcode: postcode,
      radius: radius,
      total: result.rows.length,
      schools: result.rows.map(school => ({
        ...school,
        ofsted_label: getOfstedLabel(school.ofsted_rating),
        overall_rating: school.overall_rating ? parseFloat(school.overall_rating) : null,
        rating_display: school.overall_rating ? `${parseFloat(school.overall_rating).toFixed(1)}/10` : 'N/A'
      }))
    });

  } catch (error) {
    console.error('Postcode search error:', error);
    res.status(500).json({ 
      error: 'Failed to search by postcode',
      message: error.message 
    });
  }
});

/**
 * @route   GET /api/search/city/:city
 * @desc    Get top schools for a city/town
 * @example /api/search/city/london?limit=10
 */
router.get('/city/:city', async (req, res) => {
  try {
    const { city } = req.params;
    const { limit = 10, phase } = req.query;

    let sqlQuery = `
      SELECT 
        s.urn,
        s.name,
        s.postcode,
        s.town,
        s.phase_of_education,
        s.type_of_establishment,
        COALESCE(s.overall_rating, 5.0) as overall_rating,  -- Use stored rating or default to 5
        s.rating_percentile,
        o.overall_effectiveness as ofsted_rating,
        COALESCE(c.number_on_roll, s.total_pupils) AS number_on_roll
      FROM uk_schools s
      LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
      LEFT JOIN uk_census_data c ON s.urn = c.urn
      WHERE LOWER(s.town) = LOWER($1) OR LOWER(s.local_authority) = LOWER($1)
    `;

    const params = [city];
    
    if (phase) {
      sqlQuery += ` AND s.phase_of_education = $2`;
      params.push(phase);
    }

    sqlQuery += ` ORDER BY s.overall_rating DESC NULLS LAST, o.overall_effectiveness ASC NULLS LAST`;
    sqlQuery += ` LIMIT $${params.length + 1}`;
    params.push(parseInt(limit));

    const result = await query(sqlQuery, params);

    // Get city statistics
    const statsSql = `
      SELECT 
        COUNT(DISTINCT s.urn) as total_schools,
        COUNT(DISTINCT CASE WHEN s.phase_of_education = 'Primary' THEN s.urn END) as primary_count,
        COUNT(DISTINCT CASE WHEN s.phase_of_education = 'Secondary' THEN s.urn END) as secondary_count,
        COUNT(DISTINCT CASE WHEN o.overall_effectiveness = 1 THEN s.urn END) as outstanding_count,
        COUNT(DISTINCT CASE WHEN o.overall_effectiveness = 2 THEN s.urn END) as good_count,
        AVG(s.overall_rating) as avg_rating
      FROM uk_schools s
      LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
      WHERE LOWER(s.town) = LOWER($1) OR LOWER(s.local_authority) = LOWER($1)
    `;

    const statsResult = await query(statsSql, [city]);
    const stats = statsResult.rows[0];

    res.json({
      success: true,
      city: city,
      statistics: {
        total_schools: parseInt(stats.total_schools) || 0,
        primary_schools: parseInt(stats.primary_count) || 0,
        secondary_schools: parseInt(stats.secondary_count) || 0,
        outstanding_schools: parseInt(stats.outstanding_count) || 0,
        good_schools: parseInt(stats.good_count) || 0,
        average_rating: stats.avg_rating ? parseFloat(stats.avg_rating).toFixed(1) : null
      },
      top_schools: result.rows.map(school => ({
        ...school,
        ofsted_label: getOfstedLabel(school.ofsted_rating),
        overall_rating: school.overall_rating ? parseFloat(school.overall_rating) : null,
        rating_display: school.overall_rating ? `${parseFloat(school.overall_rating).toFixed(1)}/10` : 'N/A'
      }))
    });

  } catch (error) {
    console.error('City search error:', error);
    res.status(500).json({ 
      error: 'Failed to get schools for city',
      message: error.message 
    });
  }
});

/**
 * @route   GET /api/search/suggestions
 * @desc    Get autocomplete suggestions
 * @query   q (search term)
 * @example /api/search/suggestions?q=West
 */
router.get('/suggestions', async (req, res) => {
  try {
    const { q } = req.query;
    
    if (!q || q.length < 2) {
      return res.json({ suggestions: [] });
    }

    const sqlQuery = `
      (
        SELECT DISTINCT 
          name as suggestion,
          'school' as type,
          urn as id,
          overall_rating
        FROM uk_schools
        WHERE LOWER(name) LIKE LOWER($1)
        ORDER BY overall_rating DESC NULLS LAST
        LIMIT 5
      )
      UNION ALL
      (
        SELECT DISTINCT 
          town as suggestion,
          'town' as type,
          NULL as id,
          NULL as overall_rating
        FROM uk_schools
        WHERE LOWER(town) LIKE LOWER($1)
        AND town IS NOT NULL
        LIMIT 3
      )
      UNION ALL
      (
        SELECT DISTINCT 
          local_authority as suggestion,
          'la' as type,
          NULL as id,
          NULL as overall_rating
        FROM uk_schools
        WHERE LOWER(local_authority) LIKE LOWER($1)
        AND local_authority IS NOT NULL
        LIMIT 2
      )
      LIMIT 10
    `;
    
    const result = await query(sqlQuery, [`${q}%`]);
    
    res.json({
      success: true,
      suggestions: result.rows.map(row => ({
        ...row,
        rating_display: row.overall_rating ? `${parseFloat(row.overall_rating).toFixed(1)}/10` : null
      }))
    });

  } catch (error) {
    console.error('Suggestions error:', error);
    res.status(500).json({ 
      error: 'Failed to get suggestions',
      message: error.message 
    });
  }
});

/**
 * @route   GET /api/search/filters
 * @desc    Get available filter options
 * @example /api/search/filters
 */
router.get('/filters', async (req, res) => {
  try {
    // Get unique phases
    const phasesQuery = `
      SELECT DISTINCT phase_of_education as value, COUNT(*) as count
      FROM uk_schools
      WHERE phase_of_education IS NOT NULL
      GROUP BY phase_of_education
      ORDER BY count DESC
    `;
    
    // Get unique local authorities
    const laQuery = `
      SELECT DISTINCT local_authority as value, COUNT(*) as count
      FROM uk_schools
      WHERE local_authority IS NOT NULL
      GROUP BY local_authority
      ORDER BY local_authority
    `;
    
    // Get unique school types
    const typesQuery = `
      SELECT DISTINCT type_of_establishment as value, COUNT(*) as count
      FROM uk_schools
      WHERE type_of_establishment IS NOT NULL
      GROUP BY type_of_establishment
      ORDER BY count DESC
      LIMIT 20
    `;

    const [phases, localAuthorities, types] = await Promise.all([
      query(phasesQuery),
      query(laQuery),
      query(typesQuery)
    ]);

    res.json({
      success: true,
      filters: {
        phases: phases.rows,
        localAuthorities: localAuthorities.rows,
        types: types.rows,
        ofstedRatings: [
          { value: 1, label: 'Outstanding', count: null },
          { value: 2, label: 'Good', count: null },
          { value: 3, label: 'Requires Improvement', count: null },
          { value: 4, label: 'Inadequate', count: null }
        ]
      }
    });

  } catch (error) {
    console.error('Filters error:', error);
    res.status(500).json({ 
      error: 'Failed to get filters',
      message: error.message 
    });
  }
});

// Helper function to convert Ofsted rating to label
function getOfstedLabel(rating) {
  const labels = {
    1: 'Outstanding',
    2: 'Good',
    3: 'Requires Improvement',
    4: 'Inadequate'
  };
  return labels[rating] || 'Not Inspected';
}

module.exports = router;
