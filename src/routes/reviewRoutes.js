// src/routes/reviewRoutes.js
const express = require('express');
const router = express.Router();
const { pool } = require('../config/database');

/* --------------------------------- utils --------------------------------- */
const toInt = (v, def) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
};
const clamp = (n, min, max) => Math.min(Math.max(n, min), max);
const clientIP = (req) => req.ip || req.connection?.remoteAddress || '';

/* ======================== GET: school reviews list ======================== */
router.get('/schools/:urn/reviews', async (req, res) => {
  const { urn } = req.params;

  // Sanitize paging/sort parameters
  const page = Math.max(toInt(req.query.page ?? '1', 10), 1);
  const limit = clamp(Math.max(toInt(req.query.limit ?? '20', 10), 1), 1, 50);
  const offset = (page - 1) * limit;

  const sort = (req.query.sort || 'recent').toString();
  let orderBy = 'r.created_at DESC';
  if (sort === 'helpful') orderBy = 'r.helpful_count DESC, r.created_at DESC';
  else if (sort === 'rating_high') orderBy = 'r.overall_rating DESC, r.created_at DESC';
  else if (sort === 'rating_low') orderBy = 'r.overall_rating ASC, r.created_at DESC';

  try {
    // 1) Get precomputed stats if available
    const statsRow = await pool.query(
      'SELECT * FROM uk_school_review_stats WHERE urn = $1',
      [urn]
    );

    // 2) Get total count for pagination
    const countQ = await pool.query(
      `SELECT COUNT(*)::int AS total
       FROM uk_school_reviews
       WHERE urn = $1 AND COALESCE(is_published, true) = true`,
      [urn]
    );
    const total = countQ.rows[0]?.total ?? 0;
    const totalPages = Math.max(1, Math.ceil(total / limit));

    // 3) Get the actual reviews
    const reviewsQ = await pool.query(
      `
      SELECT 
        r.*,
        s.name AS school_name,
        s.town AS town,
        TRIM(TO_CHAR(r.created_at, 'Mon DD, YYYY')) AS formatted_date
      FROM uk_school_reviews r
      JOIN uk_schools s ON r.urn = s.urn
      WHERE r.urn = $1
        AND COALESCE(r.is_published, true) = true
      ORDER BY ${orderBy}
      LIMIT $2 OFFSET $3
      `,
      [urn, limit, offset]
    );

    // 4) Process stats - transform existing or compute from scratch
    let stats = null;
    
    if (statsRow.rows[0]) {
      // We have precomputed stats - transform them to frontend format
      const dbStats = statsRow.rows[0];
      
      // Calculate distribution from actual reviews on this page
      const distribution = { 5: 0, 4: 0, 3: 0, 2: 0, 1: 0 };
      
      // Get ALL reviews for accurate distribution (not just current page)
      const allReviewsQ = await pool.query(
        `SELECT overall_rating, learning_rating, teaching_rating, safety_rating,
                social_emotional_rating, special_education_rating, family_engagement_rating
         FROM uk_school_reviews 
         WHERE urn = $1 AND COALESCE(is_published, true) = true`,
        [urn]
      );
      
      allReviewsQ.rows.forEach(review => {
        if (review.overall_rating) {
          distribution[review.overall_rating]++;
        }
      });

      // Transform flat structure to nested structure
      stats = {
        urn: dbStats.urn,
        total_reviews: dbStats.total_reviews,
        avg_overall_rating: dbStats.avg_overall_rating,
        recommendation_percentage: dbStats.recommendation_percentage,
        distribution: distribution,
        categories: {
          family: {
            average: dbStats.avg_family_engagement_rating,
            count: allReviewsQ.rows.filter(r => r.family_engagement_rating !== null).length
          },
          learning: {
            average: dbStats.avg_learning_rating,
            count: allReviewsQ.rows.filter(r => r.learning_rating !== null).length
          },
          teaching: {
            average: dbStats.avg_teaching_rating,
            count: allReviewsQ.rows.filter(r => r.teaching_rating !== null).length
          },
          safety: {
            average: dbStats.avg_safety_rating,
            count: allReviewsQ.rows.filter(r => r.safety_rating !== null).length
          },
          social: {
            average: dbStats.avg_social_emotional_rating,
            count: allReviewsQ.rows.filter(r => r.social_emotional_rating !== null).length
          },
          special: {
            average: dbStats.avg_special_education_rating,
            count: allReviewsQ.rows.filter(r => r.special_education_rating !== null).length
          }
        }
      };
    } else {
      // No precomputed stats - calculate everything from scratch
      const distQ = await pool.query(
        `
        SELECT
          COUNT(*)::int AS total_reviews,
          ROUND(AVG(overall_rating)::numeric, 1) AS avg_overall_rating,
          SUM(CASE WHEN overall_rating=5 THEN 1 ELSE 0 END)::int AS star5,
          SUM(CASE WHEN overall_rating=4 THEN 1 ELSE 0 END)::int AS star4,
          SUM(CASE WHEN overall_rating=3 THEN 1 ELSE 0 END)::int AS star3,
          SUM(CASE WHEN overall_rating=2 THEN 1 ELSE 0 END)::int AS star2,
          SUM(CASE WHEN overall_rating=1 THEN 1 ELSE 0 END)::int AS star1,
          ROUND(AVG(NULLIF(family_engagement_rating,0))::numeric,1) AS family_avg,
          COUNT(NULLIF(family_engagement_rating,0))::int AS family_count,
          ROUND(AVG(NULLIF(learning_rating,0))::numeric,1) AS learning_avg,
          COUNT(NULLIF(learning_rating,0))::int AS learning_count,
          ROUND(AVG(NULLIF(teaching_rating,0))::numeric,1) AS teaching_avg,
          COUNT(NULLIF(teaching_rating,0))::int AS teaching_count,
          ROUND(AVG(NULLIF(safety_rating,0))::numeric,1) AS safety_avg,
          COUNT(NULLIF(safety_rating,0))::int AS safety_count,
          ROUND(AVG(NULLIF(social_emotional_rating,0))::numeric,1) AS social_avg,
          COUNT(NULLIF(social_emotional_rating,0))::int AS social_count,
          ROUND(AVG(NULLIF(special_education_rating,0))::numeric,1) AS special_avg,
          COUNT(NULLIF(special_education_rating,0))::int AS special_count,
          ROUND(100.0 * AVG(CASE WHEN would_recommend THEN 1 ELSE 0 END)::numeric, 0) AS recommendation_percentage
        FROM uk_school_reviews
        WHERE urn = $1 AND COALESCE(is_published, true) = true
        `,
        [urn]
      );
      
      const d = distQ.rows[0] || {};
      
      stats = {
        urn: Number(urn),
        total_reviews: d.total_reviews || 0,
        avg_overall_rating: d.avg_overall_rating ?? null,
        recommendation_percentage: d.recommendation_percentage ?? 0,
        distribution: {
          5: d.star5 || 0,
          4: d.star4 || 0,
          3: d.star3 || 0,
          2: d.star2 || 0,
          1: d.star1 || 0
        },
        categories: {
          family: { average: d.family_avg, count: d.family_count },
          learning: { average: d.learning_avg, count: d.learning_count },
          teaching: { average: d.teaching_avg, count: d.teaching_count },
          safety: { average: d.safety_avg, count: d.safety_count },
          social: { average: d.social_avg, count: d.social_count },
          special: { average: d.special_avg, count: d.special_count }
        }
      };
    }

    // Send the response
    res.json({
      stats,
      reviews: reviewsQ.rows,
      pagination: { page, limit, total, totalPages }
    });
    
  } catch (err) {
    console.error('Error fetching reviews:', err);
    res.status(500).json({ error: 'Failed to fetch reviews' });
  }
});

/* ======================== POST: submit a new review ======================= */
router.post('/schools/:urn/reviews', async (req, res) => {
  const { urn } = req.params;
  const ip = clientIP(req);

  const {
    overall_rating,
    learning_rating,
    teaching_rating,
    social_emotional_rating,
    special_education_rating,
    safety_rating,
    family_engagement_rating,
    would_recommend,
    review_text,
    review_title,
    reviewer_type,
    reviewer_name
  } = req.body || {};

  try {
    // required fields
    if (
      overall_rating == null ||
      would_recommend == null ||
      !review_text ||
      !reviewer_type
    ) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    if (review_text.length < 50 || review_text.length > 2500) {
      return res.status(400).json({ error: 'Review must be between 50 and 2500 characters' });
    }

    // rate limit: 1 per IP per school per 24h
    const rl = await pool.query(
      `SELECT COUNT(*)::int AS cnt
       FROM uk_school_reviews
       WHERE urn = $1 AND reviewer_ip = $2
         AND created_at > NOW() - INTERVAL '24 hours'`,
      [urn, ip]
    );
    if ((rl.rows[0]?.cnt || 0) > 0) {
      return res.status(429).json({ error: 'You can only submit one review per school per day' });
    }

    const insert = await pool.query(
      `
      INSERT INTO uk_school_reviews (
        urn, overall_rating, learning_rating, teaching_rating,
        social_emotional_rating, special_education_rating, safety_rating,
        family_engagement_rating, would_recommend, review_text, review_title,
        reviewer_type, reviewer_name, reviewer_ip, is_published
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14, true)
      RETURNING *
      `,
      [
        urn, overall_rating, learning_rating, teaching_rating,
        social_emotional_rating, special_education_rating, safety_rating,
        family_engagement_rating, would_recommend, review_text, review_title,
        reviewer_type, reviewer_name, ip
      ]
    );

    res.status(201).json({ success: true, review: insert.rows[0] });
  } catch (err) {
    console.error('Error submitting review:', err);
    res.status(500).json({ error: 'Failed to submit review' });
  }
});

/* ==================== POST: mark a review as helpful ===================== */
router.post('/reviews/:reviewId/helpful', async (req, res) => {
  const { reviewId } = req.params;
  const ip = clientIP(req);

  try {
    const already = await pool.query(
      `SELECT 1 FROM uk_review_helpful_votes WHERE review_id = $1 AND voter_ip = $2`,
      [reviewId, ip]
    );
    if (already.rows.length) {
      return res.status(400).json({ error: 'You have already marked this review as helpful' });
    }

    await pool.query('BEGIN');
    await pool.query(
      `INSERT INTO uk_review_helpful_votes (review_id, voter_ip) VALUES ($1, $2)`,
      [reviewId, ip]
    );
    const upd = await pool.query(
      `UPDATE uk_school_reviews
       SET helpful_count = COALESCE(helpful_count,0) + 1
       WHERE id = $1
       RETURNING id, helpful_count`,
      [reviewId]
    );
    await pool.query('COMMIT');

    if (!upd.rowCount) return res.status(404).json({ error: 'Review not found' });
    res.json({ success: true, id: upd.rows[0].id, helpful_count: upd.rows[0].helpful_count });
  } catch (err) {
    await pool.query('ROLLBACK');
    console.error('Error marking review helpful:', err);
    res.status(500).json({ error: 'Failed to mark review as helpful' });
  }
});

/* ========================= POST: report a review ========================= */
router.post('/reviews/:reviewId/report', async (req, res) => {
  const { reviewId } = req.params;
  const { reason, details } = req.body || {};
  const ip = clientIP(req);

  if (!reason) return res.status(400).json({ error: 'Report reason is required' });

  try {
    await pool.query(
      `INSERT INTO uk_review_reports (review_id, report_reason, report_details, reporter_ip)
       VALUES ($1, $2, $3, $4)`,
      [reviewId, reason, details || null, ip]
    );

    const upd = await pool.query(
      `UPDATE uk_school_reviews
       SET report_count = COALESCE(report_count,0) + 1
       WHERE id = $1
       RETURNING id, report_count`,
      [reviewId]
    );

    if (!upd.rowCount) return res.status(404).json({ error: 'Review not found' });
    res.json({ success: true, message: 'Review has been reported for moderation' });
  } catch (err) {
    console.error('Error reporting review:', err);
    res.status(500).json({ error: 'Failed to report review' });
  }
});

/* ===================== GET: standalone review statistics ================== */
router.get('/schools/:urn/review-stats', async (req, res) => {
  const { urn } = req.params;
  try {
    const q = await pool.query(
      `
      SELECT 
        rs.*,
        s.name AS school_name,
        s.postcode,
        s.town
      FROM uk_school_review_stats rs
      JOIN uk_schools s ON rs.urn = s.urn
      WHERE rs.urn = $1
      `,
      [urn]
    );

    if (!q.rows.length) {
      return res.json({
        urn: Number(urn),
        total_reviews: 0,
        avg_overall_rating: null,
        recommendation_percentage: 0
      });
    }

    res.json(q.rows[0]);
  } catch (err) {
    console.error('Error fetching review stats:', err);
    res.status(500).json({ error: 'Failed to fetch review statistics' });
  }
});

module.exports = router;