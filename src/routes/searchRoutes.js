const express = require("express");
const router = express.Router();
const { query } = require("../config/database");

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 100;
const MAX_SUGGEST_LIMIT = 20;
const MAX_RADIUS_KM = 50;

const CATEGORY_PATTERNS = {
  ecole: ["Ec%"],
  college: ["Col%"],
  lycee: ["Ly%"],
  specialise: [
    "M%cico-social%",
    "EREA%",
    "Service Administratif%",
    "Information et orientation%",
    "Autre%"
  ]
};

const STATUS_PATTERNS = {
  public: ["Public%"],
  prive: ["Priv%"]
};

function parseList(value) {
  if (!value) return [];
  return String(value)
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function clampLimit(value, fallback = DEFAULT_LIMIT) {
  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, MAX_LIMIT);
}

function clampOffset(value) {
  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed < 0) return 0;
  return parsed;
}

function sanitizeLikeTerm(term) {
  return `%${term.replace(/[%_]/g, "").trim()}%`;
}

function sanitizePrefixTerm(term) {
  return `${term.replace(/[%_]/g, "").trim()}%`;
}

function applyPatternGroup(selected, patternMap, params) {
  if (!selected.length) return null;

  const clauses = [];
  for (const key of selected) {
    const patterns = patternMap[key];
    if (!patterns || !patterns.length) continue;
    const patternClauses = [];
    for (const pattern of patterns) {
      params.push(pattern);
      patternClauses.push(`e.type_etablissement ILIKE $${params.length}`);
    }
    if (patternClauses.length) {
      clauses.push(`(${patternClauses.join(" OR ")})`);
    }
  }

  if (!clauses.length) return null;
  return `(${clauses.join(" OR ")})`;
}

function applyStatusGroup(selected, params) {
  if (!selected.length) return null;
  const clauses = [];
  for (const key of selected) {
    const patterns = STATUS_PATTERNS[key];
    if (!patterns || !patterns.length) continue;
    const patternClauses = [];
    for (const pattern of patterns) {
      params.push(pattern);
      patternClauses.push(`e.statut_public_prive ILIKE $${params.length}`);
    }
    if (patternClauses.length) {
      clauses.push(`(${patternClauses.join(" OR ")})`);
    }
  }

  if (!clauses.length) return null;
  return `(${clauses.join(" OR ")})`;
}

function normalizeRating(value) {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function toFloat(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function toInt(value) {
  const num = parseInt(value, 10);
  return Number.isNaN(num) ? null : num;
}

function mapSchoolRow(row) {
  const avgReview = normalizeRating(row.avg_overall_rating);
  const ratingOn10 = avgReview !== null ? Math.round(avgReview * 20) / 10 : null;
  const studentCount = toInt(row.nombre_d_eleves);
  const recommendation = normalizeRating(row.recommendation_percentage);

  const addressParts = [
    row.adresse_1,
    row.adresse_2,
    row.adresse_3,
    row.code_postal,
    row.commune
  ].filter(Boolean);

  return {
    urn: row.uai,
    uai: row.uai,
    name: row.name,
    slug: row.name ? row.name.toLowerCase() : null,
    town: row.commune,
    postcode: row.code_postal,
    departement: row.departement,
    region: row.region,
    academie: row.academie,
    statut_public_prive: row.statut_public_prive,
    type_etablissement: row.type_etablissement,
    type_of_establishment: row.type_etablissement,
    phase_of_education: row.statut_public_prive,
    contract_type: row.type_contrat_prive,
    number_on_roll: studentCount,
    number_of_students: studentCount,
    overall_rating: ratingOn10,
    avg_review_rating: avgReview,
    total_reviews: toInt(row.total_reviews) || 0,
    recommendation_percentage: recommendation,
    latitude: row.latitude !== null ? toFloat(row.latitude) : null,
    longitude: row.longitude !== null ? toFloat(row.longitude) : null,
    adresse_ligne1: row.adresse_1,
    adresse_ligne2: row.adresse_2,
    adresse_ligne3: row.adresse_3,
    address: addressParts.join(", ") || null
  };
}

async function runQuery(sql, params) {
  return query(sql, params);
}

router.get("/suggest", async (req, res) => {
  const rawQ = String(req.query.q || "").trim();
  const limit = Math.min(parseInt(req.query.limit, 10) || 8, MAX_SUGGEST_LIMIT);

  if (rawQ.length < 2) {
    return res.json({ schools: [], cities: [], authorities: [], postcodes: [] });
  }

  const likeTerm = sanitizeLikeTerm(rawQ);
  const prefixTerm = sanitizePrefixTerm(rawQ);

  try {
    const [schoolsRes, communesRes, departementsRes, postcodesRes] = await Promise.all([
      runQuery(
        `
          SELECT 
            e.identifiant_de_l_etablissement AS uai,
            e.nom_etablissement AS name,
            e.nom_commune AS commune,
            e.code_postal,
            e.libelle_departement AS departement,
            e.libelle_region AS region
          FROM fr_ecoles e
          WHERE e.nom_etablissement ILIKE $1
          ORDER BY e.nom_etablissement ASC
          LIMIT $2
        `,
        [likeTerm, limit]
      ),
      runQuery(
        `
          SELECT 
            e.nom_commune AS commune,
            e.libelle_departement AS departement,
            e.libelle_region AS region,
            COUNT(*) AS total
          FROM fr_ecoles e
          WHERE e.nom_commune ILIKE $1
          GROUP BY e.nom_commune, e.libelle_departement, e.libelle_region
          ORDER BY total DESC, commune ASC
          LIMIT $2
        `,
        [likeTerm, limit]
      ),
      runQuery(
        `
          SELECT 
            e.libelle_departement AS departement,
            e.libelle_region AS region,
            COUNT(*) AS total
          FROM fr_ecoles e
          WHERE e.libelle_departement ILIKE $1
          GROUP BY e.libelle_departement, e.libelle_region
          ORDER BY total DESC, departement ASC
          LIMIT $2
        `,
        [likeTerm, Math.max(5, Math.floor(limit / 2))]
      ),
      runQuery(
        `
          SELECT DISTINCT e.code_postal AS code_postal
          FROM fr_ecoles e
          WHERE e.code_postal ILIKE $1
          ORDER BY code_postal ASC
          LIMIT $2
        `,
        [prefixTerm, Math.max(4, Math.floor(limit / 2))]
      )
    ]);

    res.json({
      schools: schoolsRes.rows.map((row) => ({
        urn: row.uai,
        name: row.name,
        town: row.commune,
        postcode: row.code_postal,
        departement: row.departement,
        region: row.region
      })),
      cities: communesRes.rows.map((row) => ({
        town: row.commune,
        departement: row.departement,
        region: row.region,
        count: parseInt(row.total, 10)
      })),
      authorities: departementsRes.rows.map((row) => ({
        local_authority: row.departement,
        region: row.region,
        count: parseInt(row.total, 10)
      })),
      postcodes: postcodesRes.rows.map((row) => ({ postcode: row.code_postal }))
    });
  } catch (error) {
    console.error("suggest error", error);
    res.json({ schools: [], cities: [], authorities: [], postcodes: [] });
  }
});

router.get("/school-autocomplete", async (req, res) => {
  const rawQ = String(req.query.q || "").trim();
  const limit = Math.min(parseInt(req.query.limit, 10) || 8, MAX_SUGGEST_LIMIT);

  if (rawQ.length < 2) {
    return res.json({ schools: [] });
  }

  const likeTerm = sanitizeLikeTerm(rawQ);

  try {
    const { rows } = await runQuery(
      `
        SELECT 
          e.identifiant_de_l_etablissement AS uai,
          e.nom_etablissement AS name,
          e.nom_commune AS commune,
          e.code_postal,
          e.type_etablissement,
          e.statut_public_prive
        FROM fr_ecoles e
        WHERE 
          e.nom_etablissement ILIKE $1
          OR e.code_postal ILIKE $1
          OR e.nom_commune ILIKE $1
        ORDER BY e.nom_etablissement ASC
        LIMIT $2
      `,
      [likeTerm, limit]
    );

    const schools = rows.map((row) => ({
      urn: row.uai,
      name: row.name,
      town: row.commune,
      postcode: row.code_postal,
      type_etablissement: row.type_etablissement,
      statut_public_prive: row.statut_public_prive
    }));

    res.json({ schools });
  } catch (error) {
    console.error("school-autocomplete error", error);
    res.json({ schools: [] });
  }
});

router.get("/", async (req, res) => {
  try {
    const rawQ = String(req.query.q || "").trim();
    const type = String(req.query.type || "all").toLowerCase();
    const limit = clampLimit(req.query.limit);
    const offset = clampOffset(req.query.offset);

    if (rawQ.length < 2) {
      return res.status(400).json({
        error: "La recherche doit contenir au moins 2 caracteres"
      });
    }

    const params = [];
    const whereClauses = [];

    const cleaned = rawQ.replace(/\s+/g, " ").trim();
    const likeTerm = sanitizeLikeTerm(cleaned);
    const prefixTerm = sanitizePrefixTerm(cleaned);

    if (type === "name") {
      params.push(likeTerm);
      whereClauses.push(`e.nom_etablissement ILIKE $${params.length}`);
    } else if (type === "postcode") {
      params.push(prefixTerm);
      whereClauses.push(`e.code_postal ILIKE $${params.length}`);
    } else if (type === "location") {
      const isNumeric = /^\d{2,5}$/.test(cleaned);
      if (isNumeric) {
        params.push(prefixTerm);
        whereClauses.push(`e.code_postal ILIKE $${params.length}`);
      } else {
        params.push(likeTerm);
        params.push(likeTerm);
        params.push(likeTerm);
        const startIndex = params.length - 2;
        whereClauses.push(`(
          e.nom_commune ILIKE $${startIndex}
          OR e.libelle_departement ILIKE $${startIndex + 1}
          OR e.libelle_region ILIKE $${startIndex + 2}
        )`);
      }
    } else {
      params.push(likeTerm);
      const nameIdx = params.length;
      params.push(likeTerm);
      const communeIdx = params.length;
      params.push(likeTerm);
      const departementIdx = params.length;
      params.push(prefixTerm);
      const postalIdx = params.length;

      whereClauses.push(`(
        e.nom_etablissement ILIKE $${nameIdx}
        OR e.nom_commune ILIKE $${communeIdx}
        OR e.libelle_departement ILIKE $${departementIdx}
        OR e.code_postal ILIKE $${postalIdx}
      )`);
    }

    const categories = parseList(req.query.categories);
    const categoryClause = applyPatternGroup(categories, CATEGORY_PATTERNS, params);
    if (categoryClause) {
      whereClauses.push(categoryClause);
    }

    const statuses = parseList(req.query.status);
    const statusClause = applyStatusGroup(statuses, params);
    if (statusClause) {
      whereClauses.push(statusClause);
    }

    const minRating = normalizeRating(req.query.minRating);
    if (minRating !== null && minRating > 0) {
      params.push(minRating);
      whereClauses.push(`stats.avg_overall_rating >= $${params.length}`);
    }

    const whereSql = whereClauses.length ? `WHERE ${whereClauses.join(" AND ")}` : "";

    params.push(limit);
    params.push(offset);

    const sql = `
      SELECT
        e.identifiant_de_l_etablissement AS uai,
        e.nom_etablissement AS name,
        e.nom_commune AS commune,
        e.code_postal,
        e.type_etablissement,
        e.statut_public_prive,
        e.type_contrat_prive,
        e.libelle_departement AS departement,
        e.libelle_academie AS academie,
        e.libelle_region AS region,
        e.adresse_1,
        e.adresse_2,
        e.adresse_3,
        NULLIF(e.nombre_d_eleves, '')::int AS nombre_d_eleves,
        NULLIF(e.latitude, '')::double precision AS latitude,
        NULLIF(e.longitude, '')::double precision AS longitude,
        stats.avg_overall_rating,
        stats.total_reviews,
        stats.recommendation_percentage,
        COUNT(*) OVER() AS total_count
      FROM fr_ecoles e
      LEFT JOIN fr_school_review_stats stats ON stats.uai = e.identifiant_de_l_etablissement
      ${whereSql}
      ORDER BY
        COALESCE(stats.avg_overall_rating, 0) DESC,
        COALESCE(NULLIF(e.nombre_d_eleves, '')::int, 0) DESC,
        e.nom_etablissement ASC
      LIMIT $${params.length - 1}
      OFFSET $${params.length}
    `;

    const { rows } = await runQuery(sql, params);
    const total = rows.length ? parseInt(rows[0].total_count, 10) : 0;

    res.json({
      success: true,
      query: cleaned,
      type,
      total,
      limit,
      offset,
      schools: rows.map(mapSchoolRow)
    });
  } catch (error) {
    console.error("Search error", error);
    res.status(500).json({
      error: "Recherche indisponible",
      message: error.message
    });
  }
});

router.get("/nearby", async (req, res) => {
  try {
    const lat = Number(req.query.lat);
    const lng = Number(req.query.lng);
    const radius = Math.min(Number(req.query.radius) || 5, MAX_RADIUS_KM);
    const limit = clampLimit(req.query.limit || 100, 100);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return res.status(400).json({ error: "Latitude et longitude sont requises" });
    }

    const params = [lat, lng, radius, limit];

    const sql = `
      WITH school_distances AS (
        SELECT
          e.identifiant_de_l_etablissement AS uai,
          e.nom_etablissement AS name,
          e.nom_commune AS commune,
          e.code_postal,
          e.type_etablissement,
          e.statut_public_prive,
          e.type_contrat_prive,
          NULLIF(e.latitude, '')::double precision AS latitude,
          NULLIF(e.longitude, '')::double precision AS longitude,
          NULLIF(e.nombre_d_eleves, '')::int AS nombre_d_eleves,
          stats.avg_overall_rating,
          stats.total_reviews,
          stats.recommendation_percentage,
          e.libelle_departement AS departement,
          e.libelle_academie AS academie,
          e.libelle_region AS region,
          e.adresse_1,
          e.adresse_2,
          e.adresse_3,
          (
            6371 * acos(
              LEAST(
                1.0,
                cos(radians($1)) * cos(radians(NULLIF(e.latitude, '')::double precision)) *
                cos(radians(NULLIF(e.longitude, '')::double precision) - radians($2)) +
                sin(radians($1)) * sin(radians(NULLIF(e.latitude, '')::double precision))
              )
            )
          ) AS distance_km
        FROM fr_ecoles e
        LEFT JOIN fr_school_review_stats stats ON stats.uai = e.identifiant_de_l_etablissement
        WHERE
          NULLIF(e.latitude, '') IS NOT NULL
          AND NULLIF(e.longitude, '') IS NOT NULL
          AND NULLIF(e.latitude, '')::double precision BETWEEN $1 - ($3 / 111.0) AND $1 + ($3 / 111.0)
          AND NULLIF(e.longitude, '')::double precision BETWEEN $2 - ($3 / (111.0 * cos(radians($1)))) AND $2 + ($3 / (111.0 * cos(radians($1))))
      )
      SELECT *
      FROM school_distances
      WHERE distance_km <= $3
      ORDER BY distance_km ASC
      LIMIT $4
    `;

    const { rows } = await runQuery(sql, params);

    res.json({
      success: true,
      center: { lat, lng },
      radius,
      total: rows.length,
      schools: rows.map((row) => ({
        ...mapSchoolRow(row),
        distance: row.distance_km !== null && row.distance_km !== undefined
          ? `${Number(row.distance_km).toFixed(1)} km`
          : null
      }))
    });
  } catch (error) {
    console.error("Nearby search error", error);
    res.status(500).json({
      error: "La recherche geographique a echoue",
      message: error.message
    });
  }
});

router.get("/postcode/:postcode", async (req, res) => {
  try {
    const postcode = String(req.params.postcode || "").trim();
    const limit = clampLimit(req.query.limit || 50, 50);
    if (!postcode) {
      return res.status(400).json({ error: "Code postal requis" });
    }

    const prefix = sanitizePrefixTerm(postcode);

    const { rows } = await runQuery(
      `
        SELECT
          e.identifiant_de_l_etablissement AS uai,
          e.nom_etablissement AS name,
          e.nom_commune AS commune,
          e.code_postal,
          e.type_etablissement,
          e.statut_public_prive,
          e.type_contrat_prive,
          e.libelle_departement AS departement,
          e.libelle_academie AS academie,
          e.libelle_region AS region,
          e.adresse_1,
          e.adresse_2,
          e.adresse_3,
          NULLIF(e.nombre_d_eleves, '')::int AS nombre_d_eleves,
          NULLIF(e.latitude, '')::double precision AS latitude,
          NULLIF(e.longitude, '')::double precision AS longitude,
          stats.avg_overall_rating,
          stats.total_reviews,
          stats.recommendation_percentage
        FROM fr_ecoles e
        LEFT JOIN fr_school_review_stats stats ON stats.uai = e.identifiant_de_l_etablissement
        WHERE e.code_postal ILIKE $1
        ORDER BY e.nom_etablissement ASC
        LIMIT $2
      `,
      [prefix, limit]
    );

    res.json({
      success: true,
      postcode,
      total: rows.length,
      schools: rows.map(mapSchoolRow)
    });
  } catch (error) {
    console.error("Postcode search error", error);
    res.status(500).json({
      error: "Recherche par code postal indisponible",
      message: error.message
    });
  }
});

router.get("/city/:city", async (req, res) => {
  try {
    const city = String(req.params.city || "").trim();
    const limit = clampLimit(req.query.limit || 20, 50);
    if (city.length < 2) {
      return res.status(400).json({ error: "Nom de commune invalide" });
    }

    const likeTerm = sanitizeLikeTerm(city);

    const { rows } = await runQuery(
      `
        SELECT
          e.identifiant_de_l_etablissement AS uai,
          e.nom_etablissement AS name,
          e.nom_commune AS commune,
          e.code_postal,
          e.type_etablissement,
          e.statut_public_prive,
          e.type_contrat_prive,
          e.libelle_departement AS departement,
          e.libelle_academie AS academie,
          e.libelle_region AS region,
          NULLIF(e.nombre_d_eleves, '')::int AS nombre_d_eleves,
          stats.avg_overall_rating,
          stats.total_reviews,
          stats.recommendation_percentage
        FROM fr_ecoles e
        LEFT JOIN fr_school_review_stats stats ON stats.uai = e.identifiant_de_l_etablissement
        WHERE
          e.nom_commune ILIKE $1
          OR e.libelle_departement ILIKE $1
        ORDER BY
          COALESCE(stats.avg_overall_rating, 0) DESC,
          COALESCE(NULLIF(e.nombre_d_eleves, '')::int, 0) DESC,
          e.nom_etablissement ASC
        LIMIT $2
      `,
      [likeTerm, limit]
    );

    res.json({
      success: true,
      city,
      total: rows.length,
      schools: rows.map(mapSchoolRow)
    });
  } catch (error) {
    console.error("City search error", error);
    res.status(500).json({
      error: "Recherche par commune indisponible",
      message: error.message
    });
  }
});

router.get("/suggestions", async (req, res) => {
  try {
    const rawQ = String(req.query.q || "").trim();
    if (rawQ.length < 2) {
      return res.json({ suggestions: [] });
    }

    const likeTerm = sanitizeLikeTerm(rawQ);

    const { rows } = await runQuery(
      `
        (
          SELECT 
            e.nom_etablissement AS suggestion,
            'school' AS type,
            e.identifiant_de_l_etablissement AS id,
            stats.avg_overall_rating AS avg_rating
          FROM fr_ecoles e
          LEFT JOIN fr_school_review_stats stats ON stats.uai = e.identifiant_de_l_etablissement
          WHERE e.nom_etablissement ILIKE $1
          ORDER BY stats.avg_overall_rating DESC NULLS LAST, e.nom_etablissement ASC
          LIMIT 5
        )
        UNION ALL
        (
          SELECT DISTINCT
            e.nom_commune AS suggestion,
            'city' AS type,
            NULL AS id,
            NULL AS avg_rating
          FROM fr_ecoles e
          WHERE e.nom_commune ILIKE $1
          LIMIT 3
        )
        UNION ALL
        (
          SELECT DISTINCT
            e.libelle_departement AS suggestion,
            'departement' AS type,
            NULL AS id,
            NULL AS avg_rating
          FROM fr_ecoles e
          WHERE e.libelle_departement ILIKE $1
          LIMIT 2
        )
      `,
      [likeTerm]
    );

    res.json({
      success: true,
      suggestions: rows.map((row) => ({
        suggestion: row.suggestion,
        type: row.type,
        id: row.id,
        rating_display: row.avg_rating !== null && row.avg_rating !== undefined
          ? `${(Number(row.avg_rating) * 2).toFixed(1)}/10`
          : null
      }))
    });
  } catch (error) {
    console.error("Suggestions error", error);
    res.status(500).json({
      error: "Auto-completion indisponible",
      message: error.message
    });
  }
});

router.get("/filters", async (_req, res) => {
  try {
    const [typesRes, statusRes, regionRes] = await Promise.all([
      runQuery(
        `
          SELECT e.type_etablissement AS value, COUNT(*) AS count
          FROM fr_ecoles e
          WHERE e.type_etablissement IS NOT NULL
          GROUP BY e.type_etablissement
          ORDER BY count DESC
          LIMIT 50
        `,
        []
      ),
      runQuery(
        `
          SELECT e.statut_public_prive AS value, COUNT(*) AS count
          FROM fr_ecoles e
          WHERE e.statut_public_prive IS NOT NULL
          GROUP BY e.statut_public_prive
          ORDER BY count DESC
        `,
        []
      ),
      runQuery(
        `
          SELECT e.libelle_region AS value, COUNT(*) AS count
          FROM fr_ecoles e
          WHERE e.libelle_region IS NOT NULL
          GROUP BY e.libelle_region
          ORDER BY count DESC
        `,
        []
      )
    ]);

    res.json({
      success: true,
      filters: {
        types: typesRes.rows,
        statuts: statusRes.rows,
        regions: regionRes.rows
      }
    });
  } catch (error) {
    console.error("Filters error", error);
    res.status(500).json({
      error: "Filtres indisponibles",
      message: error.message
    });
  }
});

module.exports = router;
