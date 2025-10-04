const express = require('express');
const router = express.Router();

const {
  getIdentityByUai,
  searchDirectory,
  nearbyDirectory,
} = require('../services/frOpendata');
const { getEffectifsByType } = require('../services/frOpendata');
const { getEffectifsHistory } = require('../services/frOpendata');
const { getIPS } = require('../services/frOpendata');
const { query } = require('../config/database');

// GET /api/fr/identity/:uai
router.get('/identity/:uai', async (req, res) => {
  try {
    const uai = String(req.params.uai || '').trim();
    if (!uai) return res.status(400).json({ error: 'UAI requis' });
    const identity = await getIdentityByUai(uai, { ttlMs: 6 * 60 * 60 * 1000 });
    if (!identity) return res.status(404).json({ error: 'Etablissement introuvable' });
    res.json({ success: true, school: identity });
  } catch (e) {
    console.error('fr/identity error', e);
    res.status(500).json({ error: 'Service indisponible', message: e.message });
  }
});

// GET /api/fr/identity/search?q=...&rows=...
router.get('/identity', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    const rows = Math.min(parseInt(req.query.rows, 10) || 20, 1000);
    const { total, items } = await searchDirectory({ q, rows, ttlMs: 60 * 60 * 1000 });
    res.json({ success: true, total, schools: items });
  } catch (e) {
    console.error('fr/identity search error', e);
    res.status(500).json({ error: 'Recherche indisponible', message: e.message });
  }
});

// GET /api/fr/identity/nearby?lat=&lng=&radius_km=&rows=
router.get('/identity/nearby', async (req, res) => {
  try {
    const lat = Number(req.query.lat);
    const lon = Number(req.query.lng || req.query.lon);
    const radius_km = Number(req.query.radius_km || req.query.radius || 5);
    const rows = Math.min(parseInt(req.query.rows, 10) || 1000, 10000);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return res.status(400).json({ error: 'lat et lng requis' });
    }
    const { total, items } = await nearbyDirectory({ lat, lon, radius_km, rows, ttlMs: 60 * 60 * 1000 });
    res.json({ success: true, total, schools: items, center: { lat, lon }, radius_km });
  } catch (e) {
    console.error('fr/identity nearby error', e);
    res.status(500).json({ error: 'Recherche de proximite indisponible', message: e.message });
  }
});

module.exports = router;

// Effectifs by school type
router.get('/effectifs/:uai', async (req, res) => {
  try {
    const uai = String(req.params.uai || '').trim();
    const type = String(req.query.type || '').trim(); // college | lycee_gt | lycee_pro
    const year = req.query.year ? String(req.query.year) : undefined;
    if (!uai || !type) return res.status(400).json({ error: 'uai and type are required' });

    const result = await getEffectifsByType({ uai, type, year, ttlMs: 2 * 60 * 60 * 1000 });
    res.json({ success: true, uai, type, ...result });
  } catch (e) {
    console.error('fr/effectifs error', e);
    res.status(500).json({ error: 'Effectifs indisponibles', message: e.message });
  }
});

// GET /api/fr/effectifs/:uai/history?type=...
router.get('/effectifs/:uai/history', async (req, res) => {
  try {
    const uai = String(req.params.uai || '').trim();
    const type = String(req.query.type || '').trim();
    if (!uai || !type) return res.status(400).json({ error: 'uai and type are required' });
    const result = await getEffectifsHistory({ uai, type, ttlMs: 2 * 60 * 60 * 1000 });
    res.json({ success: true, uai, type, ...result });
  } catch (e) {
    console.error('fr/effectifs history error', e);
    res.status(500).json({ error: 'Historique des effectifs indisponible', message: e.message });
  }
});

// GET /api/fr/context/:uai -> { ips: { value, year, ... }, rep_flag, rep_plus_flag, qpv_proximity }
router.get('/context/:uai', async (req, res) => {
  try {
    const uai = String(req.params.uai || '').trim();
    if (!uai) return res.status(400).json({ error: 'UAI requis' });

    // Try to infer type from DB if available (for IPS dataset choice)
    let type = null;
    try {
      const { rows } = await query('SELECT type_etablissement, appartenance_education_prioritaire FROM fr_ecoles WHERE identifiant_de_l_etablissement = $1 LIMIT 1', [uai]);
      if (rows && rows.length) {
        const t = (rows[0].type_etablissement || '').toLowerCase();
        if (t.includes('coll')) type = 'college';
        else if (t.includes('lyc')) type = 'lycee';

        // REP flags from DB text
        const ap = String(rows[0].appartenance_education_prioritaire || '').toUpperCase();
        var rep_plus_flag = ap.includes('REP+') || ap.includes('REP +');
        var rep_flag = !rep_plus_flag && ap.includes('REP');
        var contextFromDb = { rep_flag, rep_plus_flag };
        // Build response progressively
        var response = { success: true, uai, ...contextFromDb };

        // IPS via open data
        if (type) {
          const ips = await getIPS({ uai, type, ttlMs: 6 * 60 * 60 * 1000 });
          if (ips) {
            response.ips = {
              value: ips.ips,
              year: ips.year,
              rentree_scolaire: ips.rentree_scolaire,
              national: ips.ips_national,
              academique: ips.ips_academique,
              departemental: ips.ips_departemental,
            };
          }
        }

        // QPV proximity not yet integrated: leave null for now
        response.qpv_proximity = null;
        return res.json(response);
      }
    } catch (e) {
      // Ignore DB error and still try IPS via directory
      console.warn('fr/context DB lookup failed:', e.message);
    }

    // Fallback: try IPS assuming college first, then lycee
    let ips = await getIPS({ uai, type: 'college', ttlMs: 6 * 60 * 60 * 1000 });
    if (!ips) ips = await getIPS({ uai, type: 'lycee', ttlMs: 6 * 60 * 60 * 1000 });
    res.json({ success: true, uai, ips: ips ? {
      value: ips.ips,
      year: ips.year,
      rentree_scolaire: ips.rentree_scolaire,
      national: ips.ips_national,
      academique: ips.ips_academique,
      departemental: ips.ips_departemental,
    } : null, rep_flag: null, rep_plus_flag: null, qpv_proximity: null });

  } catch (e) {
    console.error('fr/context error', e);
    res.status(500).json({ error: 'Contexte social indisponible', message: e.message });
  }
});
