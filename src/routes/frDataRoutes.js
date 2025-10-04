const express = require('express');
const router = express.Router();

const {
  getIdentityByUai,
  searchDirectory,
  nearbyDirectory,
} = require('../services/frOpendata');
const { getEffectifsByType } = require('../services/frOpendata');
const { getEffectifsHistory } = require('../services/frOpendata');

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
