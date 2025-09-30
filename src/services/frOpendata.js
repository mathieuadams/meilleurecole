const DEFAULT_BASE = process.env.FR_DATA_API_BASE || 'https://data.education.gouv.fr';
const APP_TOKEN = process.env.FR_DATA_APP_TOKEN || process.env.X_APP_TOKEN || null;

// Datasets
const DS = {
  directory: 'fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre',
  effectifs_college: 'fr-en-college-effectifs-niveau-sexe-lv',
  effectifs_lycee_gt: 'fr-en-lycee_gt-effectifs-niveau-sexe-lv',
  effectifs_lycee_pro: 'fr-en-lycee_pro-effectifs-niveau-sexe-lv',
};

// Simple in-memory cache with TTL
const cache = new Map();
const now = () => Date.now();

function cacheGet(key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (entry.expires && entry.expires < now()) {
    cache.delete(key);
    return null;
  }
  return entry.value;
}

function cacheSet(key, value, ttlMs = 6 * 60 * 60 * 1000) { // default 6h
  cache.set(key, { value, expires: ttlMs ? now() + ttlMs : 0 });
}

function toQs(params) {
  const usp = new URLSearchParams();
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v === undefined || v === null || v === '') return;
    usp.append(k, String(v));
  });
  return usp.toString();
}

async function callDataset(dataset, params, { ttlMs } = {}) {
  const path = `/api/records/1.0/search/?dataset=${encodeURIComponent(dataset)}&${toQs(params)}`;
  const url = `${DEFAULT_BASE}${path}`;
  const key = `GET ${url}`;
  const cached = cacheGet(key);
  if (cached) return cached;

  const headers = APP_TOKEN ? { 'X-APP-TOKEN': APP_TOKEN } : {};
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(url, { headers, signal: controller.signal });
    if (!res.ok) throw new Error(`OpenData ${res.status} ${res.statusText}`);
    const data = await res.json();
    cacheSet(key, data, ttlMs);
    return data;
  } finally {
    clearTimeout(timeout);
  }
}

function pick(fields, names, fallback = null) {
  for (const n of names) {
    if (fields[n] !== undefined && fields[n] !== null) return fields[n];
  }
  return fallback;
}

function parseCoords(rec) {
  // Try fields.geo_point_2d (array [lat, lon] or string), then geometry.coordinates [lon, lat]
  const f = rec.fields || {};
  let lat = null;
  let lon = null;
  const gp = f.geo_point_2d || f.coordonnees || f.coordinates || null;
  if (Array.isArray(gp) && gp.length >= 2) {
    lat = Number(gp[0]);
    lon = Number(gp[1]);
  } else if (typeof gp === 'string') {
    const parts = gp.split(',').map(s => Number(s.trim()));
    if (parts.length >= 2) { lat = parts[0]; lon = parts[1]; }
  }
  const geom = rec.geometry?.coordinates;
  if ((!Number.isFinite(lat) || !Number.isFinite(lon)) && Array.isArray(geom) && geom.length >= 2) {
    lon = Number(geom[0]);
    lat = Number(geom[1]);
  }
  return {
    latitude: Number.isFinite(lat) ? lat : null,
    longitude: Number.isFinite(lon) ? lon : null,
  };
}

function normalizeDirectoryRecord(rec) {
  const f = rec.fields || {};
  const coords = parseCoords(rec);
  const name = pick(f, ['appellation_officielle', 'nom_etablissement', 'denomination_principale', 'denomination', 'etablissement']);
  const type = pick(f, ['type_etablissement', 'nature']);
  const statut = pick(f, ['secteur_public_prive', 'statut_public_prive']);
  const adresse = [pick(f, ['adresse_1','adresse']), pick(f, ['adresse_2']), pick(f, ['adresse_3'])]
    .filter(Boolean)
    .join(', ');
  return {
    uai: pick(f, ['numero_uai', 'uai', 'numero_lycee', 'numero_ecole']),
    name: name || null,
    type_etablissement: type || null,
    statut_public_prive: statut || null,
    adresse_1: pick(f, ['adresse_1','adresse']) || null,
    adresse_2: pick(f, ['adresse_2']) || null,
    adresse_3: pick(f, ['adresse_3']) || null,
    address: adresse || null,
    code_postal: pick(f, ['code_postal']) || null,
    libelle_commune: pick(f, ['libelle_commune', 'commune']) || null,
    code_departement: pick(f, ['code_departement']) || null,
    libelle_departement: pick(f, ['libelle_departement']) || null,
    libelle_academie: pick(f, ['libelle_academie', 'academie']) || null,
    libelle_region: pick(f, ['libelle_region', 'region']) || null,
    latitude: coords.latitude,
    longitude: coords.longitude,
    raw: f,
  };
}

async function getIdentityByUai(uai, { rows = 1, ttlMs } = {}) {
  const data = await callDataset(DS.directory, {
    rows,
    [`refine.numero_uai`]: uai,
  }, { ttlMs });
  const rec = data?.records?.[0];
  return rec ? normalizeDirectoryRecord(rec) : null;
}

async function searchDirectory({ q, rows = 20, start = 0, refine = {}, ttlMs } = {}) {
  const params = { rows, start };
  if (q) params.q = q;
  Object.entries(refine || {}).forEach(([k, v]) => {
    params[`refine.${k}`] = v;
  });
  const data = await callDataset(DS.directory, params, { ttlMs });
  const items = (data?.records || []).map(normalizeDirectoryRecord);
  return { total: data?.nhits || items.length, items };
}

async function nearbyDirectory({ lat, lon, radius_km = 5, rows = 1000, ttlMs } = {}) {
  const meters = Math.max(50, Math.round(Number(radius_km) * 1000));
  const data = await callDataset(DS.directory, {
    rows,
    'geofilter.distance': `${lat},${lon},${meters}`,
  }, { ttlMs });
  const items = (data?.records || []).map(normalizeDirectoryRecord);
  return { total: data?.nhits || items.length, items };
}

module.exports = {
  DS,
  callDataset,
  getIdentityByUai,
  searchDirectory,
  nearbyDirectory,
};

// ------------------------------ Effectifs ----------------------------------

function readEffectifField(fields) {
  const keys = ['effectif', 'nombre_eleves', 'nb_eleves', 'nombre_d_eleves'];
  for (const k of keys) {
    const v = fields[k];
    if (v !== undefined && v !== null && v !== '') {
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
  }
  return 0;
}

function readNiveauLabel(fields) {
  return (
    fields.libelle_niveau ||
    fields.niveau ||
    fields.libelle_cycle ||
    fields.serie ||
    'Autre'
  );
}

async function getEffectifsByType({ uai, type, year, rows = 1000, ttlMs } = {}) {
  let dataset, refineKey;
  const t = String(type || '').toLowerCase();
  if (t === 'college') {
    dataset = DS.effectifs_college; refineKey = 'uai';
  } else if (t === 'lycee_gt' || t === 'lycee' || t === 'lycee_general' || t.includes('gt')) {
    dataset = DS.effectifs_lycee_gt; refineKey = 'numero_lycee';
  } else if (t === 'lycee_pro' || t.includes('pro')) {
    dataset = DS.effectifs_lycee_pro; refineKey = 'numero_lycee';
  } else {
    throw new Error('Unsupported effectifs type');
  }

  const params = { rows };
  params[`refine.${refineKey}`] = uai;
  if (year) params['refine.rentree_scolaire'] = year;

  const data = await callDataset(dataset, params, { ttlMs });
  const records = data?.records || [];
  let total = 0;
  const byLevel = {};
  let chosenYear = year || null;

  for (const rec of records) {
    const f = rec.fields || {};
    const eff = readEffectifField(f);
    total += eff;
    const lvl = readNiveauLabel(f);
    byLevel[lvl] = (byLevel[lvl] || 0) + eff;
    if (!chosenYear && f.rentree_scolaire) chosenYear = f.rentree_scolaire;
  }

  return { total, by_level: byLevel, year: chosenYear, dataset };
}

module.exports.getEffectifsByType = getEffectifsByType;
