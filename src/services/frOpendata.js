const DEFAULT_BASE = process.env.FR_DATA_API_BASE || 'https://data.education.gouv.fr';
const APP_TOKEN = process.env.FR_DATA_APP_TOKEN || process.env.X_APP_TOKEN || null;

// Datasets
const DS = {
  directory: 'fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre',
  annuaire: 'fr-en-annuaire-education',
  effectifs_college: 'fr-en-college-effectifs-niveau-sexe-lv',
  effectifs_lycee_gt: 'fr-en-lycee_gt-effectifs-niveau-sexe-lv',
  effectifs_lycee_pro: 'fr-en-lycee_pro-effectifs-niveau-sexe-lv',
  ips_colleges_2023: 'fr-en-ips-colleges-ap2023',
  ips_colleges_2022: 'fr-en-ips-colleges-ap2022',
  ips_colleges_legacy: 'fr-en-ips_colleges',
  ips_lycees_2023: 'fr-en-ips-lycees-ap2023',
  ips_lycees_2022: 'fr-en-ips-lycees-ap2022',
  ips_lycees_legacy: 'fr-en-ips_lycees',
  dnb_by_etab: 'fr-en-dnb-par-etablissement',
  college_value_added: 'fr-en-indicateurs-valeur-ajoutee-colleges',
  lycee_gt_indicators: 'fr-en-indicateurs-de-resultat-des-lycees-denseignement-general-et-technologique',
  lycee_gt_v2: 'fr-en-indicateurs-de-resultat-des-lycees-gt_v2',
  lycee_pro_indicators: 'fr-en-indicateurs-de-resultat-des-lycees-denseignement-professionnels',
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
  const keys = ['effectif', 'nombre_eleves', 'nb_eleves', 'nombre_d_eleves', 'nombre_eleves_total', 'nombre_d_eleves_total'];
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
    dataset = DS.effectifs_college;
    // The dataset uses 'numero_college' (not 'uai') for the UAI code
    refineKey = 'numero_college';
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

  // Helper: pick the year to use (latest if none provided)
  const allYears = records
    .map(r => String(r?.fields?.rentree_scolaire || ''))
    .filter(Boolean);
  let chosenYear = year || (allYears.length ? String(allYears.map(y => parseInt(y, 10)).filter(Number.isFinite).sort((a,b)=>b-a)[0]) : null);

  const yearRecords = chosenYear
    ? records.filter(r => String(r?.fields?.rentree_scolaire) === String(chosenYear))
    : records;

  // Dataset-specific aggregation to avoid double counting repeated rows
  if (t === 'college') {
    // Values appear on every row for the year; take the maximum across rows.
    const agg = { '6e': 0, '5e': 0, '4e': 0, '3e': 0, 'ULIS': 0, 'SEGPA': 0 };
    let total = 0;
    for (const rec of yearRecords) {
      const f = rec.fields || {};
      agg['6e'] = Math.max(agg['6e'], Number(f.nombre_total_de_6emes) || 0);
      agg['5e'] = Math.max(agg['5e'], Number(f.nombre_total_de_5emes) || 0);
      agg['4e'] = Math.max(agg['4e'], Number(f.nombre_total_de_4emes) || 0);
      agg['3e'] = Math.max(agg['3e'], Number(f.nombre_total_de_3emes) || 0);
      agg['ULIS'] = Math.max(agg['ULIS'], Number(f.nombre_d_eleves_total_ulis) || 0);
      agg['SEGPA'] = Math.max(agg['SEGPA'], Number(f.nombre_d_eleves_total_segpa) || 0);
      total = Math.max(total, Number(f.nombre_eleves_total) || 0);
    }
    if (!total) total = Object.values(agg).reduce((a,b)=>a+b,0);
    return { total, by_level: agg, year: chosenYear, dataset };
  }

  // Generic fallback for other datasets: sum an 'effectif' field when present
  let total = 0;
  const byLevel = {};
  for (const rec of yearRecords) {
    const f = rec.fields || {};
    const eff = readEffectifField(f);
    total += eff;
    const lvl = readNiveauLabel(f);
    byLevel[lvl] = (byLevel[lvl] || 0) + eff;
  }
  return { total, by_level: byLevel, year: chosenYear, dataset };
}

module.exports.getEffectifsByType = getEffectifsByType;

// -------------------------- Effectifs history ------------------------------

function readTotalForRecord(dataset, fields) {
  if (!fields) return 0;
  if (dataset === DS.effectifs_college) {
    const direct = Number(fields.nombre_eleves_total);
    if (Number.isFinite(direct)) return direct;
    const parts = [
      Number(fields.nombre_total_de_6emes),
      Number(fields.nombre_total_de_5emes),
      Number(fields.nombre_total_de_4emes),
      Number(fields.nombre_total_de_3emes),
      Number(fields.nombre_d_eleves_total_ulis),
      Number(fields.nombre_d_eleves_total_segpa),
    ];
    return parts.reduce((a,b)=> a + (Number.isFinite(b)?b:0), 0);
  }
  const v = Number(fields.nombre_d_eleves);
  if (Number.isFinite(v)) return v;
  return readEffectifField(fields);
}

async function getEffectifsHistory({ uai, type, rows = 1000, ttlMs } = {}) {
  let dataset, refineKey;
  const t = String(type || '').toLowerCase();
  if (t === 'college') { dataset = DS.effectifs_college; refineKey = 'numero_college'; }
  else if (t === 'lycee_gt' || t === 'lycee' || t === 'lycee_general' || t.includes('gt')) { dataset = DS.effectifs_lycee_gt; refineKey = 'numero_lycee'; }
  else if (t === 'lycee_pro' || t.includes('pro')) { dataset = DS.effectifs_lycee_pro; refineKey = 'numero_lycee'; }
  else { throw new Error('Unsupported effectifs type'); }

  const params = { rows };
  params[`refine.${refineKey}`] = uai;

  const data = await callDataset(dataset, params, { ttlMs });
  const records = data?.records || [];

  const byYear = new Map();
  for (const rec of records) {
    const f = rec.fields || {};
    const y = String(f.rentree_scolaire || '').trim();
    if (!y) continue;
    const eff = readTotalForRecord(dataset, f);
    const prev = byYear.get(y) || 0;
    if (eff > prev) byYear.set(y, eff);
  }

  const series = Array.from(byYear.entries())
    .map(([year, total]) => ({ year, total }))
    .sort((a,b) => parseInt(a.year,10) - parseInt(b.year,10));

  const latest_year = series.length ? series[series.length - 1].year : null;
  return { series, latest_year, dataset };
}

module.exports.getEffectifsHistory = getEffectifsHistory;

// ------------------------------ IPS (social index) -------------------------

function parseIPSValue(fields) {
  const v = fields?.ips ?? fields?.IPS ?? fields?.indice ?? null;
  const n = Number(String(v).replace(',', '.'));
  return Number.isFinite(n) ? Number(n.toFixed(1)) : null;
}

async function getIPS({ uai, type, rows = 5, ttlMs } = {}) {
  const t = String(type || '').toLowerCase();
  const datasets = t === 'college'
    ? [DS.ips_colleges_2023, DS.ips_colleges_2022, DS.ips_colleges_legacy]
    : [DS.ips_lycees_2023, DS.ips_lycees_2022, DS.ips_lycees_legacy];

  let best = null; // { year, ips, national, academique, departemental }
  for (const ds of datasets) {
    try {
      const data = await callDataset(ds, { rows, [`refine.uai`]: uai }, { ttlMs });
      const recs = data?.records || [];
      for (const rec of recs) {
        const f = rec.fields || {};
        const ips = parseIPSValue(f);
        if (ips == null) continue;
        const yearLabel = String(f.rentree_scolaire || '').trim();
        const yearNum = parseInt((yearLabel.match(/\d{4}/) || [null])[0], 10);
        const pickNum = (k) => {
          const val = f[k] != null ? Number(String(f[k]).replace(',', '.')) : null;
          return Number.isFinite(val) ? Number(val.toFixed(1)) : null;
        };
        const candidate = {
          dataset: ds,
          rentree_scolaire: yearLabel || null,
          year: Number.isFinite(yearNum) ? yearNum : null,
          ips,
          ips_national: pickNum('ips_national'),
          ips_academique: pickNum('ips_academique'),
          ips_departemental: pickNum('ips_departemental'),
        };
        // Keep the most recent year
        if (!best || ((candidate.year || 0) > (best.year || 0))) {
          best = candidate;
        }
      }
      if (best) break; // found something
    } catch (e) {
      // ignore and try next dataset
    }
  }
  return best; // may be null if nothing found
}

module.exports.getIPS = getIPS;

// ------------------------------ Annuaire (contacts) ------------------------

function normalizeAnnuaireRecord(rec) {
  const f = rec?.fields || {};
  return {
    uai: f.identifiant_de_l_etablissement || null,
    telephone: f.telephone || null,
    fax: f.fax || null,
    web: f.web || null,
    mail: f.mail || null,
    fiche_onisep: f.fiche_onisep || null,
    adresse_1: f.adresse_1 || null,
    adresse_2: f.adresse_2 || null,
    adresse_3: f.adresse_3 || null,
    code_postal: f.code_postal || null,
    code_commune: f.code_commune || null,
    nom_commune: f.nom_commune || null,
  };
}

async function getAnnuaireByUai(uai, { ttlMs } = {}) {
  const data = await callDataset(DS.annuaire, {
    rows: 1,
    [`refine.identifiant_de_l_etablissement`]: uai,
  }, { ttlMs });
  const rec = data?.records?.[0];
  return rec ? normalizeAnnuaireRecord(rec) : null;
}

module.exports.getAnnuaireByUai = getAnnuaireByUai;

// List all establishments in the same commune as a given UAI
async function getCommuneSchools({ uai, page = 1, pageSize = 10, ttlMs } = {}) {
  const me = await getAnnuaireByUai(uai, { ttlMs });
  if (!me) return { items: [], town: null, total: 0, page, page_size: pageSize, total_pages: 0 };
  const params = { rows: Math.max(1, Math.min(100, Number(pageSize) || 10)) };
  // Filter by commune (code_commune preferred). Do NOT filter by code_postal to include all postcodes of the commune.
  if (me.code_commune) params['refine.code_commune'] = me.code_commune;
  else if (me.nom_commune) params['refine.nom_commune'] = me.nom_commune;
  const start = Math.max(0, (Number(page) - 1) * params.rows);
  params.start = start;

  const data = await callDataset(DS.annuaire, params, { ttlMs });
  const items = (data?.records || []).map(r => {
    const f = r.fields || {};
    return {
      uai: f.identifiant_de_l_etablissement,
      name: f.nom_etablissement,
      type: f.type_etablissement,
      statut: f.statut_public_prive,
      latitude: f.latitude ?? r.geometry?.coordinates?.[1] ?? null,
      longitude: f.longitude ?? r.geometry?.coordinates?.[0] ?? null,
      address: [f.adresse_1, f.adresse_2, f.adresse_3].filter(Boolean).join(', '),
    };
  });
  const total = Number(data?.nhits || 0);
  const totalPages = Math.max(1, Math.ceil(total / params.rows));
  return { items, town: me.nom_commune, postcode: me.code_postal, total, page: Number(page), page_size: params.rows, total_pages: totalPages };
}

module.exports.getCommuneSchools = getCommuneSchools;

// ------------------------------ Levels by gender ---------------------------

function safeNum(v){
  const n = Number(v); return Number.isFinite(n) ? n : 0;
}

function aggMax(dst, k, v){ dst[k] = Math.max(dst[k] || 0, safeNum(v)); }

async function getLevelsByGender({ uai, type, years = 5, ttlMs } = {}) {
  const t = String(type || '').toLowerCase();
  let dataset, refineKey, mapping;
  if (t === 'college') {
    dataset = DS.effectifs_college; refineKey = 'numero_college';
    mapping = {
      levels: [ '6e','5e','4e','3e' ],
      girls: { '6e': '6eme_filles', '5e': '5eme_filles', '4e': '4eme_filles', '3e': '3eme_filles' },
      boys:  { '6e': '6emes_garcons', '5e': '5emes_garcons', '4e': '4emes_garcons', '3e': '3emes_garcons' },
      totals:{ '6e': 'nombre_total_de_6emes', '5e': 'nombre_total_de_5emes', '4e': 'nombre_total_de_4emes', '3e': 'nombre_total_de_3emes' }
    };
  } else if (t === 'lycee_gt' || t === 'lycee' || t.includes('gt')) {
    dataset = DS.effectifs_lycee_gt; refineKey = 'numero_lycee';
    mapping = {
      levels: [ '2nde','1re','Term' ],
      girls: { '2nde': '2ndes_gt_filles', '1re': '1eres_g_filles', 'Term': 'terminales_g_filles' },
      boys:  { '2nde': '2ndes_gt_garcons', '1re': '1eres_g_garcons', 'Term': 'terminales_g_garcons' },
      totals:{ '2nde': '2ndes_gt', '1re': '1eres_g', 'Term': 'terminales_g' }
    };
  } else if (t === 'lycee_pro' || t.includes('pro')) {
    dataset = DS.effectifs_lycee_pro; refineKey = 'numero_lycee';
    mapping = {
      levels: [ '2nde pro','1re pro','Term pro' ],
      girls: { '2nde pro': '2ndes_pro_filles', '1re pro': '1eres_pro_filles', 'Term pro': 'terminales_pro_filles' },
      boys:  { '2nde pro': '2ndes_pro_garcons', '1re pro': '1eres_pro_garcons', 'Term pro': 'terminales_pro_garcons' },
      totals:{ '2nde pro': '2ndes_pro', '1re pro': '1eres_pro', 'Term pro': 'terminales_pro' }
    };
  } else {
    throw new Error('Unsupported effectifs type');
  }

  const params = { rows: 1000 };
  params[`refine.${refineKey}`] = uai;
  const data = await callDataset(dataset, params, { ttlMs });
  const recs = data?.records || [];

  // Aggregate per year with max across duplicate rows
  const byYear = new Map();
  for (const r of recs) {
    const f = r.fields || {};
    const y = String(f.rentree_scolaire || '').match(/\d{4}/)?.[0];
    if (!y) continue;
    if (!byYear.has(y)) {
      const levels = {}; mapping.levels.forEach(L => levels[L] = { girls: 0, boys: 0, total: 0 });
      byYear.set(y, { levels, grand_total: 0 });
    }
    const row = byYear.get(y);
    for (const L of mapping.levels) {
      aggMax(row.levels[L], 'girls', f[mapping.girls[L]]);
      aggMax(row.levels[L], 'boys', f[mapping.boys[L]]);
      aggMax(row.levels[L], 'total', f[mapping.totals[L]]);
    }
  }
  // compute grand totals
  for (const [y, row] of byYear.entries()) {
    let sum = 0; mapping.levels.forEach(L => { sum += safeNum(row.levels[L].total) || (safeNum(row.levels[L].girls)+safeNum(row.levels[L].boys)); });
    row.grand_total = sum;
  }

  const yearsSorted = Array.from(byYear.keys()).map(n=>parseInt(n,10)).filter(Number.isFinite).sort((a,b)=>b-a).slice(0, Number(years)||5).map(String);
  const series = yearsSorted.map(y => ({ year: y, ...byYear.get(y) }));
  return { success: true, type: t, dataset, levels: mapping.levels, series };
}

module.exports.getLevelsByGender = getLevelsByGender;

// ------------------------------ Langues ------------------------------------

function accumulateLanguagesFromFields(fields, buckets) {
  const names = Object.keys(fields || {});
  for (const key of names) {
    const m = key.match(/lv(1|2)_(anglais|allemand|espagnol|italien|autres_langues)/i);
    if (!m) continue;
    const lv = m[1] === '1' ? 'lv1' : 'lv2';
    const lang = m[2].toLowerCase();
    const val = Number(fields[key]);
    if (Number.isFinite(val) && val > 0) {
      buckets[lv][lang] = (buckets[lv][lang] || 0) + val;
    }
  }
}

async function getLanguagesByType({ uai, type, year, rows = 1000, ttlMs } = {}) {
  let dataset, refineKey;
  const t = String(type || '').toLowerCase();
  if (t === 'college') { dataset = DS.effectifs_college; refineKey = 'numero_college'; }
  else if (t === 'lycee_gt' || t === 'lycee' || t === 'lycee_general' || t.includes('gt')) { dataset = DS.effectifs_lycee_gt; refineKey = 'numero_lycee'; }
  else if (t === 'lycee_pro' || t.includes('pro')) { dataset = DS.effectifs_lycee_pro; refineKey = 'numero_lycee'; }
  else { throw new Error('Unsupported langues type'); }

  const params = { rows };
  params[`refine.${refineKey}`] = uai;
  if (year) params['refine.rentree_scolaire'] = year;

  const data = await callDataset(dataset, params, { ttlMs });
  const records = data?.records || [];
  const allYears = records.map(r => String(r?.fields?.rentree_scolaire || '')).filter(Boolean);
  let chosenYear = year || (allYears.length ? String(allYears.map(y => parseInt(y, 10)).filter(Number.isFinite).sort((a,b)=>b-a)[0]) : null);
  const yearRecords = chosenYear ? records.filter(r => String(r?.fields?.rentree_scolaire) === String(chosenYear)) : records;

  const buckets = { lv1: {}, lv2: {} };
  for (const rec of yearRecords) accumulateLanguagesFromFields(rec.fields || {}, buckets);

  const pretty = {
    anglais: 'Anglais', allemand: 'Allemand', espagnol: 'Espagnol', italien: 'Italien', autres_langues: 'Autres langues'
  };
  const lv1 = Object.keys(buckets.lv1).sort().map(k => ({ key: k, name: pretty[k] || k, total: buckets.lv1[k] }));
  const lv2 = Object.keys(buckets.lv2).sort().map(k => ({ key: k, name: pretty[k] || k, total: buckets.lv2[k] }));

  return {
    success: true,
    dataset,
    year: chosenYear,
    lv1: lv1.map(x => x.name),
    lv2: lv2.map(x => x.name),
    totals: { lv1, lv2 }
  };
}

module.exports.getLanguagesByType = getLanguagesByType;

// ------------------------------ Exam results -------------------------------

function num(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(String(v).replace(',', '.').replace('%',''));
  return Number.isFinite(n) ? n : null;
}

async function getCollegeResults({ uai, year, ttlMs } = {}) {
  // Prefer value-added college indicators
  const params = { rows: 100 };
  params[`refine.uai`] = uai;
  if (year) params['refine.session'] = year;
  const data = await callDataset(DS.college_value_added, params, { ttlMs });
  const recs = data?.records || [];
  let chosen = null; let chosenYear = null;
  for (const r of recs) {
    const f = r.fields || {};
    const year = parseInt(String(f.session || '').match(/\d{4}/)?.[0] || '0', 10);
    if (!chosen || year > chosenYear) { chosen = f; chosenYear = year; }
  }
  if (!chosen) return { year: null, summary: null };
  const presenceRate = num(chosen.part_presents_3eme_ordinaire_total);
  const successRate = num(chosen.taux_de_reussite_g);
  const accessRate = num(chosen.taux_d_acces_6eme_3eme);
  const mentionsTotal = num(chosen.nb_mentions_global_g);
  const valueAdded = num(chosen.va_du_taux_de_reussite_g);
  const ab = num(chosen.nb_mentions_ab_g);
  const bien = num(chosen.nb_mentions_b_g);
  const tb = num(chosen.nb_mentions_tb_g);
  return {
    year: String(chosenYear),
    summary: {
      presence_rate: presenceRate,
      success_rate: successRate,
      access_rate: accessRate,
      mentions_total: mentionsTotal,
      value_added_success: valueAdded,
    },
    mentions: { ab, bien, tb },
    dataset: DS.college_value_added,
  };
}

async function getLyceeGTResults({ uai, year, ttlMs } = {}) {
  // Try the v2 dataset first (preferred)
  const tryV2 = async () => {
    const params = { rows: 100 };
    params[`refine.uai`] = uai;
    if (year) params['refine.annee'] = year;
    const data = await callDataset(DS.lycee_gt_v2, params, { ttlMs });
    const recs = data?.records || [];
    let chosen = null; let chosenYear = null;
    for (const r of recs) {
      const f = r.fields || {};
      const year = parseInt(String(f.annee || '').match(/\d{4}/)?.[0] || '0', 10);
      if (!chosen || year > chosenYear) { chosen = f; chosenYear = year; }
    }
    if (!chosen) return null;
    const presents = num(chosen.presents_total ?? chosen.presents_gnle);
    const successRate = num(chosen.taux_reu_total);
    const mentionsRate = num(chosen.taux_men_total ?? chosen.taux_men_gnle);
    return {
      year: String(chosenYear),
      summary: { presents, success_rate: successRate, mentions_rate: mentionsRate },
      mentions_counts: null,
      dataset: DS.lycee_gt_v2,
    };
  };

  const v2 = await tryV2();
  if (v2) return v2;

  // Fallback to older indicators dataset
  const params2 = { rows: 100 };
  params2[`refine.code_etablissement`] = uai;
  if (year) params2['refine.annee'] = year;
  const data = await callDataset(DS.lycee_gt_indicators, params2, { ttlMs });
  const recs = data?.records || [];
  let chosen = null; let chosenYear = null;
  for (const r of recs) {
    const f = r.fields || {};
    const year = parseInt(String(f.annee || '').match(/\d{4}/)?.[0] || '0', 10);
    if (!chosen || year > chosenYear) { chosen = f; chosenYear = year; }
  }
  if (!chosen) return { year: null, summary: null };
  const presents = num(chosen.effectif_presents_total_series) ?? num(chosen.presents_gnle);
  const successRate = num(chosen.taux_brut_de_reussite_total_series) ?? num(chosen.taux_reu_brut_gnle);
  const mentionsRate = num(chosen.taux_mention_brut_toutes_series);
  const mentionsCounts = {
    ab: num(chosen.nombre_de_mentions_ab_t) ?? num(chosen.nombre_de_mentions_ab_g),
    bien: num(chosen.nombre_de_mentions_b_t) ?? num(chosen.nombre_de_mentions_b_g),
    tb_sans: num(chosen.nombre_de_mentions_tb_sans_felicitations_t) ?? num(chosen.nombre_de_mentions_tb_sans_felicitations_g),
    tb_fel: num(chosen.nombre_de_mentions_tb_avec_felicitations_t) ?? num(chosen.nombre_de_mentions_tb_avec_felicitations_g),
  };
  return {
    year: String(chosenYear),
    summary: { presents, success_rate: successRate, mentions_rate: mentionsRate },
    mentions_counts: mentionsCounts,
    dataset: DS.lycee_gt_indicators,
  };
}

module.exports.getCollegeResults = getCollegeResults;
module.exports.getLyceeGTResults = getLyceeGTResults;
