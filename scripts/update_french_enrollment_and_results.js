// Update French school aggregates from newly provided CSVs
// - Lycee class enrollment (total students)
// - Lycee general & pro results (Bac pass/mentions)
// - College DNB results (best-effort join by name/commune/departement)
// - 2024 enrollment per class (best-effort total students)

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { parse } = require('csv-parse');

const DEFAULT_DATABASE_URL = process.env.DATABASE_URL
  || 'postgresql://school_platform_db_fr_user:cDW1EB5Ah6x9KCguituTxOs63EZlXgnV@dpg-d37b0r6r433s73ejel20-a.oregon-postgres.render.com/school_platform_db_fr';

const FILES = {
  LYCEE_CLASSES: path.join(__dirname, '..', 'french_school_etudiant_par_class_lycee.csv'),
  LYCEE_RESULTS: path.join(__dirname, '..', 'Lycee general and pro.csv'),
  COLLEGE_RESULTS: path.join(__dirname, '..', 'college resultats.csv'),
  ENROLL_2024: path.join(__dirname, '..', 'french_school_etudiant_par_class2024.csv'),
};

const pool = new Pool({
  connectionString: DEFAULT_DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const removeDiacritics = (value = '') =>
  String(value).normalize('NFKD').replace(/[\u0300-\u036f]/g, '');

const norm = (s) => removeDiacritics(String(s || '').toLowerCase())
  .replace(/[^a-z0-9]+/g, ' ')
  .replace(/\s+/g, ' ')
  .trim();

function parseNumber(val) {
  if (val === null || val === undefined) return null;
  const s = String(val).replace(/\s+/g, '').replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function getCol(record, candidates) {
  const map = new Map();
  Object.keys(record).forEach((k) => {
    map.set(norm(k), k);
  });
  for (const c of candidates) {
    const key = norm(c);
    if (map.has(key)) return record[map.get(key)];
  }
  // fallback: find contains
  const all = Array.from(map.keys());
  for (const c of candidates) {
    const key = norm(c);
    const found = all.find((k) => k.includes(key));
    if (found) return record[map.get(found)];
  }
  return null;
}

async function ensureTables(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS fr_school_fr_stats (
      uai TEXT PRIMARY KEY REFERENCES fr_ecoles("identifiant_de_l_etablissement") ON DELETE CASCADE,
      students_total INTEGER,
      lycee_students_total INTEGER,
      lycee_effectifs_seconde INTEGER,
      lycee_effectifs_premiere INTEGER,
      lycee_effectifs_terminale INTEGER,
      lycee_bac_candidates INTEGER,
      lycee_bac_success_rate NUMERIC(6,3),
      lycee_mentions_rate NUMERIC(6,3),
      college_dnb_candidates INTEGER,
      college_dnb_success_rate NUMERIC(6,3),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

async function readCsvRows(file) {
  if (!fs.existsSync(file)) return [];
  const records = [];
  const parser = fs.createReadStream(file, { encoding: 'utf8' })
    .pipe(parse({ columns: true, bom: true, skip_empty_lines: true }));
  for await (const rec of parser) records.push(rec);
  return records;
}

async function loadLyceeClasses(client) {
  if (!fs.existsSync(FILES.LYCEE_CLASSES)) return {};
  const rows = await readCsvRows(FILES.LYCEE_CLASSES);
  const byUai = {};
  rows.forEach((r) => {
    const uai = getCol(r, ['UAI', 'uai', 'Identifiant_de_l_etablissement']);
    if (!uai) return;
    const total = parseNumber(getCol(r, [
      "Nombre d'élèves",
      "Nombre d eleves",
      'nombre_eleve',
      'nombre_eleves',
    ]));
    const eff2 = parseNumber(getCol(r, ['Effectifs 2nde', 'Effectifs a la rentree N 2nde']));
    const eff1 = parseNumber(getCol(r, ["Effectifs 1ere", 'Effectifs a la rentree N 1ere']));
    const effT = parseNumber(getCol(r, ['Effectifs Term', 'Effectifs a la rentree N Term']));
    const lyceeTotal = [eff2, eff1, effT].filter((x) => x != null).reduce((a, b) => a + b, 0) || total || null;
    if (!byUai[uai]) byUai[uai] = { lycee_students_total: null, eff2: null, eff1: null, effT: null };
    if (lyceeTotal != null) byUai[uai].lycee_students_total = lyceeTotal;
    if (eff2 != null) byUai[uai].eff2 = eff2;
    if (eff1 != null) byUai[uai].eff1 = eff1;
    if (effT != null) byUai[uai].effT = effT;
  });
  for (const [uai, v] of Object.entries(byUai)) {
    await client.query(
      `INSERT INTO fr_school_fr_stats (uai, lycee_students_total, lycee_effectifs_seconde, lycee_effectifs_premiere, lycee_effectifs_terminale, updated_at)
       VALUES ($1,$2,$3,$4,$5,NOW())
       ON CONFLICT (uai) DO UPDATE SET
         lycee_students_total = COALESCE(EXCLUDED.lycee_students_total, fr_school_fr_stats.lycee_students_total),
         lycee_effectifs_seconde = COALESCE(EXCLUDED.lycee_effectifs_seconde, fr_school_fr_stats.lycee_effectifs_seconde),
         lycee_effectifs_premiere = COALESCE(EXCLUDED.lycee_effectifs_premiere, fr_school_fr_stats.lycee_effectifs_premiere),
         lycee_effectifs_terminale = COALESCE(EXCLUDED.lycee_effectifs_terminale, fr_school_fr_stats.lycee_effectifs_terminale),
         updated_at = NOW()`,
      [uai, v.lycee_students_total, v.eff2, v.eff1, v.effT]
    );
  }
  return byUai;
}

async function loadLyceeResults(client) {
  if (!fs.existsSync(FILES.LYCEE_RESULTS)) return {};
  const rows = await readCsvRows(FILES.LYCEE_RESULTS);
  for (const r of rows) {
    const uai = getCol(r, ['UAI', 'uai']);
    if (!uai) continue;
    const candidates = parseInt(String(getCol(r, ["Nombre d'élèves présents au Bac", 'Nombre eleves presents au Bac']) || '').replace(/\D+/g, '')) || null;
    const success = parseNumber(getCol(r, ['Taux de reussite bruts total', 'Taux de réussite bruts total', 'Taux reussite']))
    const mentions = parseNumber(getCol(r, ['Taux de mentions bruts', 'Taux mentions']));
    await client.query(
      `INSERT INTO fr_school_fr_stats (uai, lycee_bac_candidates, lycee_bac_success_rate, lycee_mentions_rate, updated_at)
       VALUES ($1,$2,$3,$4,NOW())
       ON CONFLICT (uai) DO UPDATE SET
         lycee_bac_candidates = COALESCE(EXCLUDED.lycee_bac_candidates, fr_school_fr_stats.lycee_bac_candidates),
         lycee_bac_success_rate = COALESCE(EXCLUDED.lycee_bac_success_rate, fr_school_fr_stats.lycee_bac_success_rate),
         lycee_mentions_rate = COALESCE(EXCLUDED.lycee_mentions_rate, fr_school_fr_stats.lycee_mentions_rate),
         updated_at = NOW()`,
      [uai, candidates, success, mentions]
    );
  }
}

async function buildEcoleLookup(client) {
  const { rows } = await client.query(`
    SELECT "identifiant_de_l_etablissement" AS uai,
           nom_etablissement,
           nom_commune,
           libelle_departement
    FROM fr_ecoles
  `);
  const map = new Map();
  rows.forEach(r => {
    const key = `${norm(r.nom_etablissement)}|${norm(r.nom_commune)}|${norm(r.libelle_departement)}`;
    if (!map.has(key)) map.set(key, r.uai);
  });
  return map;
}

async function loadCollegeResults(client) {
  if (!fs.existsSync(FILES.COLLEGE_RESULTS)) return;
  const lookup = await buildEcoleLookup(client);
  const rows = await readCsvRows(FILES.COLLEGE_RESULTS);
  for (const r of rows) {
    const etab = getCol(r, ['Etablissement', 'Établissement']);
    const commune = getCol(r, ['Libellé commune', 'Commune']);
    const dep = getCol(r, ['Libellé département', 'Departement', 'Département']);
    if (!etab || !commune || !dep) continue;
    const key = `${norm(etab)}|${norm(commune)}|${norm(dep)}`;
    const uai = lookup.get(key);
    if (!uai) continue;
    const candG = parseInt(String(getCol(r, ['Candidats au DNB Série générale']) || '').replace(/\D+/g, '')) || 0;
    const candP = parseInt(String(getCol(r, ['Candidats au DNB Série professionnelle']) || '').replace(/\D+/g, '')) || 0;
    const rateG = parseNumber(getCol(r, ['Taux Brut Série générale']));
    const rateP = parseNumber(getCol(r, ['Taux Brut Série professionnelle']));
    const candidates = candG + candP || null;
    const rate = rateG != null ? rateG : rateP;
    await client.query(
      `INSERT INTO fr_school_fr_stats (uai, college_dnb_candidates, college_dnb_success_rate, updated_at)
       VALUES ($1,$2,$3,NOW())
       ON CONFLICT (uai) DO UPDATE SET
         college_dnb_candidates = COALESCE(EXCLUDED.college_dnb_candidates, fr_school_fr_stats.college_dnb_candidates),
         college_dnb_success_rate = COALESCE(EXCLUDED.college_dnb_success_rate, fr_school_fr_stats.college_dnb_success_rate),
         updated_at = NOW()`,
      [uai, candidates, rate]
    );
  }
}

async function loadEnroll2024(client) {
  if (!fs.existsSync(FILES.ENROLL_2024)) return;
  const lookup = await buildEcoleLookup(client);
  const rows = await readCsvRows(FILES.ENROLL_2024);
  for (const r of rows) {
    const etab = getCol(r, ['Dénomination principale', 'Denomination principale']);
    const commune = getCol(r, ['Commune']);
    const dep = getCol(r, ['Département', 'Departement']);
    const total = parseInt(String(getCol(r, ["Nombre total d'élèves", 'Nombre total d eleves']) || '').replace(/\D+/g, '')) || null;
    if (!etab || !commune || !dep || !total) continue;
    const key = `${norm(etab)}|${norm(commune)}|${norm(dep)}`;
    const uai = lookup.get(key);
    if (!uai) continue;
    await client.query(
      `INSERT INTO fr_school_fr_stats (uai, students_total, updated_at)
       VALUES ($1,$2,NOW())
       ON CONFLICT (uai) DO UPDATE SET
         students_total = COALESCE(EXCLUDED.students_total, fr_school_fr_stats.students_total),
         updated_at = NOW()`,
      [uai, total]
    );
    await client.query(
      `UPDATE fr_ecoles SET "Nombre_d_eleves" = $2 WHERE "identifiant_de_l_etablissement" = $1`,
      [uai, String(total)]
    );
  }
}

async function main() {
  const client = await pool.connect();
  try {
    await ensureTables(client);
    console.log('Loading lycee class enrollment…');
    await loadLyceeClasses(client);
    console.log('Loading lycee results…');
    await loadLyceeResults(client);
    console.log('Loading college DNB results…');
    await loadCollegeResults(client);
    console.log('Loading 2024 enrollment per class…');
    await loadEnroll2024(client);
    console.log('Done updating fr_school_fr_stats.');
  } catch (e) {
    console.error('Update failed:', e);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

if (require.main === module) {
  main();
}

