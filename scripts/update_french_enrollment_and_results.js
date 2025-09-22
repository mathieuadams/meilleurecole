// Update French school aggregates from CSVs directly into fr_ecoles
// - students_total from french_school_etudiant_par_class2024.csv (UAI or match by name+commune+departement)
// - lycée effectifs + boys/girls from french_school_etudiant_par_class_lycee.csv
// - bac results from "Lycee general and pro.csv"
// - DNB results from "college resultats.csv"

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
  Object.keys(record).forEach((k) => map.set(norm(k), k));
  for (const c of candidates) {
    const key = norm(c);
    if (map.has(key)) return record[map.get(key)];
  }
  // fallback contains
  const keys = Array.from(map.keys());
  for (const c of candidates) {
    const key = norm(c);
    const found = keys.find(k => k.includes(key));
    if (found) return record[map.get(found)];
  }
  return null;
}

async function ensureColumnsOnEcoles(client) {
  const alters = [
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS students_total INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS boys_total INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS girls_total INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_students_total INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_effectifs_seconde INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_effectifs_premiere INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_effectifs_terminale INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_bac_candidates INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_bac_success_rate NUMERIC(6,3)',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_mentions_rate NUMERIC(6,3)',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS college_dnb_candidates INTEGER',
    'ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS college_dnb_success_rate NUMERIC(6,3)'
  ];
  for (const sql of alters) {
    try { await client.query(sql); } catch (e) { if (e.code !== '42P07') throw e; }
  }
}

function detectDelimiter(file) {
  try {
    const head = fs.readFileSync(file, { encoding: 'utf8' }).slice(0, 4096);
    const semi = (head.match(/;/g) || []).length;
    const comma = (head.match(/,/g) || []).length;
    return semi > comma ? ';' : ',';
  } catch {
    return ',';
  }
}

async function readCsvRows(file) {
  if (!fs.existsSync(file)) return [];
  const delimiter = detectDelimiter(file);
  const records = [];
  const parser = fs.createReadStream(file, { encoding: 'utf8' })
    .pipe(parse({ columns: true, bom: true, skip_empty_lines: true, delimiter }));
  for await (const rec of parser) records.push(rec);
  console.log(`Parsed ${records.length} rows from ${path.basename(file)} using delimiter '${delimiter}'`);
  return records;
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

async function loadLyceeClasses(client) {
  if (!fs.existsSync(FILES.LYCEE_CLASSES)) return {};
  const rows = await readCsvRows(FILES.LYCEE_CLASSES);
  const byUai = {};
  rows.forEach((r) => {
    const uai = getCol(r, ['UAI', 'Identifiant_de_l_etablissement']);
    if (!uai) return;
    const eff2 = parseNumber(getCol(r, ['Effectifs à la rentrée N 2nde', 'Effectifs a la rentree N 2nde', 'Effectifs 2nde']));
    const eff1 = parseNumber(getCol(r, ['Effectifs à la rentrée N 1ère', 'Effectifs a la rentree N 1ere', 'Effectifs 1ere']));
    const effT = parseNumber(getCol(r, ['Effectifs à la rentrée N Term.', 'Effectifs a la rentree N Term', 'Effectifs Term']));
    const lyceeTotal = [eff2, eff1, effT].filter(x => x != null).reduce((a,b)=>a+b,0) || null;
    const girlKeys = Object.keys(r).filter(k => /filles/i.test(k));
    const boyKeys = Object.keys(r).filter(k => /(gar[çc]ons|garcons)/i.test(k));
    const girls = girlKeys.map(k => parseNumber(r[k]) || 0).reduce((a,b)=>a+b,0) || null;
    const boys = boyKeys.map(k => parseNumber(r[k]) || 0).reduce((a,b)=>a+b,0) || null;
    byUai[String(uai).trim()] = { lyceeTotal, eff2, eff1, effT, girls, boys };
  });
  for (const [uai, v] of Object.entries(byUai)) {
    await client.query(
      `UPDATE fr_ecoles SET 
         lycee_students_total = COALESCE($2, lycee_students_total),
         lycee_effectifs_seconde = COALESCE($3, lycee_effectifs_seconde),
         lycee_effectifs_premiere = COALESCE($4, lycee_effectifs_premiere),
         lycee_effectifs_terminale = COALESCE($5, lycee_effectifs_terminale),
         girls_total = COALESCE($6, girls_total),
         boys_total = COALESCE($7, boys_total)
       WHERE "identifiant_de_l_etablissement" = $1`,
      [uai, v.lyceeTotal, v.eff2, v.eff1, v.effT, v.girls, v.boys]
    );
  }
  console.log(`Lycee classes: updated ${Object.keys(byUai).length} UAI with effectifs/lycee totals`);
}

async function loadLyceeResults(client) {
  if (!fs.existsSync(FILES.LYCEE_RESULTS)) return {};
  const rows = await readCsvRows(FILES.LYCEE_RESULTS);
  for (const r of rows) {
    const uai = getCol(r, ['UAI']);
    if (!uai) continue;
    const candidates = parseInt(String(getCol(r, ["Nombre d'élèves présents au Bac"]) || '').replace(/\D+/g, '')) || null;
    const success = parseNumber(getCol(r, ['Taux de réussite bruts TOTAL', 'Taux de reussite bruts total']));
    const mentions = parseNumber(getCol(r, ['Taux de mentions bruts']));
    await client.query(
      `UPDATE fr_ecoles SET 
         lycee_bac_candidates = COALESCE($2, lycee_bac_candidates),
         lycee_bac_success_rate = COALESCE($3, lycee_bac_success_rate),
         lycee_mentions_rate = COALESCE($4, lycee_mentions_rate)
       WHERE "identifiant_de_l_etablissement" = $1`,
      [String(uai).trim(), candidates, success, mentions]
    );
  }
  console.log(`Lycee results: processed ${rows.length} rows (matched by UAI)`);
}

async function loadCollegeResults(client) {
  if (!fs.existsSync(FILES.COLLEGE_RESULTS)) return;
  const lookup = await buildEcoleLookup(client);
  const rows = await readCsvRows(FILES.COLLEGE_RESULTS);
  for (const r of rows) {
    const etab = getCol(r, ['Etablissement', 'Établissement']);
    const commune = getCol(r, ['Libellé commune', 'Commune']);
    const dep = getCol(r, ['Libellé département', 'Département', 'Departement']);
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
      `UPDATE fr_ecoles SET 
         college_dnb_candidates = COALESCE($2, college_dnb_candidates),
         college_dnb_success_rate = COALESCE($3, college_dnb_success_rate)
       WHERE "identifiant_de_l_etablissement" = $1`,
      [uai, candidates, rate]
    );
  }
  console.log(`College results: processed ${rows.length} rows (matched by name/commune/département)`);
}

async function loadEnroll2024(client) {
  if (!fs.existsSync(FILES.ENROLL_2024)) return;
  const lookup = await buildEcoleLookup(client);
  const rows = await readCsvRows(FILES.ENROLL_2024);
  let matchedByUai = 0, matchedByName = 0;
  for (const r of rows) {
    const total = parseInt(String(getCol(r, ["Nombre total d'élèves", 'Nombre total d eleves']) || '').replace(/\D+/g, '')) || null;
    if (!total) continue;
    let uai = getCol(r, ['UAI', "Numéro de l'école", 'Numero de l ecole', "Numéro de l'ecole", "Numéro de l’ecole"]);
    if (uai) {
      await client.query(
        `UPDATE fr_ecoles SET students_total = COALESCE($2, students_total), "Nombre_d_eleves" = $2
         WHERE "identifiant_de_l_etablissement" = $1`,
        [String(uai).trim(), total]
      );
      matchedByUai++;
      continue;
    }
    const etab = getCol(r, ['Dénomination principale', 'Denomination principale']);
    const commune = getCol(r, ['Commune']);
    const dep = getCol(r, ['Département', 'Departement']);
    if (!etab || !commune || !dep) continue;
    const key = `${norm(etab)}|${norm(commune)}|${norm(dep)}`;
    uai = lookup.get(key);
    if (!uai) continue;
    await client.query(
      `UPDATE fr_ecoles SET students_total = COALESCE($2, students_total), "Nombre_d_eleves" = $2
       WHERE "identifiant_de_l_etablissement" = $1`,
      [uai, total]
    );
    matchedByName++;
  }
  console.log(`2024 enrollment: matched ${matchedByUai} by UAI/Numéro de l'école, ${matchedByName} by name/commune/département.`);
}

async function main() {
  const client = await pool.connect();
  try {
    await ensureColumnsOnEcoles(client);
    console.log('Loading lycee class enrollment...');
    await loadLyceeClasses(client);
    console.log('Loading lycee results...');
    await loadLyceeResults(client);
    console.log('Loading college DNB results...');
    await loadCollegeResults(client);
    console.log('Loading 2024 enrollment per class...');
    await loadEnroll2024(client);
    console.log('French aggregates updated on fr_ecoles.');
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
