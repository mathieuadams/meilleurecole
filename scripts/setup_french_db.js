const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { parse } = require('csv-parse');

const DEFAULT_DATABASE_URL = 'postgresql://school_platform_db_fr_user:cDW1EB5Ah6x9KCguituTxOs63EZlXgnV@dpg-d37b0r6r433s73ejel20-a.oregon-postgres.render.com/school_platform_db_fr';
const DATABASE_URL = process.env.DATABASE_URL || DEFAULT_DATABASE_URL;
const CSV_FILE = path.join(__dirname, '..', 'french_school.csv');
const BATCH_SIZE = 200;

if (!fs.existsSync(CSV_FILE)) {
  console.error(`CSV file not found at ${CSV_FILE}`);
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const removeDiacritics = (value) => value.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');

const sanitizeHeader = (header) => {
  const cleaned = removeDiacritics(header)
    .replace(/"/g, '')
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_|_$/g, '')
    .toLowerCase();
  return cleaned || 'col';
};

async function readHeaders() {
  const firstLine = await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(CSV_FILE, { encoding: 'utf8' });
    let buffer = '';
    stream.on('data', (chunk) => {
      buffer += chunk;
      const newlineIndex = buffer.indexOf('\n');
      if (newlineIndex !== -1) {
        stream.close();
        resolve(buffer.slice(0, newlineIndex).trim());
      }
    });
    stream.on('error', reject);
    stream.on('close', () => {
      if (!buffer.length) reject(new Error('Unable to read CSV header row.'));
    });
  });

  const rawHeaders = firstLine
    .split(',')
    .map((h) => h.replace(/^"|"$/g, ''));

  const seen = new Map();
  const columns = rawHeaders.map((raw) => {
    let sanitized = sanitizeHeader(raw);
    if (!sanitized.length) sanitized = 'col';
    if (seen.has(sanitized)) {
      const idx = seen.get(sanitized);
      seen.set(sanitized, idx + 1);
      sanitized = `${sanitized}_${idx + 1}`;
    } else {
      seen.set(sanitized, 1);
    }
    return { raw, name: sanitized };
  });

  return columns;
}

async function createEcolesTable(client, columns) {
  const columnDefinitions = columns.map(({ name }) =>
    name === 'identifiant_de_l_etablissement'
      ? `"${name}" TEXT PRIMARY KEY`
      : `"${name}" TEXT`
  ).join(',\n      ');

  await client.query('DROP TABLE IF EXISTS fr_ecoles CASCADE');
  await client.query(`
    CREATE TABLE fr_ecoles (
      ${columnDefinitions}
    )
  `);

  const columnSet = new Set(columns.map(({ name }) => name));
  if (columnSet.has('nom_etablissement')) {
    await client.query('CREATE INDEX IF NOT EXISTS fr_ecoles_nom_idx ON fr_ecoles (nom_etablissement)');
  }
  if (columnSet.has('nom_commune')) {
    await client.query('CREATE INDEX IF NOT EXISTS fr_ecoles_commune_idx ON fr_ecoles (nom_commune)');
  }
  if (columnSet.has('code_postal')) {
    await client.query('CREATE INDEX IF NOT EXISTS fr_ecoles_postcode_idx ON fr_ecoles (code_postal)');
  }
}

async function createReviewTables(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS fr_school_reviews (
      id SERIAL PRIMARY KEY,
      uai TEXT NOT NULL REFERENCES fr_ecoles("identifiant_de_l_etablissement") ON DELETE CASCADE,
      overall_rating SMALLINT CHECK (overall_rating BETWEEN 1 AND 5),
      learning_rating SMALLINT CHECK (learning_rating BETWEEN 1 AND 5),
      teaching_rating SMALLINT CHECK (teaching_rating BETWEEN 1 AND 5),
      social_emotional_rating SMALLINT CHECK (social_emotional_rating BETWEEN 1 AND 5),
      special_education_rating SMALLINT CHECK (special_education_rating BETWEEN 1 AND 5),
      safety_rating SMALLINT CHECK (safety_rating BETWEEN 1 AND 5),
      family_engagement_rating SMALLINT CHECK (family_engagement_rating BETWEEN 1 AND 5),
      would_recommend BOOLEAN,
      review_text TEXT NOT NULL,
      review_title TEXT,
      reviewer_type TEXT,
      reviewer_name TEXT,
      reviewer_ip INET,
      is_published BOOLEAN DEFAULT TRUE,
      helpful_count INTEGER DEFAULT 0,
      report_count INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await client.query('CREATE INDEX IF NOT EXISTS fr_school_reviews_uai_idx ON fr_school_reviews (uai)');
  await client.query('CREATE INDEX IF NOT EXISTS fr_school_reviews_published_idx ON fr_school_reviews (uai, is_published)');

  await client.query(`
    CREATE TABLE IF NOT EXISTS fr_review_helpful_votes (
      review_id INTEGER REFERENCES fr_school_reviews(id) ON DELETE CASCADE,
      voter_ip INET NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (review_id, voter_ip)
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS fr_review_reports (
      id SERIAL PRIMARY KEY,
      review_id INTEGER REFERENCES fr_school_reviews(id) ON DELETE CASCADE,
      report_reason TEXT NOT NULL,
      report_details TEXT,
      reporter_ip INET,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS fr_school_review_stats (
      uai TEXT PRIMARY KEY,
      total_reviews INTEGER DEFAULT 0,
      avg_overall_rating NUMERIC(4,2),
      recommendation_percentage NUMERIC(5,2),
      avg_family_engagement_rating NUMERIC(4,2),
      avg_learning_rating NUMERIC(4,2),
      avg_teaching_rating NUMERIC(4,2),
      avg_safety_rating NUMERIC(4,2),
      avg_social_emotional_rating NUMERIC(4,2),
      avg_special_education_rating NUMERIC(4,2),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

function dedupeRows(columns, rows) {
  const pkIndex = columns.findIndex(({ name }) => name === 'identifiant_de_l_etablissement');
  if (pkIndex === -1) return rows;
  const map = new Map();
  for (const row of rows) {
    const key = row[pkIndex];
    if (!key) continue;
    map.set(key, row);
  }
  return Array.from(map.values());
}

async function insertBatch(client, columns, rows) {
  if (!rows.length) return;
  const uniqueRows = dedupeRows(columns, rows);
  if (!uniqueRows.length) return;

  const columnNames = columns.map(({ name }) => `"${name}"`);
  const values = [];
  const clauses = uniqueRows.map((row, rowIndex) => {
    const placeholders = row.map((_, colIndex) => {
      values.push(row[colIndex]);
      return `$${rowIndex * columns.length + colIndex + 1}`;
    });
    return `(${placeholders.join(', ')})`;
  });

  const assignments = columns
    .filter(({ name }) => name !== 'identifiant_de_l_etablissement')
    .map(({ name }) => `"${name}" = EXCLUDED."${name}"`)
    .join(', ');

  const sql = `
    INSERT INTO fr_ecoles (${columnNames.join(', ')})
    VALUES ${clauses.join(', ')}
    ON CONFLICT ("identifiant_de_l_etablissement") DO UPDATE SET ${assignments}
  `;

  await client.query(sql, values);
}

async function loadCSV(client, columns) {
  await client.query('TRUNCATE fr_ecoles CASCADE');

  const parser = fs.createReadStream(CSV_FILE)
    .pipe(parse({ columns: true, bom: true, skip_empty_lines: true }));

  const rows = [];
  const rawHeaders = columns.map((col) => col.raw);
  const pkIndex = columns.findIndex(({ name }) => name === 'identifiant_de_l_etablissement');
  if (pkIndex === -1) {
    throw new Error('Primary key column identifiant_de_l_etablissement not found in CSV header.');
  }

  let total = 0;

  for await (const record of parser) {
    const values = rawHeaders.map((header) => {
      const value = record[header];
      if (value === undefined || value === '') return null;
      return value;
    });

    if (!values[pkIndex]) {
      continue; // skip rows without identifier
    }

    rows.push(values);

    if (rows.length >= BATCH_SIZE) {
      const chunk = rows.splice(0, rows.length);
      await insertBatch(client, columns, chunk);
      total += chunk.length;
      process.stdout.write(`\rInserted ${total} records...`);
    }
  }

  if (rows.length) {
    const chunk = rows.splice(0, rows.length);
    await insertBatch(client, columns, chunk);
    total += chunk.length;
  }

  process.stdout.write(`\rInserted ${total} records.\n`);
}

async function main() {
  const columns = await readHeaders();
  const client = await pool.connect();

  try {
    console.log('Creating tables...');
    await createEcolesTable(client, columns);
    await createReviewTables(client);
    console.log('Loading CSV data into fr_ecoles...');
    await loadCSV(client, columns);
    console.log('Database setup completed successfully.');
  } catch (error) {
    console.error('Failed to set up French database:', error);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();
