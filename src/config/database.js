const { Pool } = require('pg');

const DEFAULT_DATABASE_URL = 'postgresql://school_platform_db_fr_user:cDW1EB5Ah6x9KCguituTxOs63EZlXgnV@dpg-d37b0r6r433s73ejel20-a.oregon-postgres.render.com/school_platform_db_fr';
const DATABASE_URL = process.env.DATABASE_URL || DEFAULT_DATABASE_URL;

if (!process.env.DATABASE_URL) {
  console.warn('Utilisation de la connexion PostgreSQL francaise par defaut. Definissez DATABASE_URL pour la remplacer.');
}

const dbInfo = DATABASE_URL.split('@')[1]?.split('/');
if (dbInfo) {
  const host = dbInfo[0]?.split('.')?.[0] || 'instance';
  console.log(`Connexion a la base de donnees : ${dbInfo[1]} sur ${host}...`);
}

const poolConfig = {
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  },
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 30000,
  query_timeout: 30000,
};

const pool = new Pool(poolConfig);

pool.on('error', (err) => {
  console.error('Erreur inattendue sur un client PostgreSQL inactif :', err);
});

pool.on('connect', () => {
  console.log('Nouveau client connecte au pool PostgreSQL');
});

const resolveSchoolTable = async (client) => {
  const { rows } = await client.query(`
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name ILIKE '%school%'
    ORDER BY CASE
      WHEN table_name = 'fr_schools' THEN 0
      WHEN table_name = 'schools' THEN 1
      WHEN table_name = 'uk_schools' THEN 2
      ELSE 3
    END, table_name
    LIMIT 1
  `);

  return rows[0]?.table_name || null;
};

const testConnection = async () => {
  let client;
  try {
    client = await pool.connect();

    const timeResult = await client.query('SELECT NOW() as current_time');
    console.log('Test de connexion reussi a :', timeResult.rows[0].current_time);

    const schoolTable = await resolveSchoolTable(client);
    if (schoolTable) {
      const { rows } = await client.query(`SELECT COUNT(*)::int AS school_count FROM ${schoolTable}`);
      console.log('Statistiques de la base de donnees :');
      console.log(`   - Table ecoles : ${schoolTable}`);
      console.log(`   - Etablissements scolaires : ${rows[0].school_count}`);
    } else {
      console.warn("Aucune table d'etablissements trouvee (pattern %school%).");
    }

    return true;
  } catch (err) {
    console.error('Echec du test de connexion a la base de donnees :', err.message);

    if (err.code === '42P01') {
      console.error("La table d'ecoles attendue est absente. Importez la structure de donnees francaise.");
    } else if (err.code === 'ECONNREFUSED') {
      console.error('Connexion refusee. Verifiez la valeur de DATABASE_URL.');
    } else if (err.code === '28P01') {
      console.error("Echec de l'authentification. Controlez vos identifiants.");
    } else if (err.message?.includes('SSL')) {
      console.error('Probleme SSL detecte. Verifiez la configuration SSL.');
    }

    throw err;
  } finally {
    if (client) {
      client.release();
    }
  }
};

const query = async (text, params) => {
  const start = Date.now();
  try {
    const result = await pool.query(text, params);
    const duration = Date.now() - start;

    if (process.env.NODE_ENV === 'development' && duration > 1000) {
      console.log('Requete lente detectee :', {
        requete: text.substring(0, 100),
        duree: `${duration}ms`,
        lignes: result.rowCount,
      });
    }

    return result;
  } catch (error) {
    console.error('Erreur lors de la requete :', {
      erreur: error.message,
      requete: text.substring(0, 100),
      code: error.code,
    });
    throw error;
  }
};

const getClient = async () => {
  return pool.connect();
};

const closePool = async () => {
  console.log('Fermeture du pool de connexions PostgreSQL...');
  await pool.end();
  console.log('Connexions a la base de donnees fermees.');
};

module.exports = {
  pool,
  query,
  testConnection,
  getClient,
  closePool,
};
