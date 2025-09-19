const { Pool } = require('pg');

// Get database URL from environment variable
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error('❌ DATABASE_URL environment variable is not set!');
  console.error('Please add it in Render dashboard or .env file for local development');
  process.exit(1);
}

// Log connection info (without password)
const dbInfo = DATABASE_URL.split('@')[1]?.split('/');
if (dbInfo) {
  console.log(`📊 Connecting to database: ${dbInfo[1]} on ${dbInfo[0].split('.')[0]}...`);
}

// Configure connection pool with proper SSL settings for Render
const poolConfig = {
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false  // Required for Render PostgreSQL
  },
  max: 20, // Maximum number of clients in the pool
  idleTimeoutMillis: 30000, // How long a client is allowed to remain idle
  connectionTimeoutMillis: 10000, // How long to wait for connection
  statement_timeout: 30000, // Timeout for statements
  query_timeout: 30000, // Timeout for queries
};

// Create the pool
const pool = new Pool(poolConfig);

// Handle pool errors
pool.on('error', (err, client) => {
  console.error('Unexpected error on idle PostgreSQL client:', err);
});

pool.on('connect', () => {
  console.log('New client connected to PostgreSQL pool');
});

// Test connection function
const testConnection = async () => {
  let client;
  try {
    client = await pool.connect();
    
    // Test basic connection
    const timeResult = await client.query('SELECT NOW() as current_time');
    console.log('✅ Database connection test passed at:', timeResult.rows[0].current_time);
    
    // Check if uk_schools table exists and has data
    const tableCheck = await client.query(`
      SELECT 
        COUNT(*) as school_count,
        (SELECT COUNT(*) FROM information_schema.tables 
         WHERE table_schema = 'public' 
         AND table_name LIKE 'uk_%') as table_count
      FROM uk_schools
      LIMIT 1
    `);
    
    console.log(`📚 Database stats:`);
    console.log(`   - Tables found: ${tableCheck.rows[0].table_count}`);
    console.log(`   - Schools in database: ${tableCheck.rows[0].school_count}`);
    
    return true;
  } catch (err) {
    console.error('❌ Database connection test failed:', err.message);
    
    if (err.code === '42P01') {
      console.error('Table "uk_schools" does not exist. Please import your database schema.');
    } else if (err.code === 'ECONNREFUSED') {
      console.error('Could not connect to database. Check your DATABASE_URL.');
    } else if (err.code === '28P01') {
      console.error('Authentication failed. Check your database credentials.');
    } else if (err.message.includes('SSL')) {
      console.error('SSL connection issue. Make sure SSL is properly configured.');
    }
    
    throw err;
  } finally {
    if (client) {
      client.release();
    }
  }
};

// Query wrapper with error handling and logging
const query = async (text, params) => {
  const start = Date.now();
  try {
    const result = await pool.query(text, params);
    const duration = Date.now() - start;
    
    // Log slow queries in development
    if (process.env.NODE_ENV === 'development' && duration > 1000) {
      console.log('⚠️ Slow query detected:', {
        query: text.substring(0, 100),
        duration: `${duration}ms`,
        rows: result.rowCount
      });
    }
    
    return result;
  } catch (error) {
    console.error('Query error:', {
      error: error.message,
      query: text.substring(0, 100),
      code: error.code
    });
    throw error;
  }
};

// Get a client from the pool (for transactions)
const getClient = async () => {
  const client = await pool.connect();
  return client;
};

// Close all connections (for graceful shutdown)
const closePool = async () => {
  console.log('Closing database connection pool...');
  await pool.end();
  console.log('Database connections closed.');
};

module.exports = {
  pool,
  query,
  testConnection,
  getClient,
  closePool
};