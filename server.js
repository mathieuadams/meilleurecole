require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');
const path = require('path');

// ---- Database
const { pool, testConnection } = require('./src/config/database');

// ---- API routes
const schoolRoutes = require('./src/routes/schoolRoutes');
const searchRoutes = require('./src/routes/searchRoutes');
const reviewRoutes = require('./src/routes/reviewRoutes');
const contactRoutes = require('./src/routes/contactRoutes');

// ---- App
const app = express();
const PORT = process.env.PORT || 10000;

// Render / proxies
app.set('trust proxy', 1);

// ---- Security
app.use(helmet({
  crossOriginResourcePolicy: { policy: 'cross-origin' },
  contentSecurityPolicy: false,  // allow inline for now
}));

// ---- CORS
app.use(cors({
  origin: process.env.FRONTEND_URL || '*',
  credentials: true,
}));

// ---- Rate limit (API only)
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: 'Too many requests from this IP, please try again later.',
});
app.use('/api/', limiter);

// ---- Parsers
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ---- Perf / logs
app.use(compression());
if (process.env.NODE_ENV !== 'test') app.use(morgan('dev'));

// ---- Static
const PUBLIC_DIR = path.join(__dirname, 'public');
app.use(express.static(PUBLIC_DIR));
app.use('/css', express.static(path.join(PUBLIC_DIR, 'css')));
app.use('/js', express.static(path.join(PUBLIC_DIR, 'js')));
app.use('/components', express.static(path.join(PUBLIC_DIR, 'components')));
app.use('/images', express.static(path.join(PUBLIC_DIR, 'images')));

// ---- Health
app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      environment: process.env.NODE_ENV || 'development',
      database: 'connected',
    });
  } catch (err) {
    console.error('Health check failed:', err);
    res.status(503).json({ status: 'unhealthy', error: 'Database connection failed' });
  }
});

// ---- API Welcome
app.get('/api', (_req, res) => {
  res.json({
    message: 'Welcome to FindSchool.uk API',
    version: '1.0.0',
    endpoints: {
      health: '/health',
      search: '/api/search',
      schools: '/api/schools/:urn',
      contact: '/api/contact',
    },
  });
});

// ---- API Routers
app.use('/api/schools', schoolRoutes);
app.use('/api/search', searchRoutes);
app.use('/api', reviewRoutes);
app.use('/api', contactRoutes);

// ---- HTML pages helper function
const sendPublic = (res, file) => res.sendFile(path.join(PUBLIC_DIR, file));

// ---- Main pages
app.get('/', (_req, res) => sendPublic(res, 'index.html'));
app.get(['/search', '/search.html'], (_req, res) => sendPublic(res, 'search.html'));
app.get(['/compare', '/compare.html'], (_req, res) => sendPublic(res, 'compare.html'));

// ---- Review pages
app.get(['/review','/review.html'], (_req, res) => sendPublic(res, 'review.html'));
app.get(['/write-review','/write-review.html'], (_req, res) => sendPublic(res, 'write-review.html'));

// ---- Legal and informational pages
app.get(['/about','/about.html'], (_req, res) => sendPublic(res, 'about.html'));
app.get(['/terms','/terms.html'], (_req, res) => sendPublic(res, 'terms.html'));
app.get(['/privacy','/privacy.html'], (_req, res) => sendPublic(res, 'privacy.html'));
app.get(['/contact','/contact.html'], (_req, res) => sendPublic(res, 'contact.html'));
app.get(['/data-sources','/data-sources.html'], (_req, res) => sendPublic(res, 'data-sources.html'));
app.get(['/methodology','/methodology.html'], (_req, res) => sendPublic(res, 'methodology.html'));
app.get(['/faq','/faq.html'], (_req, res) => sendPublic(res, 'faq.html'));

// ---- School type specific search redirects (from footer links)
app.get('/schools/primary', (_req, res) => {
  res.redirect('/search?type=primary');
});

app.get('/schools/secondary', (_req, res) => {
  res.redirect('/search?type=secondary');
});

app.get('/schools/sixth-form', (_req, res) => {
  res.redirect('/search?type=sixth-form');
});

app.get('/schools/special', (_req, res) => {
  res.redirect('/search?type=special');
});

app.get('/schools/independent', (_req, res) => {
  res.redirect('/search?type=independent');
});

app.get('/schools/academies', (_req, res) => {
  res.redirect('/search?type=academy');
});

// ---- Popular area search redirects (from footer links)
app.get('/schools/london', (_req, res) => {
  res.redirect('/search?area=London');
});

app.get('/schools/manchester', (_req, res) => {
  res.redirect('/search?area=Manchester');
});

app.get('/schools/birmingham', (_req, res) => {
  res.redirect('/search?area=Birmingham');
});

app.get('/schools/leeds', (_req, res) => {
  res.redirect('/search?area=Leeds');
});

app.get('/schools/glasgow', (_req, res) => {
  res.redirect('/search?area=Glasgow');
});

app.get('/schools/edinburgh', (_req, res) => {
  res.redirect('/search?area=Edinburgh');
});

// ---- Alternative URL patterns
app.get('/primary-schools', (_req, res) => {
  res.redirect('/search?type=primary');
});

app.get('/secondary-schools', (_req, res) => {
  res.redirect('/search?type=secondary');
});

// ---- Quick links redirects (placeholder until pages are built)
app.get('/saved-schools', (_req, res) => {
  // Redirect to home until user accounts are implemented
  res.redirect('/');
});

app.get('/parent-resources', (_req, res) => {
  // Redirect to FAQ until resources page is created
  res.redirect('/faq');
});

app.get('/school-guides', (_req, res) => {
  // Redirect to methodology until guides section is created
  res.redirect('/methodology');
});

app.get('/education-glossary', (_req, res) => {
  // Redirect to FAQ until glossary is created
  res.redirect('/faq');
});

// ---- City routing
const ukCities = new Set([
  'london','birmingham','glasgow','liverpool','bristol','manchester',
  'sheffield','leeds','edinburgh','leicester','coventry','bradford',
  'cardiff','belfast','nottingham','kingston-upon-hull','newcastle',
  'stoke-on-trent','southampton','portsmouth','derby','plymouth',
  'wolverhampton','swansea','milton-keynes','northampton','york',
  'oxford','cambridge','norwich','brighton','bath','canterbury',
  'exeter','chester','durham','salisbury','lancaster','worcester','lincoln'
]);

// Serve city page at /:city (static SEO-friendly path)
app.get('/:city', (req, res, next) => {
  const { city } = req.params;
  
  // Reserved words - avoid intercepting these paths
  const reserved = new Set([
    'api','css','js','components','images','favicon.ico',
    'school','schools','health','compare','about','search',
    'review','write-review','terms','privacy','contact',
    'data-sources','methodology','faq',
    'primary-schools','secondary-schools',
    'saved-schools','parent-resources',
    'school-guides','education-glossary'
  ]);
  
  if (reserved.has(city.toLowerCase())) return next();

  // Serve city page for any non-reserved slug (do not restrict to known list)
  return sendPublic(res, 'city.html');
});

// ---- Local Authority routing
// Serve local authority page at /local-authority/:laSlug
app.get('/local-authority/:laSlug', (_req, res) => sendPublic(res, 'local-authority.html'));

// Serve local authority page at /:city/:laSlug (when it's not a school)
app.get('/:city/:identifier', (req, res, next) => {
  const { city, identifier } = req.params;
  
  // Reserved words
  const reserved = new Set([
    'api','css','js','components','images','favicon.ico',
    'health','compare','about','search',
    'review','write-review','terms','privacy','contact',
    'data-sources','methodology','faq'
  ]);
  
  if (reserved.has(city.toLowerCase())) return next();

  // Check if identifier looks like a URN (all digits or digits with dash)
  if (/^\d+(-.*)?$/.test(identifier)) {
    // It's a school (URN or URN-slug)
    return sendPublic(res, 'school.html');
  } else {
    // It's a local authority
    return sendPublic(res, 'local-authority.html');
  }
});

// ---- API endpoint for local authority summary
app.get('/api/local-authority/:laName/summary', async (req, res) => {
  const { laName } = req.params;
  
  try {
    // Get all schools in this LA
    const schoolsQuery = `
      SELECT 
        s.urn,
        s.name,
        s.postcode,
        s.town,
        s.local_authority,
        s.region,
        s.phase_of_education,
        s.type_of_establishment,
        s.gender,
        s.religious_character,
        s.english_score,
        s.math_score,
        s.science_score,
        s.overall_rating,
        o.overall_effectiveness as ofsted_rating,
        COALESCE(c.number_on_roll, s.total_pupils) AS number_on_roll,
        c.percentage_fsm_ever6 as fsm_percentage,
        a.overall_absence_rate
      FROM uk_schools s
      LEFT JOIN uk_ofsted_inspections o ON s.urn = o.urn
      LEFT JOIN uk_census_data c ON s.urn = c.urn
      LEFT JOIN uk_absence_data a ON s.urn = a.urn
      WHERE LOWER(s.local_authority) = LOWER($1)
    `;
    
    const result = await pool.query(schoolsQuery, [laName]);
    const schools = result.rows;
    
    // Extract common city/town from schools
    let mostCommonCity = null;
    let region = null;
    if (schools.length > 0) {
      // Find most common town
      const townCounts = {};
      schools.forEach(school => {
        if (school.town) {
          townCounts[school.town] = (townCounts[school.town] || 0) + 1;
        }
        // Also capture region from first school that has it
        if (!region && school.region) {
          region = school.region;
        }
      });
      
      if (Object.keys(townCounts).length > 0) {
        mostCommonCity = Object.keys(townCounts).reduce((a, b) => 
          townCounts[a] > townCounts[b] ? a : b
        );
      }
    }
    
    // Process summary statistics
    let primaryCount = 0;
    let secondaryCount = 0;
    let sixthFormCount = 0;
    let specialCount = 0;
    let totalStudents = 0;
    
    let ofstedCounts = {
      outstanding: 0,
      good: 0,
      requiresImprovement: 0,
      inadequate: 0,
      notInspected: 0
    };
    
    let englishScores = [];
    let mathScores = [];
    let scienceScores = [];
    let attendanceRates = [];
    let fsmPercentages = [];
    
    schools.forEach(school => {
      // Count by phase
      const phase = (school.phase_of_education || '').toLowerCase();
      if (phase.includes('primary')) primaryCount++;
      if (phase.includes('secondary')) secondaryCount++;
      if (phase.includes('sixth') || phase.includes('16')) sixthFormCount++;
      
      // Check for special schools
      const type = (school.type_of_establishment || '').toLowerCase();
      if (type.includes('special')) specialCount++;
      
      // Count students
      if (school.number_on_roll) {
        totalStudents += parseInt(school.number_on_roll) || 0;
      }
      
      // Count Ofsted ratings
      switch(school.ofsted_rating) {
        case 1: ofstedCounts.outstanding++; break;
        case 2: ofstedCounts.good++; break;
        case 3: ofstedCounts.requiresImprovement++; break;
        case 4: ofstedCounts.inadequate++; break;
        default: ofstedCounts.notInspected++; break;
      }
      
      // Collect performance metrics
      if (school.english_score) englishScores.push(parseFloat(school.english_score));
      if (school.math_score) mathScores.push(parseFloat(school.math_score));
      if (school.science_score) scienceScores.push(parseFloat(school.science_score));
      if (school.overall_absence_rate) {
        attendanceRates.push(100 - parseFloat(school.overall_absence_rate));
      }
      if (school.fsm_percentage) fsmPercentages.push(parseFloat(school.fsm_percentage));
    });
    
    // Calculate averages
    const avg = arr => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : null;
    
    res.json({
      success: true,
      laName: laName,
      city: mostCommonCity,
      region: region,
      totalSchools: schools.length,
      totalStudents: totalStudents,
      primaryCount: primaryCount,
      secondaryCount: secondaryCount,
      sixthFormCount: sixthFormCount,
      specialCount: specialCount,
      ofstedCounts: ofstedCounts,
      avgEnglish: avg(englishScores),
      avgMaths: avg(mathScores),
      avgScience: avg(scienceScores),
      avgAttendance: avg(attendanceRates),
      avgFSM: avg(fsmPercentages),
      schools: schools.map(school => ({
        ...school,
        overall_rating: school.ofsted_rating ? 
          (school.ofsted_rating === 1 ? 9 : 
           school.ofsted_rating === 2 ? 7 :
           school.ofsted_rating === 3 ? 5 : 3) : 5
      }))
    });
    
  } catch (error) {
    console.error('Error fetching LA summary:', error);
    res.status(500).json({ 
      error: 'Failed to fetch local authority summary',
      message: error.message 
    });
  }
});

const toSlug = (str = '') => {
  if (!str) return '';
  return String(str)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
};

// Serve school page at /school/:identifier (URN or URN-name)
app.get('/school/:identifier', async (req, res) => {
  const { identifier } = req.params;
  const match = String(identifier || '').match(/^(\d{4,})(?:-(.*))?$/);

  if (!match) {
    return sendPublic(res, 'school.html');
  }

  const urn = match[1];
  const existingSlug = match[2] ? match[2].toLowerCase() : null;

  try {
    const { rows } = await pool.query('SELECT name FROM uk_schools WHERE urn = $1 LIMIT 1', [urn]);
    if (rows && rows.length) {
      const canonicalSlug = toSlug(rows[0].name);
      if (canonicalSlug && (!existingSlug || existingSlug !== canonicalSlug)) {
        const query = req.originalUrl.includes('?') ? req.originalUrl.slice(req.originalUrl.indexOf('?')) : '';
        return res.redirect(301, `/school/${urn}-${canonicalSlug}${query}`);
      }
    }
  } catch (err) {
    console.warn('Failed to resolve school slug for', urn, err.message);
  }

  return sendPublic(res, 'school.html');
});

// Serve school page at /:city/:schoolIdentifier
app.get('/:city/:schoolIdentifier', (req, res, next) => {
  const { city } = req.params;
  // Don't swallow assets or API
  const reserved = new Set([
    'api','css','js','components','images','favicon.ico',
    'health','compare','about','search'
  ]);
  if (reserved.has(city.toLowerCase())) return next();
  if (!ukCities.has(city.toLowerCase())) return next();
  return sendPublic(res, 'school.html');
});

// ---- Catch-all and 404 handling
app.get('*', (req, res) => {
  // If an unknown API route: proper 404 JSON
  if (req.path.startsWith('/api')) {
    return res.status(404).json({ error: 'API endpoint not found', path: req.path });
  }
  // For unknown front-end paths, return 404
  return res.status(404).send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>404 - Page Not Found</title>
      <style>
        body {
          font-family: 'Inter', -apple-system, sans-serif;
          display: flex;
          justify-content: center;
          align-items: center;
          height: 100vh;
          margin: 0;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .container {
          text-align: center;
          color: white;
        }
        h1 { font-size: 4rem; margin: 0; }
        p { font-size: 1.25rem; margin: 1rem 0 2rem; }
        a {
          display: inline-block;
          padding: 0.75rem 2rem;
          background: white;
          color: #667eea;
          text-decoration: none;
          border-radius: 8px;
          font-weight: 600;
        }
        a:hover {
          transform: translateY(-2px);
          box-shadow: 0 10px 20px rgba(0,0,0,0.2);
        }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>404</h1>
        <p>Page not found</p>
        <a href="/">Go to Homepage</a>
      </div>
    </body>
    </html>
  `);
});

// ---- Error handler
app.use((err, _req, res, _next) => {
  console.error('Error:', err.stack);
  res.status(err.status || 500).json({
    error: err.message || 'Internal Server Error',
    ...(process.env.NODE_ENV === 'development' && { stack: err.stack }),
  });
});

// ---- Start server
const startServer = async () => {
  try {
    console.log('🔍 Testing database connection...');
    await testConnection();
    console.log('✅ Database connected successfully');

    app.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Server running on port ${PORT}`);
      console.log(`🔗 Health:  http://localhost:${PORT}/health`);
      console.log(`📚 API:     http://localhost:${PORT}/api`);
      console.log(`🌐 Website: http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('❌ Failed to start server:', error.message);
    console.error('Check your DATABASE_URL environment variable.');
    process.exit(1);
  }
};

startServer();

// ---- Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received. Closing connections...');
  try { await pool.end(); } catch {}
  process.exit(0);
});
