// Orchestrates loading French data into the existing database
// 1) Loads base schools from french_school.csv into fr_ecoles
// 2) Updates aggregates (totals, lycee, boys/girls, bac/DNB) from the other CSVs

const { spawnSync } = require('child_process');

function runStep(cmd, args, env = process.env) {
  console.log(`\n▶ ${[cmd, ...args].join(' ')}`);
  const res = spawnSync(cmd, args, { stdio: 'inherit', env });
  if (res.error) {
    console.error(`✖ Failed: ${cmd} ${args.join(' ')}`);
    console.error(res.error);
    process.exit(res.status || 1);
  }
  if (res.status !== 0) {
    console.error(`✖ Exit code ${res.status} from: ${cmd} ${args.join(' ')}`);
    process.exit(res.status);
  }
}

function main() {
  // Show target DB for safety
  const dbUrl = process.env.DATABASE_URL || '(default in code)';
  console.log('Using DATABASE_URL =', dbUrl);

  // Step 1: base load of French schools (creates fr_ecoles + review tables)
  runStep('node', ['scripts/setup_french_db.js']);

  // Step 2: add/update aggregate fields on fr_ecoles from the four CSVs
  runStep('node', ['scripts/update_french_enrollment_and_results.js']);

  console.log('\n✅ French data load completed successfully.');
}

if (require.main === module) {
  main();
}

