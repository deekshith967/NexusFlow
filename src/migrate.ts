/**
 * Applies the database migrations for DATABASE_URL.
 *
 * Usage (development, from a checkout):
 *   npm run migrate
 *
 * Usage (production, from compiled output — no ts-node needed):
 *   npm run build && npm run migrate:prod
 *
 * Idempotent: already-applied migrations are skipped (tracked in
 * schema_migrations). Honors DATABASE_SSL / sslmode=require for
 * TLS-mandating PostgreSQL endpoints (leave DATABASE_SSL unset for
 * Render's internal URL). Run once per deploy, from a single process:
 * concurrent migrates are not coordinated.
 */
import {
  applyMigrations,
  checkDatabaseReachable,
} from './database/migrations/applier';

async function main(): Promise<void> {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    console.error('DATABASE_URL is required');
    process.exit(1);
  }

  const version = await checkDatabaseReachable(connectionString);
  console.log(`Connected to: ${version.split('\n')[0]}`);

  const results = await applyMigrations(connectionString);

  for (const result of results) {
    console.log(
      result.applied ? `applied:   ${result.filename}` : `skipped:   ${result.filename}`,
    );
  }

  console.log('Migrations complete.');
}

main().catch((error) => {
  console.error(`Migration failed: ${(error as Error).message}`);
  process.exit(1);
});
