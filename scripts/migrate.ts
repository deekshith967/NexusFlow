/**
 * Applies the database migrations for DATABASE_URL.
 *
 * Usage:
 *   npm run migrate
 *
 * Idempotent: already-applied migrations are skipped (tracked in
 * schema_migrations).
 */
import { applyMigrations, checkDatabaseReachable } from '../src/database/migrations/applier';

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
