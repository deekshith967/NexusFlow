import { types } from 'pg';

let configured = false;

/**
 * PostgreSQL BIGINT (int8) values are returned as strings by the pg driver.
 * NexusFlow only stores small integers in int8 columns (fencing tokens,
 * attempt counts, timeouts), so it is safe to parse them as JS numbers.
 */
export function configurePgTypeParsers(): void {
  if (configured) {
    return;
  }

  configured = true;
  types.setTypeParser(20, (value) => Number(value));
}
