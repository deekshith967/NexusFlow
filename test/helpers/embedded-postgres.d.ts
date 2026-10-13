/**
 * Type shim for the `embedded-postgres` package (devDependency). Its
 * `exports` field ships only a JavaScript entry point, so under this
 * project's `node` module resolution the bundled types are not picked up.
 */
declare module 'embedded-postgres' {
  interface EmbeddedPostgresOptions {
    databaseDir: string;
    user: string;
    password: string;
    port: number;
    persistent?: boolean;
  }

  class EmbeddedPostgres {
    constructor(options: EmbeddedPostgresOptions);
    initialise(): Promise<void>;
    start(): Promise<void>;
    createDatabase(name: string): Promise<void>;
    stop(options?: { force?: boolean }): Promise<void>;
  }

  export { EmbeddedPostgres, EmbeddedPostgresOptions };
  export default EmbeddedPostgres;
}
