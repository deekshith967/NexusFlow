// Minimal ambient declaration for express. The project runs with
// esModuleInterop disabled and does not install @types/express; only the
// few members used by the API layer are declared.
declare module 'express' {
  export interface Request {
    headers: Record<string, string | string[] | undefined>;
  }

  export interface Response {
    status(code: number): Response;
    json(body: unknown): Response;
  }
}
