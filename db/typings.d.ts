import type { Pool } from 'pg'

export interface PgPools {
  stats: Pool;
  evaluate: Pool;
  end(): Promise<void>
}

// Copied from import('@types/pg').
export type Queryable = Pick<Pool, 'query'>
