import type { Pool } from 'pg'

export interface PgPoolEvaluate extends pg.Pool {
  db: 'evaluate'
}

export interface PgPoolStats extends pg.Pool {
  db: 'stats'
}

export type PgPool =
 | PgPoolEvaluate
 | PgPoolStats

export interface PgPools {
  stats: PgPoolStats;
  evaluate: PgPoolEvaluate;
  end(): Promise<void>
}

// Copied from import('@types/pg').
export type Queryable = Pick<Pool, 'query'>
