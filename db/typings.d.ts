import pg from 'pg'

export interface PgPoolEvaluate extends pg.Pool {
  db: 'evaluate'
}

export interface PgPoolStats extends pg.Pool {
  db: 'stats'
}

export interface PgPoolApi extends pg.Pool {
  db: 'api'
}

export type PgPool =
 | PgPoolEvaluate
 | PgPoolStats
 | PgPoolApi

export interface PgPools {
  stats: PgPoolStats;
  evaluate: PgPoolEvaluate;
  api: PgPoolApi;
  end(): Promise<void>
}

// Copied from import('@types/pg').
export type Queryable = Pick<Pool, 'query'>
