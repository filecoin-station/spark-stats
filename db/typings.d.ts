import type { Pool } from 'pg'

export interface PgPools {
  stats: Pool;
  evaluate: Pool;
}

export interface EndablePgPools extends PgPools {
  end(): Promise<unknown>
}

