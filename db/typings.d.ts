import type { Pool } from 'pg'

export interface pgPools {
  pgPool: Pool;
  pgPoolEvaluate: Pool;
}
