import type { Pool } from 'pg'

export interface pgPools {
  stats: Pool;
  evaluate: Pool;
}
