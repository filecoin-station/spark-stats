// TODO: remove this file once we have real tests in place

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '@filecoin-station/spark-stats-db-migrations'
import pg from 'pg'

describe('spark-observer', () => {
  /** @type {pg.Pool} */
  let pgPool

  before(async () => {
    pgPool = new pg.Pool({ connectionString: DATABASE_URL })
    await migrateWithPgClient(pgPool)
  })

  after(async () => {
    await pgPool.end()
  })

  it('works', async () => {
    await import('../index.js')
  })
})
