import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { updateDailyTransferStats } from '../lib/platform-stats.js'
import { migrateWithPgClient } from '@filecoin-station/spark-stats-db-migrations'

describe('platform-stats-generator', () => {
  /** @type {pg.Pool} */
  let pgPool

  before(async () => {
    pgPool = new pg.Pool({ connectionString: DATABASE_URL })
    await migrateWithPgClient(pgPool)
  })

  let today
  beforeEach(async () => {
    await pgPool.query('DELETE FROM daily_reward_transfers')

    // Run all tests inside a transaction to ensure `now()` always returns the same value
    // See https://dba.stackexchange.com/a/63549/125312
    // This avoids subtle race conditions when the tests are executed around midnight.
    await pgPool.query('BEGIN TRANSACTION')
    today = await getCurrentDate()
  })

  afterEach(async () => {
    await pgPool.query('END TRANSACTION')
  })

  after(async () => {
    await pgPool.end()
  })

  describe('updateDailyTransferStats', () => {
    it('should correctly update daily Transfer stats with new transfer events', async () => {
      await updateDailyTransferStats(pgPool, { to_address: 'address1', amount: 100 }, 1)
      await updateDailyTransferStats(pgPool, { to_address: 'address1', amount: 200 }, 2)

      const { rows } = await pgPool.query(`
        SELECT day::TEXT, to_address, amount, last_checked_block FROM daily_reward_transfers
        `)
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{
        day: today, to_address: 'address1', amount: '300', last_checked_block: 2
      }])
    })

    it('should handle multiple addresses in daily Transfer stats', async () => {
      await updateDailyTransferStats(pgPool, { to_address: 'address1', amount: 50 }, 1)
      await updateDailyTransferStats(pgPool, { to_address: 'address2', amount: 150 }, 1)

      const { rows } = await pgPool.query(`
        SELECT day::TEXT, to_address, amount, last_checked_block FROM daily_reward_transfers
        ORDER BY to_address
      `)
      assert.strictEqual(rows.length, 2)

      assert.deepStrictEqual(rows, [
        { day: today, to_address: 'address1', amount: '50', last_checked_block: 1 },
        { day: today, to_address: 'address2', amount: '150', last_checked_block: 1 }
      ])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgPool.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
