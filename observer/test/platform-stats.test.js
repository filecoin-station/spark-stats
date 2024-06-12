import assert from 'node:assert'
import { beforeEach, describe, it } from 'mocha'

import { getStatsPgPool } from '@filecoin-station/spark-stats-db'
import { updateDailyTransferStats } from '../lib/platform-stats.js'
import { migrateWithPgClient } from '@filecoin-station/spark-stats-db-migrations'

describe('platform-stats-generator', () => {
  /** @type {pg.Client} */
  let pgClient

  before(async () => {
    const pgPool = await getStatsPgPool()
    pgClient = await pgPool.connect()
    await migrateWithPgClient(pgClient)
  })

  let today
  beforeEach(async () => {
    await pgClient.query('DELETE FROM daily_reward_transfers')

    // Run all tests inside a transaction to ensure `now()` always returns the same value
    // See https://dba.stackexchange.com/a/63549/125312
    // This avoids subtle race conditions when the tests are executed around midnight.
    await pgClient.query('BEGIN TRANSACTION')
    today = await getCurrentDate()
  })

  afterEach(async () => {
    await pgClient.query('END TRANSACTION')
  })

  after(async () => {
    await pgClient.release()
  })

  describe('updateDailyTransferStats', () => {
    it('should correctly update daily Transfer stats with new transfer events', async () => {
      await updateDailyTransferStats(pgClient, { toAddress: 'address1', amount: 100 }, 1)
      await updateDailyTransferStats(pgClient, { toAddress: 'address1', amount: 200 }, 2)

      const { rows } = await pgClient.query(`
        SELECT day::TEXT, to_address, amount, last_checked_block FROM daily_reward_transfers
        `)
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{
        day: today, to_address: 'address1', amount: '300', last_checked_block: 2
      }])
    })

    it('should handle multiple addresses in daily Transfer stats', async () => {
      await updateDailyTransferStats(pgClient, { toAddress: 'address1', amount: 50 }, 1)
      await updateDailyTransferStats(pgClient, { toAddress: 'address2', amount: 150 }, 1)

      const { rows } = await pgClient.query(`
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
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
