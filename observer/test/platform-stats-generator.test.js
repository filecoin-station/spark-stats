import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { updateDailyFilStats } from '../lib/platform-stats-generator.js'

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('platform-stats-generator', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
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
    await pgClient.end()
  })

  describe('updateDailyFilStats', () => {
    it('should correctly update daily FIL stats with new transfer events', async () => {
      await updateDailyFilStats(pgClient, { to_address: 'address1', amount: 100, blockNumber: 1 })
      await updateDailyFilStats(pgClient, { to_address: 'address1', amount: 200, blockNumber: 1 })

      const { rows } = await pgClient.query(`
        SELECT day::TEXT, to_address, amount FROM daily_reward_transfers
        WHERE to_address = $1
      `, ['address1'])
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{ day: today, to_address: 'address1', amount: '300' }])
    })

    it('should handle multiple addresses in daily FIL stats', async () => {
      await updateDailyFilStats(pgClient, { to_address: 'address1', amount: 50, blockNumber: 1 })
      await updateDailyFilStats(pgClient, { to_address: 'address2', amount: 150, blockNumber: 1 })

      const { rows } = await pgClient.query(`
        SELECT day::TEXT, to_address, amount FROM daily_reward_transfers
        ORDER BY to_address
      `)
      assert.strictEqual(rows.length, 2)

      assert.deepStrictEqual(rows, [
        { day: today, to_address: 'address1', amount: '50' },
        { day: today, to_address: 'address2', amount: '150' }
      ])
    })

    it('should update the last block number', async () => {
      await updateDailyFilStats(pgClient, { to_address: 'address1', amount: 100, blockNumber: 1 })
      await updateDailyFilStats(pgClient, { to_address: 'address2', amount: 200, blockNumber: 2 })

      const { rows } = await pgClient.query('SELECT last_block FROM reward_transfer_last_block')
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0].last_block, 2)
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
