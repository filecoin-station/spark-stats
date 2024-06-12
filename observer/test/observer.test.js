import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { observeTransferEvents } from '../lib/observer.js'
import { migrateWithPgClient } from '@filecoin-station/spark-stats-db-migrations'

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('observer', () => {
  /** @type {pg.Client} */
  let pgClient
  let ieContractMock
  let providerMock

  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  let today
  beforeEach(async () => {
    await pgClient.query('DELETE FROM daily_reward_transfers')

    // Run all tests inside a transaction to ensure `now()` always returns the same value
    await pgClient.query('BEGIN TRANSACTION')
    today = await getCurrentDate()

    // Mock ieContract and provider
    ieContractMock = {
      filters: {
        Transfer: () => 'TransferEventFilter'
      },
      queryFilter: async () => []
    }
    providerMock = {
      getBlockNumber: async () => 2000
    }
  })

  afterEach(async () => {
    await pgClient.query('END TRANSACTION')
  })

  after(async () => {
    await pgClient.end()
  })

  describe('observeTransferEvents', () => {
    it('should correctly observe and update transfer events', async () => {
      ieContractMock.queryFilter = async (eventName, fromBlock) => {
        const events = [
          { args: { to: 'address1', amount: 100 }, blockNumber: 2000 },
          { args: { to: 'address1', amount: 200 }, blockNumber: 2000 }
        ]
        return events.filter((event) => event.blockNumber >= fromBlock)
      }

      await observeTransferEvents(pgClient, ieContractMock, providerMock)

      const { rows } = await pgClient.query(`
        SELECT day::TEXT, to_address, amount, last_checked_block FROM daily_reward_transfers
      `)
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{
        day: today, to_address: 'address1', amount: '300', last_checked_block: 2000
      }])
    })

    it('should handle multiple addresses in transfer events', async () => {
      ieContractMock.queryFilter = async (eventName, fromBlock) => {
        const events = [
          { args: { to: 'address1', amount: 50 }, blockNumber: 2000 },
          { args: { to: 'address2', amount: 150 }, blockNumber: 2000 }
        ]
        return events.filter((event) => event.blockNumber >= fromBlock)
      }

      await observeTransferEvents(pgClient, ieContractMock, providerMock)

      const { rows } = await pgClient.query(`
        SELECT day::TEXT, to_address, amount, last_checked_block FROM daily_reward_transfers
        ORDER BY to_address
      `)
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        { day: today, to_address: 'address1', amount: '50', last_checked_block: 2000 },
        { day: today, to_address: 'address2', amount: '150', last_checked_block: 2000 }
      ])
    })

    it('should not duplicate transfer events', async () => {
      ieContractMock.queryFilter = async (eventName, fromBlock) => {
        const events = [
          { args: { to: 'address1', amount: 50 }, blockNumber: 2000 },
          { args: { to: 'address1', amount: 50 }, blockNumber: 2000 }
        ]
        return events.filter((event) => event.blockNumber >= fromBlock)
      }

      const numEvents1 = await observeTransferEvents(pgClient, ieContractMock, providerMock)
      assert.strictEqual(numEvents1, 2)

      const numEvents2 = await observeTransferEvents(pgClient, ieContractMock, providerMock)
      assert.strictEqual(numEvents2, 0)

      const { rows } = await pgClient.query(`
        SELECT day::TEXT, to_address, amount, last_checked_block FROM daily_reward_transfers
      `)
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{
        day: today, to_address: 'address1', amount: '100', last_checked_block: 2000
      }])
    })

    it('should avoid querying too old blocks', async () => {
      providerMock.getBlockNumber = async () => 2500
      ieContractMock.queryFilter = async (eventName, fromBlock) => {
        const events = [
          { args: { to: 'address1', amount: 50 }, blockNumber: 400 },
          { args: { to: 'address2', amount: 150 }, blockNumber: 400 },
          { args: { to: 'address1', amount: 250 }, blockNumber: 2000 }
        ]
        return events.filter((event) => event.blockNumber >= fromBlock)
      }

      await observeTransferEvents(pgClient, ieContractMock, providerMock)

      const { rows } = await pgClient.query(`
        SELECT day::TEXT, to_address, amount, last_checked_block FROM daily_reward_transfers
        ORDER BY to_address
      `)
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [
        { day: today, to_address: 'address1', amount: '250', last_checked_block: 2500 }
      ])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
