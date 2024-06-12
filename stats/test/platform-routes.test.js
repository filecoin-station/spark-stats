import http from 'node:http'
import { once } from 'node:events'
import assert from 'node:assert'
import createDebug from 'debug'
import { getPgPools } from '@filecoin-station/spark-stats-db'

import { assertResponseStatus } from './test-helpers.js'
import { createHandler } from '../lib/handler.js'

const debug = createDebug('test')

describe('Platform Routes HTTP request handler', () => {
  /** @type {import('@filecoin-station/spark-stats-db').pgPools} */
  let pgPools
  let server
  /** @type {string} */
  let baseUrl

  before(async () => {
    pgPools = await getPgPools()

    const handler = createHandler({
      pgPools,
      logger: {
        info: debug,
        error: console.error,
        request: debug
      }
    })

    server = http.createServer(handler)
    server.listen()
    await once(server, 'listening')
    baseUrl = `http://127.0.0.1:${server.address().port}`
  })

  after(async () => {
    server.closeAllConnections()
    server.close()
    await pgPools.end()
  })

  beforeEach(async () => {
    await pgPools.evaluate.query('DELETE FROM daily_stations')
    await pgPools.stats.query('DELETE FROM daily_reward_transfers')
  })

  describe('GET /stations/daily', () => {
    it('returns daily station metrics for the given date range', async () => {
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-10', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-11', [
        { stationId: 'station2', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-12', [
        { stationId: 'station2', acceptedMeasurementCount: 2 },
        { stationId: 'station3', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-13', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])

      const res = await fetch(
        new URL(
          '/stations/daily?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const metrics = await res.json()
      assert.deepStrictEqual(metrics, [
        { day: '2024-01-11', station_id_count: 1 },
        { day: '2024-01-12', station_id_count: 2 }
      ])
    })
  })

  describe('GET /stations/monthly', () => {
    it('returns monthly station metrics for the given date range ignoring the day number', async () => {
      // before the date range
      await givenDailyStationMetrics(pgPools.evaluate, '2023-12-31', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])
      // in the date range
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-10', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-11', [
        { stationId: 'station2', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-12', [
        { stationId: 'station2', acceptedMeasurementCount: 2 },
        { stationId: 'station3', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-02-13', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])
      // after the date range
      await givenDailyStationMetrics(pgPools.evaluate, '2024-03-01', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])

      const res = await fetch(
        new URL(
          '/stations/monthly?from=2024-01-11&to=2024-02-11',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const metrics = await res.json()
      assert.deepStrictEqual(metrics, [
        { month: '2024-01-01', station_id_count: 3 },
        { month: '2024-02-01', station_id_count: 1 }
      ])
    })
  })

  describe('GET /measurements/daily', () => {
    it('returns daily total accepted measurement count for the given date range', async () => {
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-10', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-11', [
        { stationId: 'station2', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-12', [
        { stationId: 'station2', acceptedMeasurementCount: 2 },
        { stationId: 'station3', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-13', [
        { stationId: 'station1', acceptedMeasurementCount: 1 }
      ])

      const res = await fetch(
        new URL(
          '/measurements/daily?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const metrics = await res.json()
      assert.deepStrictEqual(metrics, [
        { day: '2024-01-11', accepted_measurement_count: '1' },
        { day: '2024-01-12', accepted_measurement_count: '3' }
      ])
    })
  })

  describe('GET /transfers/daily', () => {
    it('returns daily total Rewards sent for the given date range', async () => {
      await givenDailyRewardTransferMetrics(pgPools.stats, '2024-01-10', [
        { toAddress: 'to1', amount: 100, lastCheckedBlock: 1 }
      ])
      await givenDailyRewardTransferMetrics(pgPools.stats, '2024-01-11', [
        { toAddress: 'to2', amount: 150, lastCheckedBlock: 1 }
      ])
      await givenDailyRewardTransferMetrics(pgPools.stats, '2024-01-12', [
        { toAddress: 'to2', amount: 300, lastCheckedBlock: 1 },
        { toAddress: 'to3', amount: 250, lastCheckedBlock: 1 }
      ])
      await givenDailyRewardTransferMetrics(pgPools.stats, '2024-01-13', [
        { toAddress: 'to1', amount: 100, lastCheckedBlock: 1 }
      ])

      const res = await fetch(
        new URL(
          '/transfers/daily?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const metrics = await res.json()
      assert.deepStrictEqual(metrics, [
        { day: '2024-01-11', amount: '150' },
        { day: '2024-01-12', amount: '550' }
      ])
    })
  })
})

const givenDailyStationMetrics = async (pgPoolEvaluate, day, stationStats) => {
  await pgPoolEvaluate.query(`
    INSERT INTO daily_stations (day, station_id, accepted_measurement_count)
    SELECT $1 AS day, UNNEST($2::text[]) AS station_id, UNNEST($3::int[]) AS accepted_measurement_count
    ON CONFLICT DO NOTHING
    `, [
    day,
    stationStats.map(s => s.stationId),
    stationStats.map(s => s.acceptedMeasurementCount)
  ])
}

const givenDailyRewardTransferMetrics = async (pgPoolStats, day, transferStats) => {
  await pgPoolStats.query(`
    INSERT INTO daily_reward_transfers (day, to_address, amount, last_checked_block)
    SELECT $1 AS day, UNNEST($2::text[]) AS to_address, UNNEST($3::int[]) AS amount, UNNEST($4::int[]) AS last_checked_block
    ON CONFLICT DO NOTHING
    `, [
    day,
    transferStats.map(s => s.toAddress),
    transferStats.map(s => s.amount),
    transferStats.map(s => s.lastCheckedBlock)
  ])
}
