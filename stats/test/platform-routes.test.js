import http from 'node:http'
import { once } from 'node:events'
import assert from 'node:assert'
import pg from 'pg'
import createDebug from 'debug'

import { assertResponseStatus } from './test-helpers.js'
import { createHandler } from '../lib/handler.js'
import { EVALUATE_DB_URL } from '../lib/config.js'

const debug = createDebug('test')

describe('Platform Routes HTTP request handler', () => {
  /** @type {pg.Pool} */
  let pgPool
  /** @type {http.Server} */
  let server
  /** @type {string} */
  let baseUrl

  before(async () => {
    pgPool = new pg.Pool({ connectionString: EVALUATE_DB_URL })

    const handler = createHandler({
      pgPool,
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
    await pgPool.end()
  })

  beforeEach(async () => {
    await pgPool.query('DELETE FROM daily_stations')
    await pgPool.query('DELETE FROM daily_fil')
  })

  describe('GET /stations/daily', () => {
    it('returns daily station metrics for the given date range', async () => {
      await givenDailyStationMetrics(pgPool, '2024-01-10', [
        { station_id: 'station1', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-11', [
        { station_id: 'station2', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-12', [
        { station_id: 'station2', accepted_measurement_count: 2 },
        { station_id: 'station3', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-13', [
        { station_id: 'station1', accepted_measurement_count: 1 }
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
      await givenDailyStationMetrics(pgPool, '2023-12-31', [
        { station_id: 'station1', accepted_measurement_count: 1 }
      ])
      // in the date range
      await givenDailyStationMetrics(pgPool, '2024-01-10', [
        { station_id: 'station1', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-11', [
        { station_id: 'station2', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-12', [
        { station_id: 'station2', accepted_measurement_count: 2 },
        { station_id: 'station3', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-02-13', [
        { station_id: 'station1', accepted_measurement_count: 1 }
      ])
      // after the date range
      await givenDailyStationMetrics(pgPool, '2024-03-01', [
        { station_id: 'station1', accepted_measurement_count: 1 }
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
      await givenDailyStationMetrics(pgPool, '2024-01-10', [
        { station_id: 'station1', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-11', [
        { station_id: 'station2', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-12', [
        { station_id: 'station2', accepted_measurement_count: 2 },
        { station_id: 'station3', accepted_measurement_count: 1 }
      ])
      await givenDailyStationMetrics(pgPool, '2024-01-13', [
        { station_id: 'station1', accepted_measurement_count: 1 }
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

  describe('GET /fil/daily', () => {
    it('returns daily total FIL sent for the given date range', async () => {
      await givenDailyFilMetrics(pgPool, '2024-01-10', [
        { to_address: 'to1', amount: 100 }
      ])
      await givenDailyFilMetrics(pgPool, '2024-01-11', [
        { to_address: 'to2', amount: 150 }
      ])
      await givenDailyFilMetrics(pgPool, '2024-01-12', [
        { to_address: 'to2', amount: 300 },
        { to_address: 'to3', amount: 250 }
      ])
      await givenDailyFilMetrics(pgPool, '2024-01-13', [
        { to_address: 'to1', amount: 100 }
      ])

      const res = await fetch(
        new URL(
          '/fil/daily?from=2024-01-11&to=2024-01-12',
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

const givenDailyStationMetrics = async (pgPool, day, stationStats) => {
  await pgPool.query(`
    INSERT INTO daily_stations (day, station_id, accepted_measurement_count)
    SELECT $1 AS day, UNNEST($2::text[]) AS station_id, UNNEST($3::int[]) AS accepted_measurement_count
    ON CONFLICT DO NOTHING
    `, [
    day,
    stationStats.map(s => s.station_id),
    stationStats.map(s => s.accepted_measurement_count)
  ])
}

const givenDailyFilMetrics = async (pgPool, day, filStats) => {
  await pgPool.query(`
    INSERT INTO daily_fil (day, to_address, amount)
    SELECT $1 AS day, UNNEST($2::text[]) AS to_address, UNNEST($3::int[]) AS amount
    ON CONFLICT DO NOTHING
    `, [
    day,
    filStats.map(s => s.to_address),
    filStats.map(s => s.amount)
  ])
}
