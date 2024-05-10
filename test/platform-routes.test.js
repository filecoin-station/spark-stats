import http from 'node:http'
import { once } from 'node:events'
import assert from 'node:assert'
import pg from 'pg'
import createDebug from 'debug'

import { assertResponseStatus } from './test-helpers.js'
import { createHandler } from '../lib/handler.js'
import { DATABASE_URL } from '../lib/config.js'

const debug = createDebug('test')

describe('Platform Routes HTTP request handler', () => {
  /** @type {pg.Pool} */
  let pgPool
  /** @type {http.Server} */
  let server
  /** @type {string} */
  let baseUrl

  before(async () => {
    pgPool = new pg.Pool({ connectionString: DATABASE_URL })

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
  })

  describe('GET /stations/daily', () => {
    it('returns daily station metrics for the given date range', async () => {
      await givenDailyStationMetrics(pgPool, '2024-01-10', ['station1'])
      await givenDailyStationMetrics(pgPool, '2024-01-11', ['station2'])
      await givenDailyStationMetrics(pgPool, '2024-01-12', ['station2', 'station3'])
      await givenDailyStationMetrics(pgPool, '2024-01-13', ['station1'])

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
      await givenDailyStationMetrics(pgPool, '2023-12-31', ['station1'])
      // in the date range
      await givenDailyStationMetrics(pgPool, '2024-01-10', ['station1'])
      await givenDailyStationMetrics(pgPool, '2024-01-11', ['station2'])
      await givenDailyStationMetrics(pgPool, '2024-01-12', ['station2', 'station3'])
      await givenDailyStationMetrics(pgPool, '2024-02-13', ['station1'])
      // after the date range
      await givenDailyStationMetrics(pgPool, '2024-03-01', ['station1'])

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
})

const givenDailyStationMetrics = async (pgPool, day, stationIds) => {
  await pgPool.query(`
    INSERT INTO daily_stations (day, station_id)
    SELECT $1 AS day, UNNEST($2::text[]) AS station_id
    ON CONFLICT DO NOTHING
    `, [
    day,
    stationIds
  ])
}
