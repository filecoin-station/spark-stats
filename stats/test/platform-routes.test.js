import http from 'node:http'
import { once } from 'node:events'
import assert from 'node:assert'
import createDebug from 'debug'
import { getPgPools } from '@filecoin-station/spark-stats-db'

import { assertResponseStatus, getPort } from './test-helpers.js'
import { createHandler } from '../lib/handler.js'
import { getDayAsISOString, today } from '../lib/request-helpers.js'

const STATION_STATS = { stationId: 'station1', participantAddress: 'f1abcdef', inetGroup: 'group1' }

const debug = createDebug('test')

describe('Platform Routes HTTP request handler', () => {
  /** @type {import('@filecoin-station/spark-stats-db').PgPools} */
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
    baseUrl = `http://127.0.0.1:${getPort(server)}`
  })

  after(async () => {
    server.closeAllConnections()
    server.close()
    await pgPools.end()
  })

  beforeEach(async () => {
    await pgPools.evaluate.query('DELETE FROM daily_stations')
    await pgPools.evaluate.query('REFRESH MATERIALIZED VIEW top_measurement_participants_yesterday_mv')

    await pgPools.stats.query('DELETE FROM daily_reward_transfers')
    await pgPools.stats.query('DELETE FROM daily_scheduled_rewards')
  })

  describe('GET /stations/daily', () => {
    it('returns daily station metrics for the given date range', async () => {
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-10', [
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-11', [
        { ...STATION_STATS, stationId: 'station2', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-12', [
        { ...STATION_STATS, stationId: 'station2', acceptedMeasurementCount: 2 },
        { ...STATION_STATS, stationId: 'station3', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-13', [
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
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
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
      ])
      // in the date range
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-10', [
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-11', [
        { ...STATION_STATS, stationId: 'station2', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-12', [
        { ...STATION_STATS, stationId: 'station2', acceptedMeasurementCount: 2 },
        { ...STATION_STATS, stationId: 'station3', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-02-13', [
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
      ])
      // after the date range
      await givenDailyStationMetrics(pgPools.evaluate, '2024-03-01', [
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
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
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-11', [
        { ...STATION_STATS, stationId: 'station2', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-12', [
        { ...STATION_STATS, stationId: 'station2', acceptedMeasurementCount: 2 },
        { ...STATION_STATS, stationId: 'station3', acceptedMeasurementCount: 1 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, '2024-01-13', [
        { ...STATION_STATS, acceptedMeasurementCount: 1 }
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

  describe('GET /participants/top-measurements', () => {
    it('returns top measurement stations for the given date', async () => {
      const today = new Date()
      const yesterday = new Date()
      yesterday.setDate(today.getDate() - 1)

      const todayUTC = today.toISOString().split('T')[0]
      const yesterdayUTC = yesterday.toISOString().split('T')[0]

      await givenDailyStationMetrics(pgPools.evaluate, yesterdayUTC, [
        { ...STATION_STATS, stationId: 's3', participantAddress: 'f1ghijkl', acceptedMeasurementCount: 50 },
        { ...STATION_STATS, acceptedMeasurementCount: 20 },
        { ...STATION_STATS, stationId: 's2', acceptedMeasurementCount: 30 },
        { ...STATION_STATS, stationId: 's2', inetGroup: 'group2', acceptedMeasurementCount: 40 }
      ])
      await givenDailyStationMetrics(pgPools.evaluate, todayUTC, [
        { ...STATION_STATS, acceptedMeasurementCount: 10 }
      ])

      await pgPools.evaluate.query('REFRESH MATERIALIZED VIEW top_measurement_participants_yesterday_mv')

      const res = await fetch(
        new URL(
          `/participants/top-measurements?from=${yesterdayUTC}&to=${yesterdayUTC}`,
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const metrics = await res.json()
      assert.deepStrictEqual(metrics, [{
        participant_address: STATION_STATS.participantAddress,
        inet_group_count: '2',
        station_count: '2',
        accepted_measurement_count: '90'
      },
      {
        participant_address: 'f1ghijkl',
        inet_group_count: '1',
        station_count: '1',
        accepted_measurement_count: '50'
      }])
    })

    it('returns 400 if the date range is more than one day', async () => {
      const res = await fetch(
        new URL(
          '/participants/top-measurements?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 400)
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

  describe('GET /participants/top-earning', () => {
    const yesterdayDate = new Date()
    yesterdayDate.setDate(yesterdayDate.getDate() - 1)
    const yesterday = getDayAsISOString(yesterdayDate)
    console.log('yesterday', yesterday)

    const oneWeekAgoDate = new Date()
    oneWeekAgoDate.setDate(oneWeekAgoDate.getDate() - 7)
    const oneWeekAgo = getDayAsISOString(oneWeekAgoDate)
    console.log('oneWeekAgo', oneWeekAgo)

    const setupScheduledRewardsData = async () => {
      await pgPools.stats.query(`
        INSERT INTO daily_scheduled_rewards (day, participant_address, scheduled_rewards)
        VALUES 
          ('${yesterday}', 'address1', 10),
          ('${yesterday}', 'address2', 20),
          ('${yesterday}', 'address3', 30),
          ('${today()}', 'address1', 15),
          ('${today()}', 'address2', 25),
          ('${today()}', 'address3', 35)
      `)
    }
    it('returns top earning participants for the given date range', async () => {
      // First two dates should be ignored
      await givenDailyRewardTransferMetrics(pgPools.stats, '2024-01-09', [
        { toAddress: 'address1', amount: 100, lastCheckedBlock: 1 },
        { toAddress: 'address2', amount: 100, lastCheckedBlock: 1 },
        { toAddress: 'address3', amount: 100, lastCheckedBlock: 1 }
      ])
      await givenDailyRewardTransferMetrics(pgPools.stats, '2024-01-10', [
        { toAddress: 'address1', amount: 100, lastCheckedBlock: 1 }
      ])

      // These should be included in the results
      await givenDailyRewardTransferMetrics(pgPools.stats, oneWeekAgo, [
        { toAddress: 'address2', amount: 150, lastCheckedBlock: 1 },
        { toAddress: 'address1', amount: 50, lastCheckedBlock: 1 }
      ])
      await givenDailyRewardTransferMetrics(pgPools.stats, today(), [
        { toAddress: 'address3', amount: 200, lastCheckedBlock: 1 },
        { toAddress: 'address2', amount: 100, lastCheckedBlock: 1 }
      ])

      // Set up scheduled rewards data
      await setupScheduledRewardsData()

      const res = await fetch(
        new URL(
          `/participants/top-earning?from=${oneWeekAgo}&to=${today()}`,
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const topEarners = await res.json()
      assert.deepStrictEqual(topEarners, [
        { participant_address: 'address2', total_rewards: '275' },
        { participant_address: 'address3', total_rewards: '235' },
        { participant_address: 'address1', total_rewards: '65' }
      ])
    })
    it('returns top earning participants for the given date range with no existing reward transfers', async () => {
      await setupScheduledRewardsData()

      await givenDailyRewardTransferMetrics(pgPools.stats, today(), [
        { toAddress: 'address1', amount: 100, lastCheckedBlock: 1 }
      ])

      const res = await fetch(
        new URL(
          `/participants/top-earning?from=${oneWeekAgo}&to=${today()}`,
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const topEarners = await res.json()
      assert.deepStrictEqual(topEarners, [
        { participant_address: 'address1', total_rewards: '115' },
        { participant_address: 'address3', total_rewards: '35' },
        { participant_address: 'address2', total_rewards: '25' }
      ])
    })
    it('returns 400 if the date range end is not today', async () => {
      const res = await fetch(
        new URL(
          `/participants/top-earning?from=${oneWeekAgo}&to=${yesterday}`,
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 400)
    })
  })
})

const givenDailyStationMetrics = async (pgPoolEvaluate, day, stationStats) => {
  await pgPoolEvaluate.query(`
    INSERT INTO daily_stations (
      day,
      station_id,
      participant_address,
      inet_group,
      accepted_measurement_count
    )
    SELECT 
      $1 AS day,
      UNNEST($2::text[]) AS station_id,
      UNNEST($3::text[]) AS participant_address,
      UNNEST($4::text[]) AS inet_group,
      UNNEST($5::int[]) AS accepted_measurement_count
    ON CONFLICT DO NOTHING
    `, [
    day,
    stationStats.map(s => s.stationId),
    stationStats.map(s => s.participantAddress),
    stationStats.map(s => s.inetGroup),
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
