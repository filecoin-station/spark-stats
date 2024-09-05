import http from 'node:http'
import { once } from 'node:events'
import assert from 'node:assert'
import createDebug from 'debug'
import { getPgPools } from '@filecoin-station/spark-stats-db'

import { assertResponseStatus, getPort } from './test-helpers.js'
import { createHandler } from '../lib/handler.js'
import { getDayAsISOString, todayDate, today, yesterday } from '../lib/request-helpers.js'

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
      SPARK_API_BASE_URL: 'https://api.filspark.com/',
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
    await pgPools.evaluate.query('DELETE FROM recent_station_details')
    await pgPools.evaluate.query('DELETE FROM recent_participant_subnets')
    await pgPools.evaluate.query('DELETE FROM daily_participants')
    await pgPools.evaluate.query('DELETE FROM participants')
    await pgPools.evaluate.query('DELETE FROM monthly_active_station_count')
    await pgPools.evaluate.query('DELETE FROM daily_measurements_summary')

    await pgPools.evaluate.query('REFRESH MATERIALIZED VIEW top_measurement_participants_yesterday_mv')

    await pgPools.stats.query('DELETE FROM daily_reward_transfers')
    await pgPools.stats.query('DELETE FROM daily_scheduled_rewards')
  })

  describe('GET /stations/daily', () => {
    it('returns daily station metrics for the given date range', async () => {
      await givenDailyMeasurementsSummary(pgPools.evaluate, [
        { day: '2024-01-10', station_count: 3 },
        { day: '2024-01-11', station_count: 1 },
        { day: '2024-01-12', station_count: 2 },
        { day: '2024-01-13', station_count: 4 }
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
        { day: '2024-01-11', station_count: 1 },
        { day: '2024-01-12', station_count: 2 }
      ])
    })
  })

  describe('GET /stations/monthly', () => {
    it('returns monthly station metrics for the given date range', async () => {
      // before the date range
      await givenMonthlyActiveStationCount(pgPools.evaluate, '2023-12-01', 10)
      // in the date range
      await givenMonthlyActiveStationCount(pgPools.evaluate, '2024-01-01', 3)
      await givenMonthlyActiveStationCount(pgPools.evaluate, '2024-02-01', 1)
      // after the date range
      await givenMonthlyActiveStationCount(pgPools.evaluate, '2024-03-01', 5)

      const res = await fetch(
        new URL(
          '/stations/monthly?from=2024-01-01&to=2024-02-11',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const metrics = await res.json()
      assert.deepStrictEqual(metrics, [
        { month: '2024-01-01', station_count: 3 },
        { month: '2024-02-01', station_count: 1 }
      ])
    })
  })

  describe('GET /measurements/daily', () => {
    it('returns daily total accepted measurement count for the given date range', async () => {
      await givenDailyMeasurementsSummary(pgPools.evaluate, [
        { day: '2024-01-10', accepted_measurement_count: 5, total_measurement_count: 6 },
        { day: '2024-01-11', accepted_measurement_count: 1, total_measurement_count: 2 },
        { day: '2024-01-12', accepted_measurement_count: 3, total_measurement_count: 4 },
        { day: '2024-01-13', accepted_measurement_count: 7, total_measurement_count: 8 }
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
        { day: '2024-01-11', accepted_measurement_count: 1, total_measurement_count: 2 },
        { day: '2024-01-12', accepted_measurement_count: 3, total_measurement_count: 4 }
      ])
    })
  })

  describe('GET /participants/top-measurements', () => {
    it('returns top measurement stations for the given date', async () => {
      const day = yesterday()

      await pgPools.evaluate.query(`
        INSERT INTO participants (id, participant_address) VALUES
          (1, 'f1abcdef'),
          (2, 'f1ghijkl'),
          (3, 'f1mnopqr')
      `)

      await pgPools.evaluate.query(`
        INSERT INTO recent_station_details (day, participant_id, station_id, accepted_measurement_count, total_measurement_count) VALUES
          ($1, 1, 'station1', 20, 25),
          ($1, 1, 'station2', 20, 25),
          ($1, 1, 'station3', 10, 15),
          ($1, 2, 'station4', 50, 55),
          ($1, 2, 'station5', 40, 45),
          ($1, 3, 'station6', 10, 15)
      `, [day])

      await pgPools.evaluate.query(`
        INSERT INTO recent_participant_subnets (day, participant_id, subnet) VALUES
          ($1, 1, 'subnet1'),
          ($1, 1, 'subnet2'),
          ($1, 1, 'subnet3'),
          ($1, 2, 'subnet4'),
          ($1, 2, 'subnet5'),
          ($1, 3, 'subnet6')
      `, [day])

      // Refresh the materialized view
      await pgPools.evaluate.query('REFRESH MATERIALIZED VIEW top_measurement_participants_yesterday_mv')

      const res = await fetch(
        new URL(
          '/participants/top-measurements?from=yesterday&to=yesterday',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const metrics = await res.json()
      assert.deepStrictEqual(metrics, [{
        day,
        participant_address: 'f1ghijkl',
        inet_group_count: '2',
        station_count: '2',
        accepted_measurement_count: '90'
      },
      {
        day,
        participant_address: 'f1abcdef',
        inet_group_count: '3',
        station_count: '3',
        accepted_measurement_count: '50'
      },
      {
        day,
        participant_address: 'f1mnopqr',
        inet_group_count: '1',
        station_count: '1',
        accepted_measurement_count: '10'
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
    const oneWeekAgo = getDayAsISOString(new Date(todayDate().getTime() - 7 * 24 * 60 * 60 * 1000))

    const setupScheduledRewardsData = async () => {
      await pgPools.stats.query(`
        INSERT INTO daily_scheduled_rewards (day, participant_address, scheduled_rewards)
        VALUES
          ('${yesterday()}', 'address1', 10),
          ('${yesterday()}', 'address2', 20),
          ('${yesterday()}', 'address3', 30),
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
          `/participants/top-earning?from=${oneWeekAgo}&to=yesterday`,
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 400)
    })
  })
})

const givenDailyMeasurementsSummary = async (pgPoolEvaluate, summaryData = []) => {
  const defaultValues = {
    accepted_measurement_count: 100,
    total_measurement_count: 120,
    station_count: 10,
    participant_address_count: 5,
    inet_group_count: 8
  }

  const processedData = summaryData.map(row => ({
    day: row.day,
    accepted_measurement_count: row.accepted_measurement_count ?? defaultValues.accepted_measurement_count,
    total_measurement_count: row.total_measurement_count ?? defaultValues.total_measurement_count,
    station_count: row.station_count ?? defaultValues.station_count,
    participant_address_count: row.participant_address_count ?? defaultValues.participant_address_count,
    inet_group_count: row.inet_group_count ?? defaultValues.inet_group_count
  }))

  await pgPoolEvaluate.query(`
    INSERT INTO daily_measurements_summary (
      day,
      accepted_measurement_count,
      total_measurement_count,
      station_count,
      participant_address_count,
      inet_group_count
    )
    SELECT
      UNNEST($1::date[]) AS day,
      UNNEST($2::int[]) AS accepted_measurement_count,
      UNNEST($3::int[]) AS total_measurement_count,
      UNNEST($4::int[]) AS station_count,
      UNNEST($5::int[]) AS participant_address_count,
      UNNEST($6::int[]) AS inet_group_count
    ON CONFLICT DO NOTHING
    `, [
    processedData.map(s => s.day),
    processedData.map(s => s.accepted_measurement_count),
    processedData.map(s => s.total_measurement_count),
    processedData.map(s => s.station_count),
    processedData.map(s => s.participant_address_count),
    processedData.map(s => s.inet_group_count)
  ])
}

const givenMonthlyActiveStationCount = async (pgPoolEvaluate, month, stationCount) => {
  await pgPoolEvaluate.query(`
    INSERT INTO monthly_active_station_count (month, station_count)
    VALUES ($1, $2)
    ON CONFLICT DO NOTHING
    `, [
    month,
    stationCount
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
