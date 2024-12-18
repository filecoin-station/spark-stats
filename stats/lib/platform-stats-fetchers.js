import assert from 'http-assert'
import { today, yesterday } from './request-helpers.js'

/** @typedef {import('@filecoin-station/spark-stats-db').Queryable} Queryable */

const ONE_DAY = 24 * 60 * 60 * 1000

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyStationCount = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, station_count
    FROM daily_platform_stats
    WHERE day >= $1 AND day <= $2
    ORDER BY day
  `, [filter.from, filter.to])
  return rows
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchMonthlyStationCount = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT month::TEXT, station_count
    FROM monthly_active_station_count
    WHERE
      month >= date_trunc('month', $1::DATE)
      AND month < date_trunc('month', $2::DATE) + INTERVAL '1 month'
    ORDER BY month
  `, [filter.from, filter.to])
  return rows
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyStationMeasurementCounts = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, accepted_measurement_count, total_measurement_count
    FROM daily_platform_stats
    WHERE day >= $1 AND day <= $2
    ORDER BY day
  `, [filter.from, filter.to])
  return rows
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchParticipantsWithTopMeasurements = async (pgPool, filter) => {
  assert(filter.to === filter.from, 400, 'Multi-day queries are not supported for this endpoint')
  assert(filter.to === yesterday(), 400, 'filter.to must be set to yesterday, other values are not supported yet')
  // Ignore the filter for this query
  // Get the top measurement stations from the Materialized View
  return (await pgPool.query(`
    SELECT day::TEXT, participant_address, station_count, accepted_measurement_count, inet_group_count
    FROM top_measurement_participants_yesterday_mv
  `)).rows
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyRewardTransfers = async (pgPool, filter) => {
  assert(
    new Date(filter.to).getTime() - new Date(filter.from).getTime() <= 31 * ONE_DAY,
    400,
    'Date range must be 31 days max'
  )
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, to_address, amount
    FROM daily_reward_transfers
    WHERE day >= $1 AND day <= $2
  `, [filter.from, filter.to])
  const days = {}
  for (const row of rows) {
    if (!days[row.day]) {
      days[row.day] = {
        day: row.day,
        amount: '0',
        transfers: []
      }
    }
    const day = days[row.day]
    day.amount = String(BigInt(day.amount) + BigInt(row.amount))
    day.transfers.push({
      toAddress: row.to_address,
      amount: row.amount
    })
  }
  return Object.values(days)
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchAccumulativeDailyParticipantCount = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    WITH first_appearance AS (
      SELECT participant_id, MIN(day) as day
      FROM daily_participants
      GROUP BY participant_id
    ),
    cumulative_participants AS (
      SELECT
        day,
        COUNT(participant_id) OVER (ORDER BY day) AS cumulative_participants
      FROM first_appearance
    )
    SELECT
        DISTINCT(day::TEXT),
        cumulative_participants::INT as participants
    FROM cumulative_participants
    WHERE day >= $1 AND day <= $2
    ORDER BY day
  `, [filter.from, filter.to])
  return rows
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchTopEarningParticipants = async (pgPool, filter) => {
  // The query combines "transfers until filter.to" with "latest scheduled rewards as of today".
  // As a result, it produces incorrect result if `to` is different from `now()`.
  // See https://github.com/filecoin-station/spark-stats/pull/170#discussion_r1664080395
  assert(filter.to === today(), 400, 'filter.to must be today, other values are not supported')
  const { rows } = await pgPool.query(`
    WITH latest_scheduled_rewards AS (
      SELECT DISTINCT ON (participant_address) participant_address, scheduled_rewards
      FROM daily_scheduled_rewards
      ORDER BY participant_address, day DESC
    )
    SELECT
      COALESCE(drt.to_address, lsr.participant_address) as participant_address,
      COALESCE(SUM(drt.amount), 0) + COALESCE(lsr.scheduled_rewards, 0) as total_rewards
    FROM daily_reward_transfers drt
    FULL OUTER JOIN latest_scheduled_rewards lsr
      ON drt.to_address = lsr.participant_address
    WHERE (drt.day >= $1 AND drt.day <= $2) OR drt.day IS NULL
    GROUP BY COALESCE(drt.to_address, lsr.participant_address), lsr.scheduled_rewards
    ORDER BY total_rewards DESC
  `, [filter.from, filter.to])
  return rows
}

/**
 * @param {Queryable} pgPool
 */
export const fetchParticipantsSummary = async (pgPool) => {
  const { rows } = await pgPool.query(`
    SELECT COUNT(DISTINCT participant_id) FROM daily_participants
  `)
  return {
    participant_count: Number(rows[0].count)
  }
}
