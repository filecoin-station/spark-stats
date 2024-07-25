import assert from 'http-assert'
import {
  getDailyDistinctCount,
  getMonthlyDistinctCount,
  today,
  yesterday
} from './request-helpers.js'

/** @typedef {import('@filecoin-station/spark-stats-db').Queryable} Queryable */

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyStationCount = async (pgPool, filter) => {
  return await getDailyDistinctCount({
    pgPool,
    table: 'daily_stations',
    column: 'station_id',
    filter
  })
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchMonthlyStationCount = async (pgPool, filter) => {
  return await getMonthlyDistinctCount({
    pgPool,
    table: 'daily_stations',
    column: 'station_id',
    filter
  })
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyStationMeasurementCounts = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT day::TEXT,
      SUM(accepted_measurement_count) as accepted_measurement_count,
      SUM(total_measurement_count) as total_measurement_count
    FROM daily_stations
    WHERE day >= $1 AND day <= $2
    GROUP BY day
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
  return (await pgPool.query('SELECT * FROM top_measurement_participants_yesterday_mv')).rows
}

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyRewardTransfers = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, SUM(amount) as amount
    FROM daily_reward_transfers
    WHERE day >= $1 AND day <= $2
    GROUP BY day
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
