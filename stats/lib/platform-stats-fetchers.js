import assert from 'http-assert'
import { getDailyDistinctCount, getMonthlyDistinctCount } from './request-helpers.js'

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
export const fetchDailyStationAcceptedMeasurementCount = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, SUM(accepted_measurement_count) as accepted_measurement_count
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
export const fetchTopMeasurementParticipants = async (pgPool, filter) => {
  assert(filter.to === filter.from, 400, 'Multi-day queries are not supported for this endpoint')
  const yesterdayUTC = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0]
  assert(filter.to === yesterdayUTC, 400, 'filter.to must be set to yesterday, other values are not supported yet')
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
