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
export const fetchTopMeasurementStations = async (pgPool, filter) => {
  // Block expensive multi-day queries
  assert(filter.to === filter.from, 400, 'Multi-day queries are not supported for this endpoint')

  const { rows } = await pgPool.query(`
  SELECT
    participant_address,
    COUNT(DISTINCT inet_group) AS inet_group_count,
    COUNT(DISTINCT station_id) AS station_count,
    SUM(accepted_measurement_count) AS accepted_measurement_count
  FROM daily_stations
  WHERE day >= $1 AND day <= $2
  GROUP BY participant_address
  ORDER BY accepted_measurement_count DESC;
  `, [filter.from, filter.to])
  return rows
}

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
