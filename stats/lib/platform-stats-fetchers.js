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
