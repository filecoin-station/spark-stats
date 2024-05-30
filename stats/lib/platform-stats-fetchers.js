import { getDailyDistinctCount, getMonthlyDistinctCount } from './request-helpers.js'

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
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
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
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
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
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

export const fetchDailyFilSent = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, SUM(amount) as amount
    FROM daily_reward_transfers
    WHERE day >= $1 AND day <= $2
    GROUP BY day
    ORDER BY day
  `, [filter.from, filter.to])
  return rows
}
