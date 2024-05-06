import { getDailyDistinctCountQuery, getMonthlyDistinctCountQuery } from './request-helpers.js'

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchDailyStationCount = async (pgPool, filter) => {
  const { rows } = await pgPool.query(
    getDailyDistinctCountQuery('daily_stations', 'station_id'),
    [filter.from, filter.to]
  )
  return rows
}

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchMonthlyStationCount = async (pgPool, filter) => {
  const { rows } = await pgPool.query(
    getMonthlyDistinctCountQuery('daily_stations', 'station_id'),
    [filter.from, filter.to]
  )
  return rows
}
