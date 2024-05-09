import { getDailyDistinctCountQuery, getMonthlyDistinctCountQuery } from './request-helpers.js'

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchDailyStationCount = async (pgPool, filter) => {
  return await getDailyDistinctCountQuery({
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
  return await getMonthlyDistinctCountQuery({
    pgPool,
    table: 'daily_stations',
    column: 'station_id',
    filter
  })
}
