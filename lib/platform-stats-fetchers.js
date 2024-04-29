/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchDailyStationMetrics = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, station_id
    FROM daily_stations
    WHERE day >= $1 AND day <= $2
    GROUP BY day, station_id
    `, [
    filter.from,
    filter.to
  ])
  return rows
}
