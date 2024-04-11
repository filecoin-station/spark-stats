/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchDailyNodeMetrics = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT metric_date::TEXT, station_id
    FROM daily_node_metrics
    WHERE metric_date >= $1 AND metric_date <= $2
    GROUP BY metric_date, station_id
    `, [
    filter.from,
    filter.to
  ])
  return rows
}
