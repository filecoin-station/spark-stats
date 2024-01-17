/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchRetrievalSuccessRate = async (pgPool, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres for converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(
    'SELECT day::text, total, successful FROM retrieval_stats WHERE day >= $1 AND day <= $2',
    [filter.from, filter.to]
  )
  const stats = rows.map(r => ({
    day: r.day,
    success_rate: r.total > 0 ? r.successful / r.total : null
  }))
  return stats
}
