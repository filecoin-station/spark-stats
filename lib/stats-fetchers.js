/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchRetrievalSuccessRate = async (pgPool, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres for converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(`
    SELECT day::text, SUM(total) as total, SUM(successful) as successful
    FROM retrieval_stats
    WHERE day >= $1 AND day <= $2
    GROUP BY day
    ORDER BY day
    `, [
    filter.from,
    filter.to
  ])
  const stats = rows.map(r => ({
    day: r.day,
    success_rate: r.total > 0 ? r.successful / r.total : null
  }))
  return stats
}

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchDailyParticipants = async (pgPool, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, COUNT(DISTINCT participant_id)::INT as participants
    FROM daily_participants
    WHERE day >= $1 AND day <= $2
    GROUP BY day
    ORDER BY day
    `, [
    filter.from,
    filter.to
  ])
  return rows
}

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchMonthlyParticipants = async (pgPool, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(`
    SELECT
      date_trunc('month', day)::DATE::TEXT as month,
      COUNT(DISTINCT participant_id)::INT as participants
    FROM daily_participants
    WHERE
      day >= date_trunc('month', $1::DATE)
      AND day < date_trunc('month', $2::DATE) + INTERVAL '1 month'
    GROUP BY month
    ORDER BY month
    `, [
    filter.from,
    filter.to
  ])
  return rows
}

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchParticipantChangeRates = async (pgPool, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(`
    SELECT
      date_trunc('month', day)::DATE::TEXT as month,
      participant_id
    FROM daily_participants
    WHERE
      day >= date_trunc('month', $1::DATE) - INTERVAL '1 month'
      AND day < date_trunc('month', $2::DATE) + INTERVAL '1 month'
    GROUP BY month, participant_id
    ORDER BY month ASC
    `, [
    filter.from,
    filter.to
  ])

  /** @type {string[]} */
  const allMonths = []
  /** @type {Set<number>[]} */
  const monthlyParticipants = []

  // Group participants by months. We rely on the rows being ordered by the months.
  for (const { month, participant_id: participant } of rows) {
    if (allMonths.length === 0 || allMonths[allMonths.length - 1] !== month) {
      allMonths.push(month)
      monthlyParticipants.push(new Set())
    }
    monthlyParticipants[monthlyParticipants.length - 1].add(participant)
  }

  /** @type {{month: string, churnRate: number}[]} */
  const stats = []

  // IMPORTANT: The iteration starts at index one.
  // The month at index zero is the last month before the selected time range.
  for (let ix = 1; ix < allMonths.length; ix++) {
    const thisMonthValue = allMonths[ix]
    const thisMonthParticipants = monthlyParticipants[ix]
    const lastMonthParticipants = monthlyParticipants[ix - 1]

    const initialCount = lastMonthParticipants.size
    const lostCount = Array.from(lastMonthParticipants.values())
      .filter(p => !thisMonthParticipants.has(p))
      .length
    const retainedCount = Array.from(lastMonthParticipants.values())
      .filter(p => thisMonthParticipants.has(p))
      .length
    const acquiredCount = Array.from(thisMonthParticipants.values())
      .filter(p => !lastMonthParticipants.has(p))
      .length

    stats.push({
      month: thisMonthValue,
      churnRate: initialCount > 0 ? lostCount / initialCount : 0,
      growthRate: initialCount > 0 ? acquiredCount / initialCount : 0,
      retentionRate: initialCount > 0 ? retainedCount / initialCount : 0
    })
  }

  return stats
}

/**
 * @param {import('pg').Pool} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchMinersRSRSummary = async (pgPool, filter) => {
  const { rows } = await pgPool.query(`
    SELECT miner_id, SUM(total) as total, SUM(successful) as successful
    FROM retrieval_stats
    WHERE day >= $1 AND day <= $2
    GROUP BY miner_id
   `, [
    filter.from,
    filter.to
  ])
  const stats = rows.map(r => ({
    miner_id: r.miner_id,
    success_rate: r.total > 0 ? r.successful / r.total : null
  }))
  return stats
}
