import { getDailyDistinctCount, getMonthlyDistinctCount } from './request-helpers.js'

/**
 * @param {import('@filecoin-station/spark-stats-db').pgPools} pgPools
 * @param {import('./typings').DateRangeFilter & {nonZero?: 'true'}} filter
 */
export const fetchRetrievalSuccessRate = async (pgPools, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres for converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPools.evaluate.query(`
    SELECT day::text, SUM(total) as total, SUM(successful) as successful
    FROM retrieval_stats
    WHERE day >= $1 AND day <= $2 ${filter.nonZero === 'true' ? 'AND successful > 0' : ''}
    GROUP BY day
    ORDER BY day
    `, [
    filter.from,
    filter.to
  ])
  const stats = rows.map(r => ({
    day: r.day,
    total: r.total,
    successful: r.successful,
    success_rate: r.total > 0 ? r.successful / r.total : null
  }))
  return stats
}

export const fetchDailyParticipants = async (pgPools, filter) => {
  return await getDailyDistinctCount({
    pgPool: pgPools.evaluate,
    table: 'daily_participants',
    column: 'participant_id',
    filter,
    asColumn: 'participants'
  })
}

export const fetchMonthlyParticipants = async (pgPools, filter) => {
  return await getMonthlyDistinctCount({
    pgPool: pgPools.evaluate,
    table: 'daily_participants',
    column: 'participant_id',
    filter,
    asColumn: 'participants'
  })
}

/**
 * @param {import('@filecoin-station/spark-stats-db').pgPools} pgPools
 * @param {import('./typings').DateRangeFilter} filter
 */
export const fetchParticipantChangeRates = async (pgPools, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPools.evaluate.query(`
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
 * @param {import('@filecoin-station/spark-stats-db').pgPools} pgPools
 * @param {import('./typings').DateRangeFilter} filter
 */
export const fetchParticipantScheduledRewards = async (pgPools, filter) => {
  const { rows } = await pgPools.stats.query(`
    SELECT day::text, scheduled_rewards
    FROM daily_scheduled_rewards
    WHERE participant_address = $1 AND day >= $2 AND day <= $3
   `, [
    filter.address,
    filter.from,
    filter.to
  ])
  return rows
}

/**
 * @param {import('@filecoin-station/spark-stats-db').pgPools} pgPool
 * @param {import('./typings').Filter} filter
 */
export const fetchMinersRSRSummary = async (pgPools, filter) => {
  const { rows } = await pgPools.evaluate.query(`
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
    total: r.total,
    successful: r.successful,
    success_rate: r.total > 0 ? r.successful / r.total : null
  }))
  return stats
}
