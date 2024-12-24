/** @typedef {import('@filecoin-station/spark-stats-db').PgPools} PgPools */
/**
 * @param {PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter & {nonZero?: 'true'}} filter
 */
export const fetchRetrievalSuccessRate = async (pgPools, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres for converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPools.evaluate.query(`
    SELECT 
    day::text, 
    SUM(total) as total, 
    SUM(successful) as successful, 
    SUM(successful_http) as successful_http
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
    success_rate: r.total > 0 ? r.successful / r.total : null,
    successful_http: r.successful_http ?? null,
    // successful_http might be null because the column was added later
    success_rate_http: r.total > 0 && r.successful_http !== null ? r.successful_http / r.total : null
  }))
  return stats
}

/**
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyDealStats = async (pgPools, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPools.evaluate.query(`
    SELECT
      day::text,
      SUM(tested) AS tested,
      SUM(index_majority_found) AS "indexMajorityFound",
      SUM(indexed) AS indexed,
      SUM(indexed_http) AS "indexedHttp",
      SUM(retrieval_majority_found) AS "retrievalMajorityFound",
      SUM(retrievable) AS retrievable
    FROM daily_deals
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
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDealSummary = async (pgPools, filter) => {
  const { rows: [summary] } = await pgPools.evaluate.query(`
    SELECT
      SUM(tested) AS tested,
      SUM(index_majority_found) AS "indexMajorityFound",
      SUM(indexed) AS indexed,
      SUM(indexed_http) AS "indexedHttp",
      SUM(retrieval_majority_found) AS "retrievalMajorityFound",
      SUM(retrievable) AS retrievable
    FROM daily_deals
    WHERE day >= date_trunc('day', $1::DATE)
      AND day <= date_trunc('day', $2::DATE)
  `, [filter.from, filter.to]
  )
  return summary
}

export const fetchDailyParticipants = async (pgPools, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPools.evaluate.query(`
    SELECT day::TEXT, COUNT(DISTINCT participant_id)::INT as participants
    FROM daily_participants
    WHERE day >= $1 AND day <= $2
    GROUP BY day
    ORDER BY day
  `, [filter.from, filter.to])
  return rows
}

export const fetchMonthlyParticipants = async (pgPools, filter) => {
  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPools.evaluate.query(`
    SELECT
      date_trunc('month', day)::DATE::TEXT as month,
      COUNT(DISTINCT participant_id)::INT as participants
    FROM daily_participants
    WHERE
      day >= date_trunc('month', $1::DATE)
      AND day < date_trunc('month', $2::DATE) + INTERVAL '1 month'
    GROUP BY month
    ORDER BY month
  `, [filter.from, filter.to]
  )
  return rows
}

/**
 * @param {PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
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

  /** @type {{month: string, growthRate: number, churnRate: number, retentionRate: number}[]} */
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
 * @param {PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 * @param {string} address
 */
export const fetchParticipantScheduledRewards = async (pgPools, { from, to }, address) => {
  const { rows } = await pgPools.stats.query(`
    SELECT day::text, scheduled_rewards
    FROM daily_scheduled_rewards
    WHERE participant_address = $1 AND day >= $2 AND day <= $3
  `, [address, from, to])
  return rows
}

/**
 * @param {PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 * @param {string} address
 */
export const fetchParticipantRewardTransfers = async (pgPools, { from, to }, address) => {
  const { rows } = await pgPools.stats.query(`
    SELECT day::TEXT, amount
    FROM daily_reward_transfers
    WHERE to_address = $1 AND day >= $2 AND day <= $3
  `, [address, from, to])
  return rows
}

/**
 * Fetches the retrieval stats summary for all miners for given date range.
 * @param {PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchMinersRSRSummary = async (pgPools, filter) => {
  const { rows } = await pgPools.evaluate.query(`
    SELECT 
    miner_id, 
    SUM(total) as total, 
    SUM(successful) as successful, 
    SUM(successful_http) as successful_http
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
    success_rate: r.total > 0 ? r.successful / r.total : null,
    successful_http: r.successful_http ?? null,
    // successful_http might be null because the column was added later
    success_rate_http: r.total > 0 && r.successful_http !== null ? r.successful_http / r.total : null
  }))
  return stats
}

/**
 * Fetches the retrieval stats summary for a single miner for given date range.
 * @param {PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 * @param {string} minerId
 */
export const fetchDailyMinerRSRSummary = async (pgPools, { from, to }, minerId) => {
  const { rows } = await pgPools.evaluate.query(`
    SELECT 
    day::TEXT, 
    SUM(total) as total, SUM(successful) as successful, 
    SUM(successful_http) as successful_http
    FROM retrieval_stats
    WHERE miner_id = $1 AND day >= $2 AND day <= $3
    GROUP BY day
    ORDER BY day
   `, [
    minerId,
    from,
    to
  ])
  const stats = rows.map(r => ({
    day: r.day,
    total: r.total,
    successful: r.successful,
    success_rate: r.total > 0 ? r.successful / r.total : null,
    successful_http: r.successful_http ?? null,
    // successful_http might be null because the column was added later
    success_rate_http: r.total > 0 && r.successful_http !== null ? r.successful_http / r.total : null
  }))
  return stats
}

export const fetchDailyRetrievalResultCodes = async (pgPools, filter) => {
  const { rows } = await pgPools.stats.query(`
    SELECT day::TEXT, code, rate
    FROM daily_retrieval_result_codes
    WHERE day >= $1 AND day <= $2
   `, [
    filter.from,
    filter.to
  ])
  const days = {}
  for (const row of rows) {
    if (!days[row.day]) {
      days[row.day] = {}
    }
    days[row.day][row.code] = row.rate
  }
  const stats = Object.entries(days).map(([day, rates]) => ({ day, rates }))
  return stats
}

/**
 * Fetches global median time-to-first-byte
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchTTFBSummary = async (pgPools, filter) => {
  const { rows } = await pgPools.evaluate.query(`
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ttfb_median) AS p50 
    FROM ttfb_retreival_stats
  `)
  return rows
}

/**
 * Fetches daily global median time-to-first-byte
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchDailyTTFBStats = async (pgPools, filter) => {
  const { rows } = await pgPools.evaluate.query(`
    SELECT
      day::text,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY ttfb_median) AS p50
    FROM ttfb_retreival_stats
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
 * Fetches per miner median time-to-first-byte
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 * @param {import('./typings.js').DateRangeFilter} filter
 * @param {string} minerId
 */
export const fetchDailyMinerTTFBStats = async (pgPools, { from, to }, minerId) => {
  const { rows } = await pgPools.evaluate.query(`
    SELECT
      day::text,
      miner_id,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY ttfb_median) AS p50
    FROM ttfb_retreival_stats
    WHERE miner_id = $1 AND day >= $2 AND day <= $3
    GROUP BY day, miner_id
    ORDER BY day
    `, [
    minerId,
    from,
    to
  ])
  return rows
}
