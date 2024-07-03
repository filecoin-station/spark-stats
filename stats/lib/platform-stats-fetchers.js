import assert from 'http-assert'
import { getDailyDistinctCount, getMonthlyDistinctCount, today } from './request-helpers.js'

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

/**
 * @param {Queryable} pgPool
 * @param {import('./typings.js').DateRangeFilter} filter
 */
export const fetchTopEarningParticipants = async (pgPool, filter) => {
  assert(filter.to === today(), 400, 'filter.to must be today, other values are not supported')
  const { rows } = await pgPool.query(`
    WITH latest_scheduled_rewards AS (
      SELECT DISTINCT ON (participant_address) participant_address, scheduled_rewards
      FROM daily_scheduled_rewards
      ORDER BY participant_address, day DESC
    )
    SELECT 
      COALESCE(drt.to_address, lsr.participant_address) as participant_address, 
      COALESCE(SUM(drt.amount), 0) + COALESCE(lsr.scheduled_rewards, 0) as total_rewards
    FROM daily_reward_transfers drt
    FULL OUTER JOIN latest_scheduled_rewards lsr
      ON drt.to_address = lsr.participant_address
    WHERE (drt.day >= $1 AND drt.day <= $2) OR drt.day IS NULL
    GROUP BY COALESCE(drt.to_address, lsr.participant_address), lsr.scheduled_rewards
    ORDER BY total_rewards DESC
  `, [filter.from, filter.to])
  return rows
}
