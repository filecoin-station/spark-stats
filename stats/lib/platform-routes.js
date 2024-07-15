import { getStatsWithFilterAndCaching } from './request-helpers.js'
import {
  fetchDailyStationCount,
  fetchMonthlyStationCount,
  fetchDailyRewardTransfers,
  fetchTopEarningParticipants,
  fetchParticipantsWithTopMeasurements,
  fetchDailyStationAcceptedMeasurementCount,
  fetchParticipantRewardTransfers
} from './platform-stats-fetchers.js'

/** @typedef {import('@filecoin-station/spark-stats-db').PgPools} PgPools */
/** @typedef {import('./typings.js').DateRangeFilter} DateRangeFilter */

/**
 * @param {string} pathname
 * @param {URLSearchParams} searchParams
 * @param {import('node:http').ServerResponse} res
 * @param {PgPools} pgPools
 * @returns {(fetchFn: (pgPools: PgPools, filter: DateRangeFilter, pathVariables: object) => Promise<any>, pathParams?: object) => Promise<void>}
 */
const createRespondWithFetchFn =
(pathname, searchParams, res, pgPools) =>
  (fetchFn, pathParams) => {
    return getStatsWithFilterAndCaching(
      pathname,
      pathParams,
      searchParams,
      res,
      pgPools,
      fetchFn
    )
  }

export const handlePlatformRoutes = async (req, res, pgPools) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)
  const url = `/${segs.join('/')}`
  const respond = createRespondWithFetchFn(pathname, searchParams, res, pgPools)

  if (req.method === 'GET' && url === '/stations/daily') {
    await respond(fetchDailyStationCount)
  } else if (req.method === 'GET' && url === '/stations/monthly') {
    await respond(fetchMonthlyStationCount)
  } else if (req.method === 'GET' && url === '/measurements/daily') {
    await respond(fetchDailyStationAcceptedMeasurementCount)
  } else if (req.method === 'GET' && url === '/participants/top-measurements') {
    await respond(fetchParticipantsWithTopMeasurements)
  } else if (req.method === 'GET' && url === '/participants/top-earning') {
    await respond(fetchTopEarningParticipants)
  } else if (req.method === 'GET' && url === '/transfers/daily') {
    await respond(fetchDailyRewardTransfers)
  } else if (req.method === 'GET' && segs[0] === 'participant' && segs[1] && segs[2] === 'reward-transfers') {
    await respond(fetchParticipantRewardTransfers, segs[1])
  } else {
    return false
  }
  return true
}
