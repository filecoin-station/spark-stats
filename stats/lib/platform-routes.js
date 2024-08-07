import { getStatsWithFilterAndCaching } from './request-helpers.js'
import {
  fetchDailyStationCount,
  fetchMonthlyStationCount,
  fetchDailyRewardTransfers,
  fetchTopEarningParticipants,
  fetchParticipantsWithTopMeasurements,
  fetchDailyStationMeasurementCounts
} from './platform-stats-fetchers.js'

const createRespondWithFetchFn = (pathname, searchParams, res) => (pgPool, fetchFn) => {
  return getStatsWithFilterAndCaching(
    pathname,
    {},
    searchParams,
    res,
    pgPool,
    fetchFn
  )
}

export const handlePlatformRoutes = async (req, res, pgPools) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)
  const url = `/${segs.join('/')}`
  const respond = createRespondWithFetchFn(pathname, searchParams, res)

  if (req.method === 'GET' && url === '/stations/daily') {
    await respond(pgPools.evaluate, fetchDailyStationCount)
  } else if (req.method === 'GET' && url === '/stations/monthly') {
    await respond(pgPools.evaluate, fetchMonthlyStationCount)
  } else if (req.method === 'GET' && url === '/measurements/daily') {
    await respond(pgPools.evaluate, fetchDailyStationMeasurementCounts)
  } else if (req.method === 'GET' && url === '/participants/top-measurements') {
    await respond(pgPools.evaluate, fetchParticipantsWithTopMeasurements)
  } else if (req.method === 'GET' && url === '/participants/top-earning') {
    await respond(pgPools.stats, fetchTopEarningParticipants)
  } else if (req.method === 'GET' && url === '/transfers/daily') {
    await respond(pgPools.stats, fetchDailyRewardTransfers)
  } else {
    return false
  }
  return true
}
