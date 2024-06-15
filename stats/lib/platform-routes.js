import { getStatsWithFilterAndCaching } from './request-helpers.js'
import {
  fetchDailyStationCount,
  fetchMonthlyStationCount,
  fetchDailyRewardTransfers,
  fetchDailyStationAcceptedMeasurementCount
} from './platform-stats-fetchers.js'
import { sanitizePathname } from './handler.js'

const createRespondWithFetchFn = (pathname, searchParams, res) => (pgPool, fetchFn) => {
  return getStatsWithFilterAndCaching(
    pathname,
    searchParams,
    res,
    pgPool,
    fetchFn
  )
}

export const handlePlatformRoutes = async (req, res, pgPools) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  let { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  pathname = sanitizePathname(pathname)
  const respond = createRespondWithFetchFn(pathname, searchParams, res)

  if (req.method === 'GET' && pathname === '/stations/daily') {
    await respond(pgPools.evaluate, fetchDailyStationCount)
  } else if (req.method === 'GET' && pathname === '/stations/monthly') {
    await respond(pgPools.evaluate, fetchMonthlyStationCount)
  } else if (req.method === 'GET' && pathname === '/measurements/daily') {
    await respond(pgPools.evaluate, fetchDailyStationAcceptedMeasurementCount)
  } else if (req.method === 'GET' && pathname === '/transfers/daily') {
    await respond(pgPools.stats, fetchDailyRewardTransfers)
  } else {
    return false
  }
  return true
}
