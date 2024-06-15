import { getStatsWithFilterAndCaching } from './request-helpers.js'
import {
  fetchDailyStationCount,
  fetchMonthlyStationCount,
  fetchDailyRewardTransfers,
  fetchDailyStationAcceptedMeasurementCount
} from './platform-stats-fetchers.js'
import { sanitizePathname } from './handler.js'

export const handlePlatformRoutes = async (req, res, pgPools) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  let { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  pathname = sanitizePathname(pathname)

  const routeHandlerInfoMap = {
    '/stations/daily': {
      fetchFunction: fetchDailyStationCount,
      pgPool: pgPools.evaluate
    },
    '/stations/monthly': {
      fetchFunction: fetchMonthlyStationCount,
      pgPool: pgPools.evaluate
    },
    '/measurements/daily': {
      fetchFunction: fetchDailyStationAcceptedMeasurementCount,
      pgPool: pgPools.evaluate
    },
    '/transfers/daily': {
      fetchFunction: fetchDailyRewardTransfers,
      pgPool: pgPools.stats
    }
  }

  const routeHandlerInfo = routeHandlerInfoMap[pathname]
  if (req.method === 'GET' && routeHandlerInfo) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      routeHandlerInfo.pgPool,
      routeHandlerInfo.fetchFunction
    )
    return true
  } else if (req.method === 'GET' && pathname === '/') {
    // health check - required by Grafana datasources
    res.end('OK')
    return true
  }

  return false
}
