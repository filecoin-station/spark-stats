import { getStatsWithFilterAndCaching } from './request-helpers.js'
import {
  fetchDailyStationCount,
  fetchMonthlyStationCount,
  fetchDailyFilSent,
  fetchDailyStationAcceptedMeasurementCount
} from './platform-stats-fetchers.js'

export const handlePlatformRoutes = async (req, res, pgPoolEvaluateDb, pgPoolStatsDb) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)

  const routeHandlerInfoMap = {
    'stations/daily': {
      fetchFunction: fetchDailyStationCount,
      pgPool: pgPoolEvaluateDb
    },
    'stations/monthly': {
      fetchFunction: fetchMonthlyStationCount,
      pgPool: pgPoolEvaluateDb
    },
    'measurements/daily': {
      fetchFunction: fetchDailyStationAcceptedMeasurementCount,
      pgPool: pgPoolEvaluateDb
    },
    'fil/daily': {
      fetchFunction: fetchDailyFilSent,
      pgPool: pgPoolStatsDb
    }
  }

  const routeHandlerInfo = routeHandlerInfoMap[segs.join('/')]
  if (req.method === 'GET' && routeHandlerInfo) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      routeHandlerInfo.pgPool,
      routeHandlerInfo.fetchFunction
    )
    return true
  } else if (req.method === 'GET' && segs.length === 0) {
    // health check - required by Grafana datasources
    res.end('OK')
    return true
  }

  return false
}
