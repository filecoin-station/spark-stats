import { getStatsWithFilterAndCaching } from './request-helpers.js'
import {
  fetchDailyStationCount,
  fetchMonthlyStationCount,
  fetchDailyFilSent,
  fetchDailyStationAcceptedMeasurementCount
} from './platform-stats-fetchers.js'

export const handlePlatformRoutes = async (req, res, pgPool) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)

  const fetchFunctionMap = {
    'stations/daily': fetchDailyStationCount,
    'stations/monthly': fetchMonthlyStationCount,
    'measurements/daily': fetchDailyStationAcceptedMeasurementCount,
    'fil/daily': fetchDailyFilSent
  }

  const fetchStatsFn = fetchFunctionMap[segs.join('/')]
  if (req.method === 'GET' && fetchStatsFn) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchStatsFn
    )
    return true
  } else if (req.method === 'GET' && segs.length === 0) {
    // health check - required by Grafana datasources
    res.end('OK')
    return true
  }

  return false
}
