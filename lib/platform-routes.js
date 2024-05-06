import { getStatsWithFilterAndCaching } from './request-helpers.js'
import { fetchDailyStationCount, fetchMonthlyStationCount } from './platform-stats-fetchers.js'

export const handlePlatformRoutes = async (req, res, pgPool) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)
  if (req.method === 'GET' && segs[0] === 'stations' && segs[1] === 'daily' && segs.length === 2) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchDailyStationCount)
    return true
  } else if (req.method === 'GET' && segs[0] === 'stations' && segs[1] === 'monthly' && segs.length === 2) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchMonthlyStationCount)
    return true
  } else if (req.method === 'GET' && segs.length === 0) {
    // health check - required by Grafana datasources
    res.end('OK')
    return true
  }

  return false
}
