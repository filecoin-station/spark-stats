import qs from 'node:querystring'
import assert from 'http-assert'
import { json } from 'http-responders'
import Sentry from '@sentry/node'

/**
 *
 * @param {object} args
 * @param {import('pg').Pool} args.pgPool
 * @param {import('./typings').Logger} args.logger
 * @returns
 */
export const createHandler = ({
  pgPool,
  logger
}) => {
  return (req, res) => {
    const start = new Date()
    logger.request(`${req.method} ${req.url} ...`)
    handler(req, res, pgPool)
      .catch(err => errorHandler(res, err, logger))
      .then(() => {
        logger.request(`${req.method} ${req.url} ${res.statusCode} (${new Date() - start}ms)`)
      })
  }
}

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {import('node:http').ServerResponse} res
 * @param {import('pg').Pool} pgPool
 */
const handler = async (req, res, pgPool) => {
  const [pathname, search] = parseRequestUrl(req.url)
  const segs = pathname.split('/').filter(Boolean)
  if (req.method === 'GET' && segs[0] === 'retrieval-success-rate' && segs.length === 1) {
    await getRetrievalSuccessRate(pathname, search, res, pgPool)
  } else {
    notFound(res)
  }
}

/**
 * @param {string} url
 * @returns {[string, string]} [pathname, search]
 */
const parseRequestUrl = (url) => {
  // Split the url in the format "/path?query" into two parts
  // We need to take into account that query can contain '?' characters
  const ix = url.indexOf('?')
  if (ix < 0) return [url, '']
  return [
    url.slice(0, ix),
    url.slice(ix + 1) // +1 to skip '?'
  ]
}

const errorHandler = (res, err, logger) => {
  if (err instanceof SyntaxError) {
    res.statusCode = 400
    res.end('Invalid JSON Body')
  } else if (err.statusCode) {
    res.statusCode = err.statusCode
    res.end(err.message)
  } else {
    logger.error(err)
    res.statusCode = 500
    res.end('Internal Server Error')
  }

  if (res.statusCode >= 500) {
    Sentry.captureException(err)
  }
}

const notFound = (res) => {
  res.statusCode = 404
  res.end('Not Found')
}

/**
 * @param {string} pathname
 * @param {string} search
 * @param {import('node:http').ServerResponse} res
 * @param {string} querystring
 * @param {import('pg').Pool} pgPool
 */
const getRetrievalSuccessRate = async (pathname, search, res, pgPool) => {
  const filter = parseAndValidateFilter(pathname, search, res)
  if (res.headersSent) return

  setCacheControlForStatsResponse(res, filter)

  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres for converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(
    'SELECT day::text, total, successful FROM retrieval_stats WHERE day >= $1 AND day <= $2',
    [filter.from, filter.to]
  )
  const stats = rows.map(r => ({
    day: r.day,
    success_rate: r.total > 0 ? r.successful / r.total : null
  }))
  json(res, stats)
}

export const today = () => new Date().toISOString().split('T')[0]

/**
 * @param {string} pathname
 * @param {string} search
 * @param {import('node:http').ServerResponse} res
 * @returns {{from: string | undefined; to: string | undefined}}
 */
export const parseAndValidateFilter = (pathname, search, res) => {
  let { from, to } = qs.parse(search)
  let shouldRedirect = false

  // Provide default values for "from" and "to" when not specified

  if (!to) {
    to = today()
    shouldRedirect = true
  }
  if (!from) {
    from = to
    shouldRedirect = true
  }
  if (shouldRedirect) {
    res.setHeader('cache-control', `public, max-age=${600 /* 10min */}`)
    res.setHeader('location', `${pathname}?${qs.stringify({ from, to })}`)
    res.writeHead(302) // Found
    res.end()
    return { from, to }
  }

  // Trim time from date-time values that are typically provided by Grafana

  const matchFrom = from.match(/^(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}:\d{2}\.\d{3}Z)?$/)
  assert(matchFrom, 400, '"from" must have format YYYY-MM-DD or YYYY-MM-DDThh:mm:ss.sssZ')
  if (matchFrom[2]) {
    from = matchFrom[1]
    shouldRedirect = true
  }

  const matchTo = to.match(/^(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}:\d{2}\.\d{3}Z)?$/)
  assert(matchTo, 400, '"to" must have format YYYY-MM-DD or YYYY-MM-DDThh:mm:ss.sssZ')
  if (matchTo[2]) {
    to = matchTo[1]
    shouldRedirect = true
  }

  if (shouldRedirect) {
    res.setHeader('cache-control', `public, max-age=${24 * 3600 /* one day */}`)
    res.setHeader('location', `${pathname}?${qs.stringify({ from, to })}`)
    res.writeHead(301) // Found
    res.end()
    return { from, to }
  }

  // We have well-formed from & to dates now
  return { from, to }
}

/**
 * @param {import('node:http').ServerResponse} res
 * @param {{from: string, to: string}} filter
 */
const setCacheControlForStatsResponse = (res, filter) => {
  // We cannot simply compare filter.to vs today() because there may be a delay in finalizing
  // stats for the previous day. Let's allow up to one hour for the finalization.
  const boundary = new Date(Date.now() - 3600_000).toISOString().split('T')[0]

  if (filter.to >= boundary) {
    // response includes partial data for today, cache it for 10 minutes only
    res.setHeader('cache-control', 'public, max-age=600')
  } else {
    // historical data should never change, cache it for one year
    res.setHeader('cache-control', `public, max-age=${365 * 24 * 3600}, immutable`)
  }
}
