import * as Sentry from '@sentry/node'

import { getStatsWithFilterAndCaching } from './request-helpers.js'

import {
  fetchDailyDealStats,
  fetchDailyParticipants,
  fetchMinersRSRSummary,
  fetchMonthlyParticipants,
  fetchParticipantChangeRates,
  fetchParticipantScheduledRewards,
  fetchRetrievalSuccessRate
} from './stats-fetchers.js'

import { handlePlatformRoutes } from './platform-routes.js'

/**
 * @param {object} args
 * @param {import('@filecoin-station/spark-stats-db').PgPools} args.pgPools
 * @param {import('./typings.d.ts').Logger} args.logger
 * @returns
 */
export const createHandler = ({
  pgPools,
  logger
}) => {
  return (req, res) => {
    const start = Date.now()
    logger.request(`${req.method} ${req.url} ...`)
    handler(req, res, pgPools)
      .catch(err => errorHandler(res, err, logger))
      .then(() => {
        logger.request(`${req.method} ${req.url} ${res.statusCode} (${Date.now() - start}ms)`)
      })
  }
}

const enableCors = (req, res) => {
  if (req.headers.origin === 'http://localhost:3000') {
    res.setHeader('Access-Control-Allow-Origin', 'http://localhost:3000')
  } else {
    res.setHeader('Access-Control-Allow-Origin', 'app://-')
  }
}

const createRespondWithFetchFn = (pathname, searchParams, res, pgPools) => fetchFn => {
  return getStatsWithFilterAndCaching(
    pathname,
    searchParams,
    res,
    pgPools,
    fetchFn
  )
}

export const sanitizePathname = pathname => `/${pathname.split('/').filter(Boolean).join('/')}`

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {import('node:http').ServerResponse} res
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 */
const handler = async (req, res, pgPools) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  let { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  pathname = sanitizePathname(pathname)

  enableCors(req, res)
  const respond = createRespondWithFetchFn(pathname, searchParams, res, pgPools)

  if (req.method === 'GET' && pathname === '/deals/daily') {
    await respond(fetchDailyDealStats)
  } else if (req.method === 'GET' && pathname === '/retrieval-success-rate') {
    await respond(fetchRetrievalSuccessRate)
  } else if (req.method === 'GET' && pathname === '/participants/daily') {
    await respond(fetchDailyParticipants)
  } else if (req.method === 'GET' && pathname === '/participants/monthly') {
    await respond(fetchMonthlyParticipants)
  } else if (req.method === 'GET' && pathname === '/participants/change-rates') {
    await respond(fetchParticipantChangeRates)
  } else if (req.method === 'GET' && pathname === '/participants/scheduled-rewards/daily') {
    await respond(fetchParticipantScheduledRewards)
  } else if (req.method === 'GET' && pathname === '/miners/retrieval-success-rate/summary') {
    await respond(fetchMinersRSRSummary)
  } else if (await handlePlatformRoutes(req, res, pgPools)) {
    // no-op, request was handled by handlePlatformRoute
  } else if (req.method === 'GET' && pathname === '/') {
    // health check - required by Grafana datasources
    res.end('OK')
  } else {
    notFound(res)
  }
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
