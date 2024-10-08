import * as Sentry from '@sentry/node'
import { redirect } from 'http-responders'

import { getStatsWithFilterAndCaching } from './request-helpers.js'

import {
  fetchDailyDealStats,
  fetchDailyParticipants,
  fetchMinersRSRSummary,
  fetchMonthlyParticipants,
  fetchParticipantChangeRates,
  fetchParticipantsFirstSeen,
  fetchParticipantScheduledRewards,
  fetchParticipantRewardTransfers,
  fetchRetrievalSuccessRate,
  fetchDealSummary
} from './stats-fetchers.js'

import { handlePlatformRoutes } from './platform-routes.js'

/** @typedef {import('@filecoin-station/spark-stats-db').PgPools} PgPools */
/** @typedef {import('./typings.js').DateRangeFilter} DateRangeFilter */

/**
 * @param {object} args
 * @param {string} args.SPARK_API_BASE_URL
 * @param {import('@filecoin-station/spark-stats-db').PgPools} args.pgPools
 * @param {import('./typings.d.ts').Logger} args.logger
 * @returns
 */
export const createHandler = ({
  SPARK_API_BASE_URL,
  pgPools,
  logger
}) => {
  return (req, res) => {
    const start = Date.now()
    logger.request(`${req.method} ${req.url} ...`)
    handler(req, res, pgPools, SPARK_API_BASE_URL)
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

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {import('node:http').ServerResponse} res
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 * @param {string} SPARK_API_BASE_URL
 */
const handler = async (req, res, pgPools, SPARK_API_BASE_URL) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)
  const url = `/${segs.join('/')}`

  enableCors(req, res)
  const respond = createRespondWithFetchFn(url, searchParams, res, pgPools)

  if (req.method === 'GET' && url === '/deals/daily') {
    await respond(fetchDailyDealStats)
  } else if (req.method === 'GET' && url === '/deals/summary') {
    await respond(fetchDealSummary)
  } else if (req.method === 'GET' && url === '/retrieval-success-rate') {
    await respond(fetchRetrievalSuccessRate)
  } else if (req.method === 'GET' && url === '/participants/daily') {
    await respond(fetchDailyParticipants)
  } else if (req.method === 'GET' && url === '/participants/monthly') {
    await respond(fetchMonthlyParticipants)
  } else if (req.method === 'GET' && url === '/participants/change-rates') {
    await respond(fetchParticipantChangeRates)
  } else if (req.method === 'GET' && url === '/participants/first-seen') {
    await respond(fetchParticipantsFirstSeen)
  } else if (req.method === 'GET' && segs[0] === 'participant' && segs[1] && segs[2] === 'scheduled-rewards') {
    await respond(fetchParticipantScheduledRewards, segs[1])
  } else if (req.method === 'GET' && segs[0] === 'participant' && segs[1] && segs[2] === 'reward-transfers') {
    await respond(fetchParticipantRewardTransfers, segs[1])
  } else if (req.method === 'GET' && url === '/miners/retrieval-success-rate/summary') {
    await respond(fetchMinersRSRSummary)
  } else if (req.method === 'GET' && segs[0] === 'miner' && segs[1] && segs[2] === 'deals' && segs[3] === 'eligible' && segs[4] === 'summary') {
    redirectToSparkApi(req, res, SPARK_API_BASE_URL)
  } else if (req.method === 'GET' && segs[0] === 'client' && segs[1] && segs[2] === 'deals' && segs[3] === 'eligible' && segs[4] === 'summary') {
    redirectToSparkApi(req, res, SPARK_API_BASE_URL)
  } else if (req.method === 'GET' && segs[0] === 'allocator' && segs[1] && segs[2] === 'deals' && segs[3] === 'eligible' && segs[4] === 'summary') {
    redirectToSparkApi(req, res, SPARK_API_BASE_URL)
  } else if (await handlePlatformRoutes(req, res, pgPools)) {
    // no-op, request was handled by handlePlatformRoute
  } else if (req.method === 'GET' && url === '/') {
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

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {import('node:http').ServerResponse} res
 * @param {string} SPARK_API_BASE_URL
 */
const redirectToSparkApi = (req, res, SPARK_API_BASE_URL) => {
  // Cache the response for 6 hours
  res.setHeader('cache-control', `max-age=${6 * 3600}`)

  const location = new URL(req.url, SPARK_API_BASE_URL).toString()
  redirect(req, res, location, 302)
}
