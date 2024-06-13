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

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {import('node:http').ServerResponse} res
 * @param {import('@filecoin-station/spark-stats-db').PgPools} pgPools
 */
const handler = async (req, res, pgPools) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)

  enableCors(req, res)

  const fetchFunctionMap = {
    'deals/daily': fetchDailyDealStats,
    'retrieval-success-rate': fetchRetrievalSuccessRate,
    'participants/daily': fetchDailyParticipants,
    'participants/monthly': fetchMonthlyParticipants,
    'participants/change-rates': fetchParticipantChangeRates,
    'participants/scheduled-rewards/daily': fetchParticipantScheduledRewards,
    'miners/retrieval-success-rate/summary': fetchMinersRSRSummary
  }

  const fetchStatsFn = fetchFunctionMap[segs.join('/')]
  if (req.method === 'GET' && fetchStatsFn) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPools,
      fetchStatsFn
    )
  } else if (await handlePlatformRoutes(req, res, pgPools)) {
    // no-op, request was handled by handlePlatformRoute
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
