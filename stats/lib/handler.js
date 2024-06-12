import * as Sentry from '@sentry/node'

import { getStatsWithFilterAndCaching } from './request-helpers.js'

import {
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
 * @param {import('@filecoin-station/spark-stats-db')} args.pgPools
 * @param {import('pg').Pool} args.pgPoolStatsDb
 * @param {import('./typings').Logger} args.logger
 * @returns
 */
export const createHandler = ({
  pgPools,
  logger
}) => {
  return (req, res) => {
    const start = new Date()
    logger.request(`${req.method} ${req.url} ...`)
    handler(req, res, pgPools)
      .catch(err => errorHandler(res, err, logger))
      .then(() => {
        logger.request(`${req.method} ${req.url} ${res.statusCode} (${new Date() - start}ms)`)
      })
  }
}

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {import('node:http').ServerResponse} res
 * @param {import('@filecoin-station/spark-stats-db')} args.pgPools
 */
const handler = async (req, res, pgPools) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)

  const fetchFunctionMap = {
    'retrieval-success-rate': fetchRetrievalSuccessRate,
    'participants/daily': fetchDailyParticipants,
    'participants/monthly': fetchMonthlyParticipants,
    'participants/change-rates': fetchParticipantChangeRates,
    'participants/scheduled-rewards': fetchParticipantScheduledRewards,
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
