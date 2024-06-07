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
 * @param {import('pg').Pool} args.pgPoolEvaluateDb
 * @param {import('pg').Pool} args.pgPoolStatsDb
 * @param {import('./typings').Logger} args.logger
 * @returns
 */
export const createHandler = ({
  pgPoolEvaluateDb,
  pgPoolStatsDb,
  logger
}) => {
  return (req, res) => {
    const start = new Date()
    logger.request(`${req.method} ${req.url} ...`)
    handler(req, res, pgPoolEvaluateDb, pgPoolStatsDb)
      .catch(err => errorHandler(res, err, logger))
      .then(() => {
        logger.request(`${req.method} ${req.url} ${res.statusCode} (${new Date() - start}ms)`)
      })
  }
}

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {import('node:http').ServerResponse} res
 * @param {import('pg').Pool} pgPoolEvaluateDb
 * @param {import('pg').Pool} pgPoolStatsDb
 */
const handler = async (req, res, pgPoolEvaluateDb, pgPoolStatsDb) => {
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)

  const fetchFunctionMap = {
    'retrieval-success-rate': fetchRetrievalSuccessRate,
    'participants/daily': fetchDailyParticipants,
    'participants/monthly': fetchMonthlyParticipants,
    'participants/change-rates': fetchParticipantChangeRates,
    'participants/scheduled-rewards': fetchParticipantScheduledRewards,
    'miners/retrieval-success-rate/summary': fetchMinersRSRSummary,
  }

  const fetchStatsFn = fetchFunctionMap[segs.join('/')]
  if (req.method === 'GET' && fetchStatsFn) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPoolEvaluateDb,
      fetchStatsFn
    )
  } else if (await handlePlatformRoutes(req, res, pgPoolEvaluateDb, pgPoolStatsDb)) {
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
