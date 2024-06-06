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
  // Caveat! `new URL('//foo', 'http://127.0.0.1')` would produce "http://foo/" - not what we want!
  const { pathname, searchParams } = new URL(`http://127.0.0.1${req.url}`)
  const segs = pathname.split('/').filter(Boolean)
  if (req.method === 'GET' && segs[0] === 'retrieval-success-rate' && segs.length === 1) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchRetrievalSuccessRate)
  } else if (req.method === 'GET' && segs[0] === 'participants' && segs[1] === 'daily' && segs.length === 2) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchDailyParticipants)
  } else if (req.method === 'GET' && segs[0] === 'participants' && segs[1] === 'monthly' && segs.length === 2) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchMonthlyParticipants)
  } else if (req.method === 'GET' && segs.join('/') === 'participants/change-rates') {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchParticipantChangeRates)
  } else if (
    req.method === 'GET'
    && segs[0] === 'participants'
    && segs[1] === 'scheduled-rewards'
  ) {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchParticipantScheduledRewards)
  } else if (req.method === 'GET' && segs.join('/') === 'miners/retrieval-success-rate/summary') {
    await getStatsWithFilterAndCaching(
      pathname,
      searchParams,
      res,
      pgPool,
      fetchMinersRSRSummary)
  } else if (await handlePlatformRoutes(req, res, pgPool)) {
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
