import * as Sentry from '@sentry/node'
import { json, status } from 'http-responders'

import { getStatsWithFilterAndCaching } from './request-helpers.js'

import {
  fetchDailyDealStats,
  fetchDailyParticipants,
  fetchMinersRSRSummary,
  fetchMonthlyParticipants,
  fetchParticipantChangeRates,
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
 */
const handler = async (req, res, pgPools) => {
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
  } else if (req.method === 'GET' && segs[0] === 'participant' && segs[1] && segs[2] === 'scheduled-rewards') {
    await respond(fetchParticipantScheduledRewards, segs[1])
  } else if (req.method === 'GET' && segs[0] === 'participant' && segs[1] && segs[2] === 'reward-transfers') {
    await respond(fetchParticipantRewardTransfers, segs[1])
  } else if (req.method === 'GET' && url === '/miners/retrieval-success-rate/summary') {
    await respond(fetchMinersRSRSummary)
  } else if (req.method === 'GET' && segs[0] === 'miner' && segs[1] && segs[2] === 'deals' && segs[3] === 'eligible' && segs[4] === 'summary') {
    await getRetrievableDealsForMiner(req, res, pgPools.api, segs[1])
  } else if (req.method === 'GET' && segs[0] === 'client' && segs[1] && segs[2] === 'deals' && segs[3] === 'eligible' && segs[4] === 'summary') {
    await getRetrievableDealsForClient(req, res, pgPools.api, segs[1])
  } else if (req.method === 'GET' && segs[0] === 'allocator' && segs[1] && segs[2] === 'deals' && segs[3] === 'eligible' && segs[4] === 'summary') {
    await getRetrievableDealsForAllocator(req, res, pgPools.api, segs[1])
  } else if (await handlePlatformRoutes(req, res, pgPools)) {
    // no-op, request was handled by handlePlatformRoute
  } else if (req.method === 'GET' && url === '/') {
    // health check - required by Grafana datasources
    res.end('OK')
  } else {
    status(res, 404)
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
    status(res, 500)
  }

  if (res.statusCode >= 500) {
    Sentry.captureException(err)
  }
}

/**
 * @param {import('node:http').IncomingMessage} _req
 * @param {import('node:http').ServerResponse} res
 * @param {PgPools['api']} client
 * @param {string} minerId
 */
const getRetrievableDealsForMiner = async (_req, res, client, minerId) => {
  /** @type {{rows: {client_id: string; deal_count: number}[]}} */
  const { rows } = await client.query(`
    SELECT client_id, COUNT(cid)::INTEGER as deal_count FROM retrievable_deals
    WHERE miner_id = $1 AND expires_at > now()
    GROUP BY client_id
    ORDER BY deal_count DESC, client_id ASC
    `, [
    minerId
  ])

  // Cache the response for 6 hours
  res.setHeader('cache-control', `max-age=${6 * 3600}`)

  const body = {
    minerId,
    dealCount: rows.reduce((sum, row) => sum + row.deal_count, 0),
    clients:
      rows.map(
        // eslint-disable-next-line camelcase
        ({ client_id, deal_count }) => ({ clientId: client_id, dealCount: deal_count })
      )
  }

  json(res, body)
}

const getRetrievableDealsForClient = async (_req, res, client, clientId) => {
  /** @type {{rows: {miner_id: string; deal_count: number}[]}} */
  const { rows } = await client.query(`
    SELECT miner_id, COUNT(cid)::INTEGER as deal_count FROM retrievable_deals
    WHERE client_id = $1 AND expires_at > now()
    GROUP BY miner_id
    ORDER BY deal_count DESC, miner_id ASC
    `, [
    clientId
  ])

  // Cache the response for 6 hours
  res.setHeader('cache-control', `max-age=${6 * 3600}`)

  const body = {
    clientId,
    dealCount: rows.reduce((sum, row) => sum + row.deal_count, 0),
    providers: rows.map(
      // eslint-disable-next-line camelcase
      ({ miner_id, deal_count }) => ({ minerId: miner_id, dealCount: deal_count })
    )
  }
  json(res, body)
}

const getRetrievableDealsForAllocator = async (_req, res, client, allocatorId) => {
  /** @type {{rows: {client_id: string; deal_count: number}[]}} */
  const { rows } = await client.query(`
    SELECT ac.client_id, COUNT(cid)::INTEGER as deal_count
    FROM allocator_clients ac
    LEFT JOIN retrievable_deals rd ON ac.client_id = rd.client_id
    WHERE ac.allocator_id = $1 AND expires_at > now()
    GROUP BY ac.client_id
    ORDER BY deal_count DESC, ac.client_id ASC
    `, [
    allocatorId
  ])

  // Cache the response for 6 hours
  res.setHeader('cache-control', `max-age=${6 * 3600}`)

  const body = {
    allocatorId,
    dealCount: rows.reduce((sum, row) => sum + row.deal_count, 0),
    clients: rows.map(
      // eslint-disable-next-line camelcase
      ({ client_id, deal_count }) => ({ clientId: client_id, dealCount: deal_count })
    )
  }
  json(res, body)
}
