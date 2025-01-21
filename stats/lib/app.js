import * as Sentry from '@sentry/node'
import Fastify from 'fastify'
import cors from '@fastify/cors'
import urlData from '@fastify/url-data'

import { withFilter } from './request-helpers.js'

import {
  fetchDailyDealStats,
  fetchDailyParticipants,
  fetchMinersRSRSummary,
  fetchMonthlyParticipants,
  fetchParticipantChangeRates,
  fetchParticipantScheduledRewards,
  fetchParticipantRewardTransfers,
  fetchRetrievalSuccessRate,
  fetchDealSummary,
  fetchDailyRetrievalResultCodes,
  fetchDailyMinerRSRSummary,
  fetchDailyRetrievalTimings,
  fetchDailyMinerRetrievalTimings,
  fetchMinersTimingsSummary
} from './stats-fetchers.js'

import { addPlatformRoutes } from './platform-routes.js'

/** @typedef {import('@filecoin-station/spark-stats-db').PgPools} PgPools */
/** @typedef {import('./typings.js').DateRangeFilter} DateRangeFilter */
/** @typedef {import('./typings.js').RequestWithFilter} RequestWithFilter */
/** @typedef {import('./typings.js').RequestWithFilterAndAddress} RequestWithFilterAndAddress */
/** @typedef {import('./typings.js').RequestWithFilterAndAddress} RequestWithFilterAndMinerId */

/**
 * @param {object} args
 * @param {string} args.SPARK_API_BASE_URL
 * @param {import('@filecoin-station/spark-stats-db').PgPools} args.pgPools
 * @param {boolean} args.logger
 * @returns
 */
export const createApp = ({
  SPARK_API_BASE_URL,
  pgPools,
  logger
}) => {
  const app = Fastify({ logger })
  Sentry.setupFastifyErrorHandler(app)

  app.register(cors, {
    origin: ['http://localhost:3000', 'app://-']
  })
  app.register(urlData)

  app.get('/deals/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyDealStats(pgPools, filter)
    })
  })
  app.get('/deals/summary', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDealSummary(pgPools, filter)
    })
  })
  app.get('/retrieval-success-rate', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchRetrievalSuccessRate(pgPools, filter)
    })
  })
  app.get('/participants/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyParticipants(pgPools, filter)
    })
  })
  app.get('/participants/monthly', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchMonthlyParticipants(pgPools, filter)
    })
  })
  app.get('/participants/change-rates', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchParticipantChangeRates(pgPools, filter)
    })
  })
  app.get('/participant/:address/scheduled-rewards', async (/** @type {RequestWithFilterAndAddress} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchParticipantScheduledRewards(pgPools, filter, request.params.address)
    })
  })
  app.get('/participant/:address/reward-transfers', async (/** @type {RequestWithFilterAndAddress} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchParticipantRewardTransfers(pgPools, filter, request.params.address)
    })
  })
  app.get('/miners/retrieval-success-rate/summary', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchMinersRSRSummary(pgPools, filter)
    })
  })
  app.get('/miners/retrieval-timings/summary', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchMinersTimingsSummary(pgPools, filter)
    })
  })
  app.get('/retrieval-result-codes/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyRetrievalResultCodes(pgPools, filter)
    })
  })
  app.get('/retrieval-timings/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyRetrievalTimings(pgPools, filter)
    })
  })
  app.get('/miner/:minerId/retrieval-timings/summary', async (/** @type {RequestWithFilterAndMinerId} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyMinerRetrievalTimings(pgPools, filter, request.params.minerId)
    })
  })
  app.get('/miner/:minerId/retrieval-success-rate/summary', async (/** @type {RequestWithFilterAndMinerId} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyMinerRSRSummary(pgPools, filter, request.params.minerId)
    })
  })
  app.get('/miner/:minerId/deals/eligible/summary', (request, reply) => {
    redirectToSparkApi(request, reply, SPARK_API_BASE_URL)
  })
  app.get('/client/:clientId/deals/eligible/summary', (request, reply) => {
    redirectToSparkApi(request, reply, SPARK_API_BASE_URL)
  })
  app.get('/allocator/:allocatorId/deals/eligible/summary', (request, reply) => {
    redirectToSparkApi(request, reply, SPARK_API_BASE_URL)
  })
  addPlatformRoutes(app, pgPools)
  app.get('/', (request, reply) => {
    reply.send('OK')
  })

  return app
}

/**
 * @param {Fastify.FastifyRequest} request
 * @param {Fastify.FastifyReply} reply
 * @param {string} SPARK_API_BASE_URL
 */
const redirectToSparkApi = (request, reply, SPARK_API_BASE_URL) => {
  // Cache the response for 6 hours
  reply.header('cache-control', `max-age=${6 * 3600}`)

  const location = new URL(request.url, SPARK_API_BASE_URL).toString()
  reply.redirect(location, 302)
}