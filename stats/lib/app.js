import * as Sentry from '@sentry/node'
import Fastify from 'fastify'
import cors from '@fastify/cors'
import urlData from '@fastify/url-data'

import { preHandlerHook, onSendHook } from './request-helpers.js'

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
/** @typedef {import('./typings.js').RequestWithFilterAndMinerId} RequestWithFilterAndMinerId */

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

  app.register(async app => {
    app.addHook('preHandler', preHandlerHook)
    app.addHook('onSend', onSendHook)

    app.get('/deals/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDailyDealStats(pgPools, request.filter))
    })
    app.get('/deals/summary', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDealSummary(pgPools, request.filter))
    })
    app.get('/retrieval-success-rate', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchRetrievalSuccessRate(pgPools, request.filter))
    })
    app.get('/participants/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDailyParticipants(pgPools, request.filter))
    })
    app.get('/participants/monthly', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchMonthlyParticipants(pgPools, request.filter))
    })
    app.get('/participants/change-rates', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchParticipantChangeRates(pgPools, request.filter))
    })
    app.get('/participant/:address/scheduled-rewards', async (/** @type {RequestWithFilterAndAddress} */ request, reply) => {
      reply.send(await fetchParticipantScheduledRewards(pgPools, request.filter, request.params.address))
    })
    app.get('/participant/:address/reward-transfers', async (/** @type {RequestWithFilterAndAddress} */ request, reply) => {
      reply.send(await fetchParticipantRewardTransfers(pgPools, request.filter, request.params.address))
    })
    app.get('/miners/retrieval-success-rate/summary', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchMinersRSRSummary(pgPools, request.filter))
    })
    app.get('/miners/retrieval-timings/summary', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchMinersTimingsSummary(pgPools, request.filter))
    })
    app.get('/retrieval-result-codes/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDailyRetrievalResultCodes(pgPools, request.filter))
    })
    app.get('/retrieval-timings/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDailyRetrievalTimings(pgPools, request.filter))
    })
    app.get('/miner/:minerId/retrieval-timings/summary', async (/** @type {RequestWithFilterAndMinerId} */ request, reply) => {
      reply.send(await fetchDailyMinerRetrievalTimings(pgPools, request.filter, request.params.minerId))
    })
    app.get('/miner/:minerId/retrieval-success-rate/summary', async (/** @type {RequestWithFilterAndMinerId} */ request, reply) => {
      reply.send(await fetchDailyMinerRSRSummary(pgPools, request.filter, request.params.minerId))
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
