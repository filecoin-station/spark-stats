import {
  fetchDailyStationCount,
  fetchMonthlyStationCount,
  fetchDailyRewardTransfers,
  fetchTopEarningParticipants,
  fetchParticipantsWithTopMeasurements,
  fetchDailyStationMeasurementCounts,
  fetchParticipantsSummary,
  fetchAccumulativeDailyParticipantCount
} from './platform-stats-fetchers.js'

import { preHandlerHook, onSendHook } from './request-helpers.js'

/** @typedef {import('./typings.js').RequestWithFilter} RequestWithFilter */

export const addPlatformRoutes = (app, pgPools) => {
  app.register(async app => {
    app.addHook('preHandler', preHandlerHook)
    app.addHook('onSend', onSendHook)

    app.get('/stations/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDailyStationCount(pgPools.evaluate, request.filter))
    })
    app.get('/stations/monthly', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchMonthlyStationCount(pgPools.evaluate, request.filter))
    })
    app.get('/measurements/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDailyStationMeasurementCounts(pgPools.evaluate, request.filter))
    })
    app.get('/participants/top-measurements', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchParticipantsWithTopMeasurements(pgPools.evaluate, request.filter))
    })
    app.get('/participants/top-earning', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchTopEarningParticipants(pgPools.stats, request.filter))
    })
    app.get('/participants/accumulative/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchAccumulativeDailyParticipantCount(pgPools.evaluate, request.filter))
    })
    app.get('/transfers/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
      reply.send(await fetchDailyRewardTransfers(pgPools.stats, request.filter))
    })
  })

  app.get('/participants/summary', async (request, reply) => {
    reply.header('cache-control', `public, max-age=${24 * 3600 /* one day */}`)
    reply.send(await fetchParticipantsSummary(pgPools.evaluate))
  })
}
