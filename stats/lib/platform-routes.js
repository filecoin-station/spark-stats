import { withFilter } from './request-helpers.js'
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

/** @typedef {import('./typings.js').RequestWithFilter} RequestWithFilter */

export const addPlatformRoutes = (app, pgPools) => {
  app.get('/stations/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyStationCount(pgPools.evaluate, filter)
    })
  })
  app.get('/stations/monthly', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchMonthlyStationCount(pgPools.evaluate, filter)
    })
  })
  app.get('/measurements/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyStationMeasurementCounts(pgPools.evaluate, filter)
    })
  })
  app.get('/participants/top-measurements', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchParticipantsWithTopMeasurements(pgPools.evaluate, filter)
    })
  })
  app.get('/participants/top-earning', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchTopEarningParticipants(pgPools.stats, filter)
    })
  })
  app.get('/participants/summary', async (request, reply) => {
    reply.header('cache-control', `public, max-age=${24 * 3600 /* one day */}`)
    reply.send(await fetchParticipantsSummary(pgPools.evaluate))
  })
  app.get('/participants/accumulative/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchAccumulativeDailyParticipantCount(pgPools.evaluate, filter)
    })
  })
  app.get('/transfers/daily', async (/** @type {RequestWithFilter} */ request, reply) => {
    await withFilter(request, reply, filter => {
      return fetchDailyRewardTransfers(pgPools.stats, filter)
    })
  })
}
