import * as Sentry from '@sentry/node'
import Fastify from 'fastify'
import cors from '@fastify/cors'
import urlData from '@fastify/url-data'

import { addRoutes } from './routes.js'
import { addPlatformRoutes } from './platform-routes.js'

/** @typedef {import('@filecoin-station/spark-stats-db').PgPools} PgPools */
/** @typedef {import('./typings.js').DateRangeFilter} DateRangeFilter */

/**
 * @param {object} args
 * @param {string} args.SPARK_API_BASE_URL
 * @param {import('@filecoin-station/spark-stats-db').PgPools} args.pgPools
 * @param {Fastify.FastifyLoggerOptions} args.logger
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
  addRoutes(app, pgPools, SPARK_API_BASE_URL)
  addPlatformRoutes(app, pgPools)
  app.get('/', (request, reply) => {
    reply.send('OK')
  })

  return app
}
