import '../lib/instrument.js'
import { createApp } from '../lib/app.js'
import { getPgPools } from '@filecoin-station/spark-stats-db'

const {
  PORT = '8080',
  HOST = '127.0.0.1',
  SPARK_API_BASE_URL = 'https://api.filspark.com/',
  REQUEST_LOGGING = 'true'
} = process.env

const pgPools = await getPgPools()

const app = await createApp({
  SPARK_API_BASE_URL,
  pgPools,
  logger: {
    level: ['1', 'true'].includes(REQUEST_LOGGING) ? 'info' : 'error'
  }
})
console.log('Starting the http server on host %j port %s', HOST, PORT)
const baseUrl = app.listen({ port: Number(PORT), host: HOST })
console.log(baseUrl)
