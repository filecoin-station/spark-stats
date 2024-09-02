import '../lib/instrument.js'
import http from 'node:http'
import { once } from 'node:events'
import { createHandler } from '../lib/handler.js'
import { getPgPools } from '@filecoin-station/spark-stats-db'

const {
  PORT = '8080',
  HOST = '127.0.0.1',
  SPARK_API_BASE_URL = 'https://api.filspark.com/',
  REQUEST_LOGGING = 'true'
} = process.env

const pgPools = await getPgPools()
const logger = {
  error: console.error,
  info: console.info,
  request: ['1', 'true'].includes(REQUEST_LOGGING) ? console.info : () => {}
}

const handler = createHandler({ SPARK_API_BASE_URL, pgPools, logger })
const server = http.createServer(handler)
console.log('Starting the http server on host %j port %s', HOST, PORT)
server.listen(Number(PORT), HOST)
await once(server, 'listening')
console.log(`http://${HOST}:${PORT}`)
