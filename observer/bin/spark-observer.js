import '../lib/instrument.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { ethers } from 'ethers'
import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'
import { InfluxDB } from '@influxdata/influxdb-client'
import assert from 'node:assert/strict'

import { RPC_URL, rpcHeaders } from '../lib/config.js'
import { getPgPools } from '@filecoin-station/spark-stats-db'
import {
  observeTransferEvents,
  observeScheduledRewards,
  observeRetrievalResultStatus
} from '../lib/observer.js'

const { INFLUXDB_TOKEN } = process.env

assert(INFLUXDB_TOKEN, 'INFLUXDB_TOKEN required')

const pgPools = await getPgPools()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)

const influx = new InfluxDB({
  url: 'https://eu-central-1-1.aws.cloud2.influxdata.com',
  // spark-stats-observer-read
  token: INFLUXDB_TOKEN
})

const influxQueryApi = influx.getQueryApi('Filecoin Station')

const ONE_HOUR = 60 * 60 * 1000

const loop = async (name, fn, interval) => {
  while (true) {
    const start = Date.now()
    try {
      await fn()
    } catch (e) {
      console.error(e)
      Sentry.captureException(e)
    }
    const dt = Date.now() - start
    console.log(`Loop "${name}" took ${dt}ms`)
    await timers.setTimeout(interval - dt)
  }
}

await Promise.all([
  loop(
    'Transfer events',
    () => observeTransferEvents(pgPools.stats, ieContract, provider),
    ONE_HOUR
  ),
  loop(
    'Scheduled rewards',
    () => observeScheduledRewards(pgPools, ieContract),
    24 * ONE_HOUR
  ),
  loop(
    'Retrieval result status breakdown',
    () => observeRetrievalResultStatus(pgPools.stats, influxQueryApi),
    ONE_HOUR
  )
])
