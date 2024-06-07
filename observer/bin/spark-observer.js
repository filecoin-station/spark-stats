import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { ethers } from 'ethers'
import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'

import { RPC_URL, rpcHeaders, OBSERVATION_INTERVAL_MS } from '../lib/config.js'
import { getPgPool } from '../lib/db.js'
import { observe } from '../lib/observer.js'

const pgPool = await getPgPool()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)

while (true) {
  const start = new Date()
  try {
    await observe(pgPool, ieContract, provider)
  } catch (e) {
    console.error(e)
    Sentry.captureException(e)
  }
  const dt = new Date() - start
  console.log(`Observation took ${dt}ms`)
  await timers.setTimeout(OBSERVATION_INTERVAL_MS - dt)
}
