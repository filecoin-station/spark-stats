import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { ethers } from 'ethers'
import * as Sentry from '@sentry/node'

import { RPC_URL, rpcHeaders, OBSERVATION_INTERVAL_MS } from '../lib/config.js'
import { getPgPool } from '../lib/db.js'
import { observeTransferEvents } from '../lib/observer.js'

const pgPool = await getPgPool()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)

// Listen for Transfer events from the IE contract
while (true) {
  try {
    await observeTransferEvents(pgPool, ieContract, provider)
  } catch (e) {
    console.error(e)
    Sentry.captureException(e)
  }
  await new Promise(resolve => setTimeout(resolve, OBSERVATION_INTERVAL_MS))
}
