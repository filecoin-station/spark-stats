import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { ethers } from 'ethers'
import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'

import { RPC_URL, rpcHeaders } from '../lib/config.js'
import { getPgPool } from '../lib/db.js'
import {
  observeTransferEvents,
  observeScheduledRewards
} from '../lib/observer.js'

const pgPool = await getPgPool()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)

const ONE_HOUR = 60 * 60 * 1000

await Promise.all([
  (async () => {
    while (true) {
      const start = new Date()
      try {
        await observeTransferEvents(pgPool, ieContract, provider)
      } catch (e) {
        console.error(e)
        Sentry.captureException(e)
      }
      const dt = new Date() - start
      console.log(`Observing Transfer events took ${dt}ms`)
      await timers.setTimeout(ONE_HOUR - dt)
    }
  })(),
  (async () => {
    while (true) {
      const start = new Date()
      try {
        await observeScheduledRewards(pgPool, ieContract, provider)
      } catch (e) {
        console.error(e)
        Sentry.captureException(e)
      }
      const dt = new Date() - start
      console.log(`Observing scheduled rewards took ${dt}ms`)
      await timers.setTimeout((24 * ONE_HOUR) - dt)
    }
  })()
])
