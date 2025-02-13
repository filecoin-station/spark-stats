import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { ethers } from 'ethers'
import assert from 'node:assert'

import { RPC_URL, rpcHeaders } from '../lib/config.js'
import { observeTransferEvents, observeScheduledRewards, observeRetrievalResultCodes, observeYesterdayDesktopUsers } from '../lib/observer.js'
import { createInflux } from '../lib/telemetry.js'
import { getPgPools } from '@filecoin-station/spark-stats-db'

const { INFLUXDB_TOKEN } = process.env
assert(INFLUXDB_TOKEN, 'INFLUXDB_TOKEN required')

const pgPools = await getPgPools()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)

const { influx } = createInflux(INFLUXDB_TOKEN)
const influxQueryApi = influx.getQueryApi('Filecoin Station')

await pgPools.stats.query('DELETE FROM daily_reward_transfers')

await Promise.all([
  observeTransferEvents(pgPools.stats, ieContract, provider),
  observeScheduledRewards(pgPools, ieContract),
  observeRetrievalResultCodes(pgPools.stats, influxQueryApi),
  observeYesterdayDesktopUsers(pgPools.stats, influxQueryApi)
])

await pgPools.stats.end()
