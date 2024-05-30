import { ethers } from 'ethers'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'

// TODO: move this to a config.js file
const {
  RPC_URLS = 'https://api.node.glif.io/rpc/v0',
  GLIF_TOKEN
} = process.env

const rpcUrls = RPC_URLS.split(',')
const RPC_URL = rpcUrls[Math.floor(Math.random() * rpcUrls.length)]
console.log(`Selected JSON-RPC endpoint ${RPC_URL}`)

const rpcHeaders = {}
if (RPC_URL.includes('glif')) {
  rpcHeaders.Authorization = `Bearer ${GLIF_TOKEN}`
}

// TODO: move this to a different file
const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(
  fetchRequest,
  null,
  { polling: true }
)

const ieContract = new ethers.Contract(
  SparkImpactEvaluator.ADDRESS,
  SparkImpactEvaluator.ABI,
  provider
)

ieContract.on('Transfer', (to, amount, ...args) => {
  /** @type {number} */
  const blockNumber = args.pop()
  console.log('Transfer %s FIL to %s at epoch %s', amount, to, blockNumber)
  // TODO: update the database
})
