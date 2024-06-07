import { updateDailyTransferStats } from './platform-stats.js'

/**
 * Observe the transfer events on the Filecoin blockchain
 * @param {import('pg').Pool} pgPool
 * @param {import('ethers').Contract} ieContract
 * @param {import('ethers').Provider} provider
 */
export const observeTransferEvents = async (pgPool, ieContract, provider) => {
  const { rows } = await pgPool.query(
    'SELECT MAX(last_checked_block) FROM daily_reward_transfers'
  )
  const lastCheckedBlock = rows[0].last_checked_block

  console.log('Querying impact evaluator Transfer events after block', lastCheckedBlock)
  let events
  try {
    events = await ieContract.queryFilter(ieContract.filters.Transfer(), lastCheckedBlock)
  } catch (error) {
    console.error('Error querying impact evaluator Transfer events', error)
    if (error.message.includes('bad tipset height')) {
      console.log('Block number too old, GLIF only provides last 2000 blocks, querying from -1900')
      events = await ieContract.queryFilter(ieContract.filters.Transfer(), -1900)
    } else {
      throw error
    }
  }
  const currentBlockNumber = await provider.getBlockNumber()
  console.log('Current block number:', currentBlockNumber)
  console.log(`Found ${events.length} Transfer events`)
  for (const event of events) {
    const transferEvent = {
      to_address: event.args.to,
      amount: event.args.amount
    }
    console.log('Transfer event:', transferEvent)
    await updateDailyTransferStats(pgPool, transferEvent, currentBlockNumber)
  }
}
