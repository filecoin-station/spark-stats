import { updateDailyTransferStats } from './platform-stats.js'

/**
 * Observe the transfer events on the Filecoin blockchain
 * @param {import('pg').Pool} pgPool
 * @param {import('ethers').Contract} ieContract
 * @param {import('ethers').Provider} provider
 */
export const observeTransferEvents = async (pgPool, ieContract, provider) => {
  const { rows } = await pgPool.query(
    'SELECT MAX(last_checked_block) AS last_checked_block FROM daily_reward_transfers'
  )
  let queryFromBlock = rows[0].last_checked_block + 1
  const currentBlockNumber = await provider.getBlockNumber()

  if (!queryFromBlock || queryFromBlock < currentBlockNumber - 1900) {
    queryFromBlock = currentBlockNumber - 1900
    console.log('Block number too old, GLIF only provides last 2000 blocks, querying from -1900')
  }

  console.log('Querying impact evaluator Transfer events after block', queryFromBlock)
  const events = await ieContract.queryFilter(ieContract.filters.Transfer(), queryFromBlock)

  console.log(`Found ${events.length} Transfer events`)
  for (const event of events) {
    const transferEvent = {
      toAddress: event.args.to,
      amount: event.args.amount
    }
    console.log('Transfer event:', transferEvent)
    await updateDailyTransferStats(pgPool, transferEvent, currentBlockNumber)
  }

  return events.length
}
