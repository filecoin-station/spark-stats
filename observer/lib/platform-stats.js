/**
 * @param {import('pg').Client} pgClient
 * @param {Object} transferEvent
 * @param {string} transferEvent.to_address
 * @param {number} transferEvent.amount
 * @param {number} currentBlockNumber
 */
export const updateDailyTransferStats = async (pgClient, transferEvent, currentBlockNumber) => {
  await pgClient.query(`
    INSERT INTO daily_reward_transfers (day, to_address, amount, last_checked_block)
    VALUES (now(), $1, $2, $3)
    ON CONFLICT (day, to_address) DO UPDATE SET
      amount = daily_reward_transfers.amount + EXCLUDED.amount
  `, [transferEvent.to_address, transferEvent.amount, currentBlockNumber])
}