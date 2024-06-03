/**
 * @param {import('pg').Client} pgClient
 * @param {Object} transferEvent
 * @param {string} transferEvent.to_address
 * @param {number} transferEvent.amount
 * @param {number} transferEvent.blockNumber
 */
export const updateDailyTransferStats = async (pgClient, transferEvent) => {
  await pgClient.query(`
    INSERT INTO daily_reward_transfers (day, to_address, amount)
    VALUES (now(), $1, $2)
    ON CONFLICT (day, to_address) DO UPDATE
    SET amount = daily_reward_transfers.amount + EXCLUDED.amount
  `, [transferEvent.to_address, transferEvent.amount])
}
