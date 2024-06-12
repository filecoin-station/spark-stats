import assert from 'node:assert'
import { observeScheduledRewards } from '../lib/observer.js'
import { getPgPools } from '@filecoin-station/spark-stats-db'

describe('observer', () => {
  describe('observeScheduledRewards', () => {
    let pgPools

    before(async () => {
      pgPools = await getPgPools()
    })

    beforeEach(async () => {
      await pgPools.evaluate.query('DELETE FROM daily_participants')
      await pgPools.evaluate.query('DELETE FROM participants')
      const { rows: insertRows } = await pgPools.evaluate.query(`
        INSERT INTO participants
        (participant_address)
        VALUES
          ('0xCURRENT'),
          ('0xOLD')
        RETURNING id
      `)
      await pgPools.evaluate.query(`
        INSERT INTO daily_participants
        (participant_id, day)
        VALUES
          ($1, now()),
          ($2, now() - interval '4 days')
      `, [insertRows[0].id, insertRows[1].id])
    })

    it('observes scheduled rewards', async () => {
      const ieContract = {
        rewardsScheduledFor: async (address) => {
          if (address === '0xCURRENT') {
            return 100n
          } else {
            throw new Error('Should never be called')
          }
        }
      }
      await observeScheduledRewards(pgPools, ieContract)
      const { rows } = await pgPools.stats.query(`
        SELECT participant_address, scheduled_rewards
        FROM daily_scheduled_rewards
      `)
      assert.deepStrictEqual(rows, [{
        participant_address: '0xCURRENT',
        scheduled_rewards: '100'
      }])
    })
    it('updates scheduled rewards', async () => {
      const ieContract = {
        rewardsScheduledFor: async () => 200n
      }
      await observeScheduledRewards(pgPools, ieContract)
      const { rows } = await pgPools.stats.query(`
        SELECT participant_address, scheduled_rewards
        FROM daily_scheduled_rewards
      `)
      assert.deepStrictEqual(rows, [{
        participant_address: '0xCURRENT',
        scheduled_rewards: '200'
      }])
    })
  })
})
