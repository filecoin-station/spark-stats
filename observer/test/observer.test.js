import assert from 'node:assert'
import { observeScheduledRewards } from '../lib/observer.js'
import { getPgPools } from '../../common/db.js'

describe('observer', () => {
  describe('observeScheduledRewards', () => {
    let pgPools

    before(async () => {
      pgPools = await getPgPools()
    })

    it('observes scheduled rewards', async () => {
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
        SELECT *
        FROM daily_scheduled_rewards
      `)
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0].participant_address, '0xCURRENT')
      assert.strictEqual(rows[0].scheduled_rewards, '100')
    })
    it('updates scheduled rewards', async () => {
      const ieContract = {
        rewardsScheduledFor: async () => 200n
      }
      await observeScheduledRewards(pgPools, ieContract)
      const { rows } = await pgPools.stats.query(`
        SELECT *
        FROM daily_scheduled_rewards
      `)
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0].participant_address, '0xCURRENT')
      assert.strictEqual(rows[0].scheduled_rewards, '200')
    })
  })
})
