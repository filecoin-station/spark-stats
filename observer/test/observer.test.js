import assert from 'node:assert'
import { observeScheduledRewards } from '../lib/observer.js'
import { getPgPools } from '@filecoin-station/spark-stats-db'
import { givenDailyParticipants } from 'spark-evaluate/test/helpers/queries.js'

describe('observer', () => {
  describe('observeScheduledRewards', () => {
    let pgPools

    before(async () => {
      pgPools = await getPgPools()
    })

    beforeEach(async () => {
      await pgPools.evaluate.query('DELETE FROM daily_participants')
      await pgPools.evaluate.query('DELETE FROM participants')
      const today = new Date()
      await givenDailyParticipants(
        pgPools.evaluate,
        `${today.getFullYear()}-${today.getMonth() + 1}-${today.getDate()}`,
        ['0xCURRENT']
      )
      await givenDailyParticipants(
        pgPools.evaluate,
        `2000-01-01`,
        ['0xOLD']
      )
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
