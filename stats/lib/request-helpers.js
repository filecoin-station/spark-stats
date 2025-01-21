import assert from 'http-assert'
import { URLSearchParams } from 'node:url'

/** @typedef {import('@filecoin-station/spark-stats-db').Queryable} Queryable */
/** @typedef {import('./typings.js').RequestWithFilter} RequestWithFilter */

export const getLocalDayAsISOString = (d) => {
  return [
    d.getFullYear(),
    String(d.getMonth() + 1).padStart(2, '0'),
    String(d.getDate()).padStart(2, '0')
  ].join('-')
}

export const today = () => getLocalDayAsISOString(new Date())
export const yesterday = () => getLocalDayAsISOString(new Date(Date.now() - 24 * 60 * 60 * 1000))

/** @typedef {import('@filecoin-station/spark-stats-db').PgPools} PgPools */
/**
 * @template {import('./typings.js').DateRangeFilter} FilterType
 * @param {RequestWithFilter} request
 * @param {import('fastify').FastifyReply} reply
 * @param {(filter: FilterType) => Promise<object[]>} fn
 */
export const withFilter = async (request, reply, fn) => {
  const filter = request.query
  let shouldRedirect = false

  filter.from = handleDateKeyword(filter.from)
  filter.to = handleDateKeyword(filter.to)

  if (!filter.to) {
    filter.to = today()
    shouldRedirect = true
  }
  if (!filter.from) {
    filter.from = filter.to
    shouldRedirect = true
  }
  if (shouldRedirect) {
    reply.header('cache-control', `public, max-age=${600 /* 10min */}`)
    return reply.redirect(
      `${request.urlData().path}?${new URLSearchParams(Object.entries(filter))}`,
      302 // Found
    )
  }

  // Trim time from date-time values that are typically provided by Grafana

  const matchFrom = filter.from.match(/^(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}:\d{2}\.\d{3}Z)?$/)
  assert(matchFrom, 400, '"from" must have format YYYY-MM-DD or YYYY-MM-DDThh:mm:ss.sssZ')
  if (matchFrom[2]) {
    filter.from = matchFrom[1]
    shouldRedirect = true
  }

  const matchTo = filter.to.match(/^(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}:\d{2}\.\d{3}Z)?$/)
  assert(matchTo, 400, '"to" must have format YYYY-MM-DD or YYYY-MM-DDThh:mm:ss.sssZ')
  if (matchTo[2]) {
    filter.to = matchTo[1]
    shouldRedirect = true
  }

  if (shouldRedirect) {
    reply.header('cache-control', `public, max-age=${24 * 3600 /* one day */}`)
    return reply.redirect(
      `${request.urlData().path}?${new URLSearchParams(Object.entries(filter))}`,
      301 // Found
    )
  }

  // We have well-formed from & to dates now, let's fetch the requested stats from the DB

  // Workaround for the following TypeScript error:
  // Argument of type '{ [k: string]: string; }' is not assignable to parameter
  //   of type 'FilterType'.
  // 'FilterType' could be instantiated with an arbitrary type which could be
  //   unrelated to '{ [k: string]: string; }'
  const typedFilter = /** @type {FilterType} */(/** @type {unknown} */(filter))
  const stats = await fn(typedFilter)
  setCacheControlForStatsResponse(reply, typedFilter)
  reply.send(stats)
}

/**
 * @param {import('fastify').FastifyReply} reply
 * @param {import('./typings.js').DateRangeFilter} filter
 */
const setCacheControlForStatsResponse = (reply, filter) => {
  // We cannot simply compare filter.to vs today() because there may be a delay in finalizing
  // stats for the previous day. Let's allow up to one hour for the finalization.
  const boundary = getLocalDayAsISOString(new Date(Date.now() - 3600_000))

  if (filter.to >= boundary) {
    // response includes partial data for today, cache it for 10 minutes only
    reply.header('cache-control', 'public, max-age=600')
  } else {
    // historical data should never change, cache it for one year
    reply.header('cache-control', `public, max-age=${365 * 24 * 3600}, immutable`)
  }
}

/**
 * @param {string} date
 * @returns {string}
 */
const handleDateKeyword = (date) => {
  switch (date) {
    case 'today':
      return today()
    case 'yesterday':
      return yesterday()
    default:
      return date
  }
}
