import { FastifyRequest } from 'fastify'

export interface DateRangeFilter {
  from: string;
  to: string;
}

export type RequestWithFilter = FastifyRequest<{
  Querystring: { from: string?, to: string? }
}>
export type RequestWithFilterAndAddress = RequestWithFilter<{
  Parameters: { address: string }
}>
export type RequestWithFilterAndMinerId = RequestWithFilter<{
  Parameters: { minerId: string }
}>
