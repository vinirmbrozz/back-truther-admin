import { FastifyInstance } from 'fastify'
import { ZodTypeProvider } from 'fastify-type-provider-zod'
import { verifyJwt } from '../../middlewares/verify-jwt'
import { env } from '@/infra/env'
import { externalAuthHook } from '../../middlewares/external-auth'

const proxyBase = env.SERVICE_PROXY_URL || process.env.SERVICE_PROXY_URL

export async function servicesRoutes(app: FastifyInstance) {
  const basePath = '/admin'

  app.withTypeProvider<ZodTypeProvider>().get(
    `${basePath}/get-services`,
    {
      preHandler: [verifyJwt(), externalAuthHook],
      schema: { 
        tags: ['Services'],
        summary: 'Proxy to external get-services'
      },
    },
    async (req, reply) => {
      if (!proxyBase) return reply.status(500).send({ error: 'SERVICE_PROXY_URL not configured' })
      const res = await fetch(`${proxyBase}/${basePath}/get-services`, {
        headers: { authorization: `Bearer ${(req as any).externalToken}` },
      })
      const json = await res.json().catch(() => null)
      return reply.status(res.status).send(json)
    }
  )

  app.withTypeProvider<ZodTypeProvider>().get(
    `${basePath}/block-levels`,
    {
      preHandler: [verifyJwt(), externalAuthHook],
      schema: { 
        tags: ['Services'], 
        summary: 'Proxy to external block-levels' 
      },
    },
    async (req, reply) => {
      if (!proxyBase) return reply.status(500).send({ error: 'SERVICE_PROXY_URL not configured' })
      const res = await fetch(`${proxyBase}/${basePath}/service-block-levels`, {
        headers: { authorization: `Bearer ${(req as any).externalToken}` },
      })
      const json = await res.json().catch(() => null)
      console.log(json)
      return reply.status(res.status).send(json)
    }
  )

  app.withTypeProvider<ZodTypeProvider>().put(
    `${basePath}/service-block-levels/users/:user_id/tag/:tag`,
    {
      preHandler: [verifyJwt(), externalAuthHook],
      schema: { 
        tags: ['Services'], 
        summary: 'Proxy to set user block level (PUT)' },
    },
    async (req, reply) => {
      if (!proxyBase) return reply.status(500).send({ error: 'SERVICE_PROXY_URL not configured' })
      const { user_id, tag } = req.params as any
      const res = await fetch(`${proxyBase}/${basePath}/service-block-levels/users/${user_id}/tag/${tag}`, {
        method: 'PUT',
        headers: {
          authorization: (req.headers.authorization as string) || '',
          'content-type': 'application/json',
        },
        body: JSON.stringify(req.body || {}),
      })
      const json = await res.json().catch(() => null)
      return reply.status(res.status).send(json)
    }
  )

  app.withTypeProvider<ZodTypeProvider>().delete(
    `${basePath}/service-block-levels/users/:user_id`,
    {
      preHandler: [verifyJwt(), externalAuthHook],
      schema: { 
        tags: ['Services'], 
        summary: 'Proxy to clear user block level (DELETE)' 
      },
    },
    async (req, reply) => {
      if (!proxyBase) return reply.status(500).send({ error: 'SERVICE_PROXY_URL not configured' })
      const { user_id } = req.params as any
      const res = await fetch(`${proxyBase}/${basePath}/service-block-levels/users/${user_id}`, {
        method: 'DELETE',
        headers: { authorization: `Bearer ${(req as any).externalToken}` },
      })
      const json = await res.json().catch(() => null)
      return reply.status(res.status).send(json)
    }
  )
}
