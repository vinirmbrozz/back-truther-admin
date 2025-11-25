import { FastifyInstance } from "fastify";
import { ZodTypeProvider } from "fastify-type-provider-zod";
import { verifyJwt } from "../../middlewares/verify-jwt";
import { externalAuthHook } from "../../middlewares/external-auth";
import { env } from "@/infra/env";
import { getKycDataUserController } from "../../controllers/kyc/get-kyc-data-user-controller";
import z from "zod";

const proxyBase = env.SERVICE_PROXY_URL || process.env.SERVICE_PROXY_URL;

export async function kycRoutes(app: FastifyInstance) {
  app.withTypeProvider<ZodTypeProvider>().post(
    "/kyc/data-user",
    {
      preHandler: [verifyJwt(), externalAuthHook],
      schema: {
        tags: ["KYC"],
        summary: "Proxy: get KYC data by document",
        body: z.object({
          document: z.string(),
        }),
      },
    },
    getKycDataUserController
  );

  app.withTypeProvider<ZodTypeProvider>().get(
    "/kyc/list-users",
    {
      preHandler: [verifyJwt(), externalAuthHook],
      schema: {
        tags: ["KYC"],
        summary: "Proxy: list users compliance",
      },
    },
    async (req, reply) => {
      if (!proxyBase) {
        return reply
          .status(500)
          .send({ error: "SERVICE_PROXY_URL not configured" });
      }

      const token = (req as any).externalToken;

      const res = await fetch(`${proxyBase}/compliance/list-users`, {
        headers: { authorization: `Bearer ${token}` },
      });

      const json = await res.json().catch(() => null);

      return reply.status(res.status).send(json);
    }
  );
}
