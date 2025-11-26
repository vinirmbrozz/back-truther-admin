import { makePostRetryKycUseCase } from "@/application/factories/kyc/make-post-retry-kyc";
import { FastifyReply, FastifyRequest } from "fastify";

export async function postRetryKycController(req: FastifyRequest, reply: FastifyReply) {
  const { uuid, ouccurenceuuid, internalComent, tryKycType } = req.body as any;

  const useCase = makePostRetryKycUseCase();

  const result = await useCase.execute({
    uuid,
    ouccurenceuuid,
    internalComent,
    tryKycType,
    externalToken: (req as any).externalToken,
  });

  if (req.audit) {
    await req.audit({
      action: "security",
      message: "External: Retry Kyc",
      description: `Usuário ${req.user?.name} realizou Retry KYC`,
      method: req.method,
      senderType: "USER",
      senderId: String(req.user?.sub),
      targetType: "ADMIN",
      targetId: "1",
      targetExternalId: "",
      severity: "medium",
    });
  }

  return reply.status(result.status).send(result.data);
}
