import { makePostDisinterestUseCase } from "@/application/factories/kyc/make-post-disinterest";
import { FastifyReply, FastifyRequest } from "fastify";

export async function postDisinterestController(req: FastifyRequest, reply: FastifyReply) {
  const { uuid, internalComent, externalComent, reason } = req.body as any;

  const useCase = makePostDisinterestUseCase();

  const result = await useCase.execute({
    uuid,
    internalComent,
    externalComent,
    reason,
    externalToken: (req as any).externalToken,
  });

  if (req.audit) {
    await req.audit({
      action: "security",
      message: "External: Disinterest",
      description: `Usuário ${req.user?.name} realizou Disinterest`,
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
