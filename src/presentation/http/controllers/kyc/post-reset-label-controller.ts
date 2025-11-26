import { makePostResetLabelUseCase } from "@/application/factories/kyc/make-post-reset-label";
import { FastifyReply, FastifyRequest } from "fastify";

export async function postResetLabelController(req: FastifyRequest, reply: FastifyReply) {
  const { document, labels } = req.body as any;

  const useCase = makePostResetLabelUseCase();

  const result = await useCase.execute({
    document,
    labels,
    externalToken: (req as any).externalToken,
  });

  if (req.audit) {
    await req.audit({
      action: "security",
      message: "External: Reset Label",
      description: `Usuário ${req.user?.name} realizou Reset Label`,
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
