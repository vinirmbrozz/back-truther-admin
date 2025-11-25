import { makeGetKycDataUserUseCase } from "@/application/factories/kyc/make-get-kyc-data-user";
import { FastifyReply, FastifyRequest } from "fastify";

export async function getKycDataUserController(req: FastifyRequest, reply: FastifyReply) {
  const { document } = req.body as { document: string };

  const useCase = makeGetKycDataUserUseCase();

  const result = await useCase.execute({
    document,
    externalToken: (req as any).externalToken,
  });

  if (req.audit) {
    await req.audit({
      action: "security",
      message: "External authentication",
      description: `Usuário ${req.user?.name} acessou dados de compliance`,
      method: req.method,
      senderType: "USER",
      senderId: String(req.user?.sub),
      targetType: "ADMIN",
      targetId: "1",
      targetExternalId: "",
      severity: "medium"
    });
  }

  return reply.status(result.status).send(result.data);
}
