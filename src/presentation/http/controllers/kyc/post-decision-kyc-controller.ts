import { makePostDecisionKycUseCase } from "@/application/factories/kyc/make-post-decision-kyc";
import { FastifyReply, FastifyRequest } from "fastify";

export async function postDecisionKycController(req: FastifyRequest, reply: FastifyReply) {
  const { decision, internalComent, levelKyc, document } = req.body as {
    decision: boolean;
    internalComent: string | null;
    levelKyc: string;
    document: string;
  };

  const useCase = makePostDecisionKycUseCase();

  const result = await useCase.execute({
    decision,
    internalComent,
    levelKyc,
    document,
    externalToken: (req as any).externalToken,
  });

  if (req.audit) {
    await req.audit({
      action: "security",
      message: "External: Decision Kyc",
      description: `Usuário ${req.user?.name} acessou dados de compliance`,
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
