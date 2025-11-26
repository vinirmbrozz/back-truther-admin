import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";

interface Input {
  decision: boolean;
  internalComent: string | null;
  levelKyc: string;
  uuid: string;
  externalToken: string;
}

export class PostDecisionKycUseCase {
  constructor(private externalService: ExternalKycService) {}

  async execute(input: Input) {
    const { decision, internalComent, levelKyc, uuid, externalToken } = input;

    // Buscar UUID real via list-users (mesma lógica do exemplo)
    const detail = await this.externalService.fetchUserKycData(uuid, externalToken);

    if (detail.status !== 200 || !detail.data?.res?.data) {
      return {
        status: detail.status,
        data: { error: "USER_NOT_FOUND", details: detail.data },
      };
    }

    const realUuid = detail.data.res.data.id ?? uuid;

    return this.externalService.fetchDecisionKyc(
      {
        decision,
        internalComent,
        levelKyc,
        uuid: realUuid,
      },
      externalToken
    );
  }
}
