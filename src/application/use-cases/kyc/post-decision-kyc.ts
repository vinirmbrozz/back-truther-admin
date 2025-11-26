import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";

interface Input {
  decision: boolean;
  internalComent: string | null;
  levelKyc: string;
  document: string;
  externalToken: string;
}

export class PostDecisionKycUseCase {
  constructor(private externalService: ExternalKycService) {}

  async execute(input: Input) {
    const { decision, internalComent, levelKyc, document, externalToken } = input;

    const listResponse = await this.externalService.listUsers(document, externalToken);

    if (listResponse.status !== 200 || !listResponse.data?.res?.data) {
      return {
        status: listResponse.status,
        data: { error: "USER_NOT_FOUND", details: listResponse.data },
      };
    }

    const uuid = listResponse.data.res.data[0].id;

    return this.externalService.fetchDecisionKyc(
      {
        decision,
        internalComent,
        levelKyc,
        uuid: uuid,
      },
      externalToken
    );
  }
}
