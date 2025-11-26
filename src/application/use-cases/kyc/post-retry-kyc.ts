import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";

interface Input {
  uuid: string;
  ouccurenceuuid?: string;
  internalComent: string;
  tryKycType: string;
  externalToken: string;
}

export class PostRetryKycUseCase {
  constructor(private externalService: ExternalKycService) {}

  async execute(input: Input) {
    const { uuid, ouccurenceuuid, internalComent, tryKycType, externalToken } = input;

    const detail = await this.externalService.fetchUserKycData(uuid, externalToken);

    if (detail.status !== 200 || !detail.data?.res?.data) {
      return { status: detail.status, data: { error: "USER_NOT_FOUND", details: detail.data } };
    }

    const realUuid = detail.data.res.data.id ?? uuid;

    return this.externalService.fetchRetryKyc(
      {
        uuid: realUuid,
        ouccurenceuuid,
        internalComent,
        tryKycType,
      },
      externalToken
    );
  }
}
