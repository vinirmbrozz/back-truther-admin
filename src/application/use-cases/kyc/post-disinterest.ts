import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";

interface Input {
  uuid: string;
  internalComent: string;
  externalComent?: string;
  reason: string;
  externalToken: string;
}

export class PostDisinterestUseCase {
  constructor(private externalService: ExternalKycService) {}

  async execute(input: Input) {
    const { uuid, internalComent, externalComent, reason, externalToken } = input;

    const detail = await this.externalService.fetchUserKycData(uuid, externalToken);

    if (detail.status !== 200 || !detail.data?.res?.data) {
      return { status: detail.status, data: { error: "USER_NOT_FOUND", details: detail.data } };
    }

    const realUuid = detail.data.res.data.id ?? uuid;

    return this.externalService.fetchDisinterest(
      {
        uuid: realUuid,
        internalComent,
        externalComent,
        reason,
      },
      externalToken
    );
  }
}
