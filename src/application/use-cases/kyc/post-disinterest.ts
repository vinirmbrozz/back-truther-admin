import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";

interface Input {
  document: string;
  internalComent: string;
  externalComent?: string;
  reason: string;
  externalToken: string;
}

export class PostDisinterestUseCase {
  constructor(private externalService: ExternalKycService) {}

  async execute(input: Input) {
    const { document, internalComent, externalComent, reason, externalToken } =
      input;

    const listResponse = await this.externalService.listUsers(
      document,
      externalToken
    );

    if (listResponse.status !== 200 || !listResponse.data?.res?.data) {
      return {
        status: listResponse.status,
        data: { error: "USER_NOT_FOUND", details: listResponse.data },
      };
    }

    const uuid = listResponse.data.res.data[0].id;

    return this.externalService.fetchDisinterest(
      {
        uuid: uuid,
        internalComent,
        externalComent,
        reason,
      },
      externalToken
    );
  }
}
