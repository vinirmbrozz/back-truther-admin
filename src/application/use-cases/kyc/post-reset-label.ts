import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";

interface Input {
  document: string;
  labels: string;
  externalToken: string;
}

export class PostResetLabelUseCase {
  constructor(private externalService: ExternalKycService) {}

  async execute(input: Input) {
    const { document, labels, externalToken } = input;
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

    return this.externalService.fetchResetLabel(
      {
        uuid: uuid,
        labels,
      },
      externalToken
    );
  }
}
