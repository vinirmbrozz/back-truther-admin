import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";
import { PostDecisionKycUseCase } from "@/application/use-cases/kyc/post-decision-kyc";

export function makePostDecisionKycUseCase() {
  const service = new ExternalKycService();
  return new PostDecisionKycUseCase(service);
}
