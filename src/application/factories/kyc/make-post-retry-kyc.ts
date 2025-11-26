import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";
import { PostRetryKycUseCase } from "@/application/use-cases/kyc/post-retry-kyc";

export function makePostRetryKycUseCase() {
  const service = new ExternalKycService();
  return new PostRetryKycUseCase(service);
}
