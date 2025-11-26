import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";
import { PostResetLabelUseCase } from "@/application/use-cases/kyc/post-reset-label";

export function makePostResetLabelUseCase() {
  const service = new ExternalKycService();
  return new PostResetLabelUseCase(service);
}
