import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";
import { PostDisinterestUseCase } from "@/application/use-cases/kyc/post-disinterest";

export function makePostDisinterestUseCase() {
  const service = new ExternalKycService();
  return new PostDisinterestUseCase(service);
}
