import { ExternalKycService } from "@/application/services/kyc/ExternalKycService";
import { GetKycDataUserUseCase } from "@/application/use-cases/kyc/get-kyc-data-user";

export function makeGetKycDataUserUseCase() {
  const service = new ExternalKycService();
  return new GetKycDataUserUseCase(service);
}
