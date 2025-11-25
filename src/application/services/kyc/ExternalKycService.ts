export class ExternalKycService {
  private baseUrl = process.env.SERVICE_PROXY_URL;

  private validateBaseUrl() {
    if (!this.baseUrl) {
      return { status: 500, data: { error: "SERVICE_PROXY_URL not configured" } };
    }
    return null;
  }

  async listUsers(document: string, token: string) {
    const invalid = this.validateBaseUrl();
    if (invalid) return invalid;

    try {
      const res = await fetch(
        `${this.baseUrl}/compliance/list-users?document=${encodeURIComponent(document)}&page=1`,
        {
          method: "GET",
          headers: {
            authorization: `Bearer ${token}`,
          },
        }
      );

      const json = await res.json().catch(() => null);

      return { status: res.status, data: json };
    } catch (err: any) {
      return {
        status: 500,
        data: {
          error: "external_request_failed",
          details: err?.message ?? "Unknown error",
        },
      };
    }
  }

  async fetchUserKycData(uuid: string, token: string) {
    if (!this.baseUrl) {
      return {
        status: 500,
        data: { error: "SERVICE_PROXY_URL not configured" },
      };
    }

    try {
      const res = await fetch(`${this.baseUrl}/compliance/data-user`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ uuid }),
      });

      const json = await res.json().catch(() => null);

      return {
        status: res.status,
        data: json,
      };
    } catch (err: any) {
      return {
        status: 500,
        data: {
          error: "external_request_failed",
          details: err?.message ?? "Unknown error",
        },
      };
    }
  }
}
