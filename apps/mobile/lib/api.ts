export type ContractorCard = {
  contract_no: string;
  seller_name?: string | null;
  seller_email?: string | null;
  seller_phone?: string | null;
  seller_gstin?: string | null;
  seller_address?: string | null;
  product_name?: string | null;
  contract_value?: number | null;
  list_date?: string | null;
  score?: number | null;
  quality_flags?: string[];
};

export type SearchResponse = {
  query: string;
  count: number;
  results: ContractorCard[];
};

const API_BASE_URL = process.env.EXPO_PUBLIC_API_BASE_URL || "http://localhost:8000";

function friendlyApiError(status: number, body: string): string {
  if (status === 503) {
    return "Service is temporarily unavailable. Check that the API and database are configured.";
  }
  if (status === 0 || body.toLowerCase().includes("network")) {
    return "Cannot reach the server. Check your connection and API URL.";
  }
  try {
    const parsed = JSON.parse(body) as { detail?: string };
    if (parsed.detail) {
      return parsed.detail;
    }
  } catch {
    /* plain text body */
  }
  return body || `Request failed (${status})`;
}

async function apiGet<T>(path: string): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${API_BASE_URL}${path}`);
  } catch {
    throw new Error("Cannot reach the server. Check your connection and API URL.");
  }
  if (!res.ok) {
    const body = await res.text();
    throw new Error(friendlyApiError(res.status, body));
  }
  return res.json() as Promise<T>;
}

export function searchContractors(query: string, limit = 20): Promise<SearchResponse> {
  const params = new URLSearchParams({
    q: query,
    limit: String(limit),
    mode: "fts",
  });
  return apiGet<SearchResponse>(`/search?${params.toString()}`);
}

export function getContractor(contractNo: string): Promise<ContractorCard> {
  return apiGet<ContractorCard>(`/contractors/${encodeURIComponent(contractNo)}`);
}
