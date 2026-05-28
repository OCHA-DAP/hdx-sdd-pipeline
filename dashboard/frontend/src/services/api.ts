const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000';

export const getApiUrl = (endpoint: string) => {
  const base = API_BASE_URL.replace(/\/$/, "");
  const path = endpoint.replace(/^\//, "");
  return `${base}/${path}`;
};
