const configuredBackendURL = (import.meta.env.VITE_API_BASE_URL || "").trim();

if (configuredBackendURL) {
  let parsedBackendURL;
  try {
    parsedBackendURL = new URL(configuredBackendURL);
  } catch {
    throw new Error("VITE_API_BASE_URL must be an absolute backend URL.");
  }
  if (!["http:", "https:"].includes(parsedBackendURL.protocol)) {
    throw new Error("VITE_API_BASE_URL must use http:// or https://.");
  }
  if (parsedBackendURL.pathname !== "/" || parsedBackendURL.search || parsedBackendURL.hash) {
    throw new Error("VITE_API_BASE_URL must be the backend origin without a path or query.");
  }
}

export const API_BASE_URL = configuredBackendURL.replace(/\/+$/, "");

export function apiURL(path) {
  return `${API_BASE_URL}${path}`;
}

export function webSocketURL() {
  const backendURL = new URL(API_BASE_URL || window.location.origin);
  backendURL.protocol = backendURL.protocol === "https:" ? "wss:" : "ws:";
  backendURL.pathname = "/ws";
  backendURL.search = "";
  backendURL.hash = "";
  return backendURL.toString();
}
