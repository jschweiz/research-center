import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "react-router-dom";

import { configureApiClient } from "./api/client";
import { queryClient } from "./lib/query-client";
import { loadRuntimeConfig } from "./runtime/config";
import type { RuntimeConfig } from "./runtime/types";
import "./styles/index.css";
import "./styles/published-viewer.css";

const rootElement = document.getElementById("root");

if (!rootElement) {
  throw new Error("Root element #root was not found.");
}

const root = ReactDOM.createRoot(rootElement);

async function renderApp(config: RuntimeConfig) {
  if (__PUBLISHED_ONLY__) {
    const { HostedViewerApp } = await import("./hosted/HostedViewerApp");
    return <HostedViewerApp config={config} />;
  }

  if (config.mode === "hosted") {
    const { HostedViewerApp } = await import("./hosted/HostedViewerApp");
    return <HostedViewerApp config={config} />;
  }

  if (config.mode === "local") {
    const { LocalControlApp } = await import("./local-control/LocalControlApp");
    return <LocalControlApp config={config} />;
  }

  const { router } = await import("./routes/router");
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

async function bootstrap() {
  const config = await loadRuntimeConfig();

  if (!__PUBLISHED_ONLY__ && config.mode !== "hosted") {
    configureApiClient(config.apiBaseUrl ?? "/api");
  }

  const app = await renderApp(config);
  root.render(<React.StrictMode>{app}</React.StrictMode>);
}

void bootstrap().catch((error) => {
  console.error("Failed to bootstrap Research Center.", error);
  root.render(
    <React.StrictMode>
      <div className="loading-screen">App bootstrap failed. Reload the page and check the runtime config.</div>
    </React.StrictMode>,
  );
});
