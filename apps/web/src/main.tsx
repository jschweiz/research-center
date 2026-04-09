import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "react-router-dom";

import { configureApiClient } from "./api/client";
import { HostedViewerApp } from "./hosted/HostedViewerApp";
import { queryClient } from "./lib/query-client";
import { LocalControlApp } from "./local-control/LocalControlApp";
import { router } from "./routes/router";
import { loadRuntimeConfig } from "./runtime/config";
import type { RuntimeConfig } from "./runtime/types";
import "./styles/index.css";

const rootElement = document.getElementById("root");

if (!rootElement) {
  throw new Error("Root element #root was not found.");
}

const root = ReactDOM.createRoot(rootElement);

function renderApp(config: RuntimeConfig) {
  if (config.mode === "hosted") {
    return <HostedViewerApp config={config} />;
  }

  if (config.mode === "local") {
    return <LocalControlApp config={config} />;
  }

  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

async function bootstrap() {
  const config = await loadRuntimeConfig();

  if (config.mode !== "hosted") {
    configureApiClient(config.apiBaseUrl ?? "/api");
  }

  root.render(<React.StrictMode>{renderApp(config)}</React.StrictMode>);
}

void bootstrap().catch((error) => {
  console.error("Failed to bootstrap Research Center.", error);
  root.render(
    <React.StrictMode>
      <div className="loading-screen">App bootstrap failed. Reload the page and check the runtime config.</div>
    </React.StrictMode>,
  );
});
