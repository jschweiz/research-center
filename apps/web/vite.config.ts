import { VitePWA } from "vite-plugin-pwa";
import react from "@vitejs/plugin-react-swc";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    VitePWA({
      registerType: "autoUpdate",
      includeAssets: ["icon.svg"],
      manifest: {
        name: "Research Center",
        short_name: "Research Center",
        description: "Editorial daily research briefing for iPad-first review and Zotero triage.",
        theme_color: "#ece4d3",
        background_color: "#ece4d3",
        display: "standalone",
        start_url: "/",
        icons: [
          {
            src: "/icon.svg",
            sizes: "512x512",
            type: "image/svg+xml",
            purpose: "any maskable"
          }
        ]
      }
    })
  ],
  server: {
    port: 5173
  }
});
