import { Suspense, lazy, type ReactNode } from "react";
import { createBrowserRouter } from "react-router-dom";

import { AppShell } from "../layout/AppShell";

const BriefPage = lazy(async () => ({
  default: (await import("../features/brief/BriefPage")).BriefPage,
}));
const InboxPage = lazy(async () => ({
  default: (await import("../features/inbox/InboxPage")).InboxPage,
}));
const ItemDetailPage = lazy(async () => ({
  default: (await import("../features/item/ItemDetailPage")).ItemDetailPage,
}));
const ProfilePage = lazy(async () => ({
  default: (await import("../features/profile/ProfilePage")).ProfilePage,
}));
const ConnectionsPage = lazy(async () => ({
  default: (await import("../features/settings/ConnectionsPage")).ConnectionsPage,
}));

function withRouteSuspense(element: ReactNode) {
  return <Suspense fallback={<div className="page-loading">Loading…</div>}>{element}</Suspense>;
}

export const router = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      { index: true, element: withRouteSuspense(<BriefPage />) },
      { path: "inbox", element: withRouteSuspense(<InboxPage />) },
      { path: "items/:itemId", element: withRouteSuspense(<ItemDetailPage />) },
      { path: "connections", element: withRouteSuspense(<ConnectionsPage />) },
      { path: "profile", element: withRouteSuspense(<ProfilePage />) },
    ],
  },
]);
