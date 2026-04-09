import { type ReactNode, useEffect, useMemo, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Compass, LibraryBig, Newspaper, PanelLeftClose, PanelLeftOpen, Settings2, UserCircle2, Workflow } from "lucide-react";

import { ApiError, api } from "../api/client";
import { LoginScreen } from "../components/LoginScreen";
import { SkimmableText } from "../components/SkimmableText";
import { getStoredShellSidebarCollapsed, setStoredShellSidebarCollapsed } from "../runtime/storage";

const navItems = [
  { label: "Brief", to: "/", icon: Newspaper },
  { label: "Inbox", to: "/inbox", icon: Compass },
  { label: "Connections", to: "/connections", icon: Settings2 },
  { label: "Pipeline", to: "/pipeline", icon: Workflow },
  { label: "Profile", to: "/profile", icon: UserCircle2 },
];

export type AppShellOutletContext = {
  setHeaderActions: (actions: ReactNode | null) => void;
};

export function AppShell() {
  const [loginError, setLoginError] = useState<string | null>(null);
  const [headerActions, setHeaderActions] = useState<ReactNode | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(getStoredShellSidebarCollapsed);
  const queryClient = useQueryClient();
  const location = useLocation();

  const meQuery = useQuery({
    queryKey: ["me"],
    queryFn: api.me,
    retry: false,
  });

  const login = useMutation({
    mutationFn: ({ email, password }: { email: string; password: string }) => api.login(email, password),
    onSuccess: async () => {
      setLoginError(null);
      await queryClient.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (error: ApiError) => setLoginError(error.message),
  });

  const logout = useMutation({
    mutationFn: api.logout,
    onSuccess: async () => {
      queryClient.clear();
      await queryClient.invalidateQueries({ queryKey: ["me"] });
    },
  });

  const title = useMemo(() => {
    if (location.pathname.startsWith("/inbox")) return "Inbox";
    if (location.pathname.startsWith("/connections")) return "Connections";
    if (location.pathname.startsWith("/pipeline")) return "Pipeline";
    if (location.pathname.startsWith("/profile")) return "Interest Profile";
    if (location.pathname.startsWith("/items/")) return "Item Detail";
    return "Morning Brief";
  }, [location.pathname]);

  useEffect(() => {
    setHeaderActions(null);
  }, [location.pathname]);

  useEffect(() => {
    setStoredShellSidebarCollapsed(sidebarCollapsed);
  }, [sidebarCollapsed]);

  if (meQuery.isLoading) {
    return <div className="loading-screen">Loading Research Center…</div>;
  }

  if (meQuery.error instanceof ApiError && meQuery.error.status === 401) {
    return (
      <LoginScreen
        busy={login.isPending}
        error={loginError}
        onSubmit={async (email, password) => {
          await login.mutateAsync({ email, password });
        }}
      />
    );
  }

  if (meQuery.error) {
    return <div className="loading-screen">Backend unavailable. Check the API service and retry.</div>;
  }

  const SidebarToggleIcon = sidebarCollapsed ? PanelLeftOpen : PanelLeftClose;

  return (
    <div className="min-h-screen bg-[var(--paper)] text-[var(--ink)]">
      <div
        className="app-grid mx-auto min-h-screen max-w-[1680px] px-4 py-4 sm:px-6 lg:px-8"
        data-sidebar-collapsed={sidebarCollapsed ? "true" : "false"}
      >
        <aside className="editorial-sidebar">
          <div className="editorial-sidebar-header">
            <div className="editorial-sidebar-heading">
              <p className="editorial-sidebar-kicker font-mono text-[11px] uppercase tracking-[0.35em] text-[var(--muted)]">Research Center</p>
              <span
                aria-hidden="true"
                className="editorial-sidebar-monogram font-mono text-[11px] uppercase tracking-[0.3em] text-[var(--muted)]"
              >
                RC
              </span>
              <button
                aria-expanded={!sidebarCollapsed}
                aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                className="secondary-button h-11 w-11 shrink-0 justify-center p-0"
                onClick={() => setSidebarCollapsed((current) => !current)}
                title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                type="button"
              >
                <SidebarToggleIcon className="h-4 w-4" />
              </button>
            </div>

            <div className="editorial-sidebar-copy space-y-3">
              <h1 className="font-display text-5xl leading-none text-[var(--ink)]">Research cockpit</h1>
              <SkimmableText className="text-sm leading-6 text-[var(--muted)]">
                Editorial briefings, ranked signals, and Zotero triage in one surface.
              </SkimmableText>
            </div>
          </div>

          <nav className="editorial-sidebar-nav mt-10 space-y-2">
            {navItems.map(({ label, to, icon: Icon }) => (
              <NavLink
                aria-label={label}
                key={to}
                className={({ isActive }) =>
                  `nav-link ${isActive ? "nav-link-active" : ""}`
                }
                title={label}
                to={to}
              >
                <Icon className="h-4 w-4" />
                <span className="nav-link-label">{label}</span>
              </NavLink>
            ))}
          </nav>

          <section className="editorial-panel editorial-sidebar-panel mt-10 bg-[rgba(255,255,255,0.58)]">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="font-mono text-[11px] uppercase tracking-[0.24em] text-[var(--muted)]">Managed Account</p>
                <p className="mt-2 text-sm font-medium text-[var(--ink)]">{meQuery.data?.email}</p>
              </div>
              <LibraryBig className="h-5 w-5 text-[var(--accent)]" />
            </div>
            <button className="secondary-button mt-5 w-full justify-center" onClick={() => logout.mutate()} type="button">
              Sign out
            </button>
          </section>
        </aside>

        <main className="editorial-main">
          <header className="editorial-topbar">
            <div className="min-w-0 flex-1">
              <p className="font-mono text-[11px] uppercase tracking-[0.26em] text-[var(--muted)]">Today / {new Date().toLocaleDateString()}</p>
              <h2 className="mt-3 max-w-full font-display text-4xl leading-[0.94] sm:text-5xl">{title}</h2>
            </div>
            {headerActions ? <div className="w-full max-w-[360px] lg:ml-auto">{headerActions}</div> : null}
          </header>
          <Outlet context={{ setHeaderActions } satisfies AppShellOutletContext} />
        </main>
      </div>
    </div>
  );
}
