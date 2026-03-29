import { type ReactNode, useEffect, useMemo, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Compass, LibraryBig, Newspaper, Settings2, UserCircle2 } from "lucide-react";

import { ApiError, api } from "../api/client";
import { LoginScreen } from "../components/LoginScreen";
import { SkimmableText } from "../components/SkimmableText";

const navItems = [
  { label: "Brief", to: "/", icon: Newspaper },
  { label: "Inbox", to: "/inbox", icon: Compass },
  { label: "Connections", to: "/connections", icon: Settings2 },
  { label: "Profile", to: "/profile", icon: UserCircle2 },
];

export type AppShellOutletContext = {
  setHeaderActions: (actions: ReactNode | null) => void;
};

export function AppShell() {
  const [loginError, setLoginError] = useState<string | null>(null);
  const [headerActions, setHeaderActions] = useState<ReactNode | null>(null);
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
    if (location.pathname.startsWith("/connections")) return "Connections and sources";
    if (location.pathname.startsWith("/profile")) return "Interest Profile";
    if (location.pathname.startsWith("/items/")) return "Item Detail";
    return "Morning Brief";
  }, [location.pathname]);

  useEffect(() => {
    setHeaderActions(null);
  }, [location.pathname]);

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

  return (
    <div className="min-h-screen bg-[var(--paper)] text-[var(--ink)]">
      <div className="app-grid mx-auto min-h-screen max-w-[1680px] px-4 py-4 sm:px-6 lg:px-8">
        <aside className="editorial-sidebar">
          <div className="space-y-3">
            <p className="font-mono text-[11px] uppercase tracking-[0.35em] text-[var(--muted)]">Research Center</p>
            <h1 className="font-display text-5xl leading-none text-[var(--ink)]">Research cockpit</h1>
            <SkimmableText className="text-sm leading-6 text-[var(--muted)]">
              Editorial briefings, ranked signals, and Zotero triage in one surface.
            </SkimmableText>
          </div>

          <nav className="mt-10 space-y-2">
            {navItems.map(({ label, to, icon: Icon }) => (
              <NavLink
                key={to}
                className={({ isActive }) =>
                  `nav-link ${isActive ? "nav-link-active" : ""}`
                }
                to={to}
              >
                <Icon className="h-4 w-4" />
                {label}
              </NavLink>
            ))}
          </nav>

          <section className="editorial-panel mt-10 bg-[rgba(255,255,255,0.58)]">
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
