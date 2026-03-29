import { useEffect, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Star } from "lucide-react";
import clsx from "clsx";

import { api } from "../api/client";

interface ImportantButtonProps {
  itemId: string;
  starred?: boolean;
  iconOnly?: boolean;
  iconOnlySize?: "default" | "compact";
  disabled?: boolean;
}

export function ImportantButton({
  itemId,
  starred = false,
  iconOnly = false,
  iconOnlySize = "default",
  disabled = false,
}: ImportantButtonProps) {
  const queryClient = useQueryClient();
  const [optimisticStarred, setOptimisticStarred] = useState(starred);

  useEffect(() => {
    setOptimisticStarred(starred);
  }, [starred]);

  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["briefs"] }),
      queryClient.invalidateQueries({ queryKey: ["items"] }),
      queryClient.invalidateQueries({ queryKey: ["item", itemId] }),
    ]);
  };

  const star = useMutation({
    mutationFn: () => api.starItem(itemId),
    onMutate: () => {
      const previousStarred = optimisticStarred;
      setOptimisticStarred(!previousStarred);
      return { previousStarred };
    },
    onError: (_error, _variables, context) => {
      setOptimisticStarred(context?.previousStarred ?? starred);
    },
    onSuccess: refresh,
  });

  const buttonDisabled = disabled || star.isPending;
  const label = star.isPending ? "Updating..." : optimisticStarred ? "Important" : "Mark important";

  if (iconOnly) {
    const iconOnlyButtonClass =
      iconOnlySize === "compact"
        ? "flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.78)] text-[var(--muted)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/26 hover:text-[var(--accent)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18 disabled:cursor-not-allowed disabled:opacity-60"
        : "flex h-11 w-11 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.78)] text-[var(--muted)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/26 hover:text-[var(--accent)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18 disabled:cursor-not-allowed disabled:opacity-60";
    const iconOnlyIconClass = iconOnlySize === "compact" ? "h-[13px] w-[13px]" : "h-4 w-4";

    return (
      <button
        aria-label={label}
        aria-pressed={optimisticStarred}
        className={clsx(
          iconOnlyButtonClass,
          optimisticStarred &&
            "border-[#d97706]/28 bg-[rgba(245,158,11,0.16)] text-[#9a3412] shadow-[0_10px_24px_rgba(217,119,6,0.14)] hover:border-[#d97706]/38 hover:text-[#9a3412]",
        )}
        disabled={buttonDisabled}
        onClick={() => star.mutate()}
        title={label}
        type="button"
      >
        <Star className={clsx(iconOnlyIconClass, optimisticStarred && "fill-current text-[#d97706]")} />
        <span className="sr-only">{label}</span>
      </button>
    );
  }

  return (
    <button
      aria-pressed={optimisticStarred}
      className={clsx(
        "secondary-button",
        optimisticStarred &&
          "border-[#d97706]/28 bg-[rgba(245,158,11,0.16)] text-[#9a3412] shadow-[0_10px_24px_rgba(217,119,6,0.14)] hover:border-[#d97706]/38 hover:bg-[rgba(245,158,11,0.2)]",
      )}
      disabled={buttonDisabled}
      onClick={() => star.mutate()}
      type="button"
    >
      <Star className={clsx("h-4 w-4", optimisticStarred && "fill-current text-[#d97706]")} />
      {label}
    </button>
  );
}
