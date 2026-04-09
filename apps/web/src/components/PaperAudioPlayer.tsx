import { useEffect, useState } from "react";
import { Headphones, SquareArrowOutUpRight } from "lucide-react";

type AudioAvailability = "checking" | "ready" | "missing";

export function PaperAudioPlayer({
  audioUrl,
  variant = "default",
}: {
  audioUrl: string;
  variant?: "default" | "compact";
}) {
  const [availability, setAvailability] = useState<AudioAvailability>("checking");

  useEffect(() => {
    const normalizedUrl = audioUrl.trim();
    if (!normalizedUrl) {
      setAvailability("missing");
      return;
    }

    setAvailability("checking");

    const probe = document.createElement("audio");
    let active = true;

    const cleanup = () => {
      probe.removeEventListener("loadedmetadata", handleReady);
      probe.removeEventListener("canplay", handleReady);
      probe.removeEventListener("error", handleMissing);
      probe.pause();
      probe.removeAttribute("src");
      probe.load();
    };

    const handleReady = () => {
      if (!active) return;
      setAvailability("ready");
      cleanup();
    };

    const handleMissing = () => {
      if (!active) return;
      setAvailability("missing");
      cleanup();
    };

    probe.preload = "metadata";
    probe.addEventListener("loadedmetadata", handleReady);
    probe.addEventListener("canplay", handleReady);
    probe.addEventListener("error", handleMissing);
    probe.src = normalizedUrl;
    probe.load();

    return () => {
      active = false;
      cleanup();
    };
  }, [audioUrl]);

  if (availability !== "ready") return null;

  if (variant === "compact") {
    return (
      <section className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="content-label">Paper audio</p>
            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Listen inline while reviewing the enrichment.</p>
          </div>
          <a
            className="inline-flex items-center gap-2 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)] transition hover:text-[var(--accent)]"
            href={audioUrl}
            rel="noreferrer"
            target="_blank"
          >
            <SquareArrowOutUpRight className="h-3.5 w-3.5" />
            Open
          </a>
        </div>
        <div className="mt-3 rounded-[1.15rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.7)] px-3 py-3">
          <div className="mb-2 flex items-center gap-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--muted-strong)]">
            <Headphones className="h-3.5 w-3.5 text-[var(--accent)]" />
            Audio ready
          </div>
          <audio className="block w-full" controls preload="metadata" src={audioUrl} onError={() => setAvailability("missing")}>
            Your browser does not support audio playback.
          </audio>
        </div>
      </section>
    );
  }

  return (
    <section className="rounded-[1.7rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.46)] px-5 py-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="content-label">Paper audio</p>
        <a
          className="inline-flex items-center gap-2 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)] hover:text-[var(--accent)]"
          href={audioUrl}
          rel="noreferrer"
          target="_blank"
        >
          <SquareArrowOutUpRight className="h-4 w-4" />
          Open source audio
        </a>
      </div>
      <div className="mt-4 flex items-center gap-3 rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4">
        <Headphones className="h-4 w-4 text-[var(--accent)]" />
        <audio className="min-w-0 flex-1" controls preload="metadata" src={audioUrl} onError={() => setAvailability("missing")}>
          Your browser does not support audio playback.
        </audio>
      </div>
    </section>
  );
}
