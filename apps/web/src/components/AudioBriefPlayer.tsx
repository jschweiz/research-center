import type { ChangeEvent } from "react";
import { useEffect, useId, useRef, useState } from "react";
import { FileText, LoaderCircle, Pause, Play } from "lucide-react";

import { api } from "../api/client";
import type { AudioBrief } from "../api/types";

function formatClock(seconds: number | null | undefined) {
  if (!seconds || seconds < 0) return "0:00";
  const totalSeconds = Math.floor(seconds);
  const minutes = Math.floor(totalSeconds / 60);
  const remainder = totalSeconds % 60;
  return `${minutes}:${String(remainder).padStart(2, "0")}`;
}

async function readAudioError(response: Response) {
  const text = await response.text();
  if (!text) return "Audio brief could not be loaded.";

  try {
    const payload = JSON.parse(text) as { detail?: string; message?: string };
    if (typeof payload.detail === "string" && payload.detail.trim()) return payload.detail;
    if (typeof payload.message === "string" && payload.message.trim()) return payload.message;
  } catch {
    return text;
  }

  return text;
}

export function AudioBriefPlayer({
  briefDate,
  audioBrief,
}: {
  briefDate: string;
  audioBrief: AudioBrief;
}) {
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const objectUrlRef = useRef<string | null>(null);
  const transcriptButtonRef = useRef<HTMLButtonElement | null>(null);
  const transcriptPanelRef = useRef<HTMLDivElement | null>(null);
  const [audioSrc, setAudioSrc] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [isTranscriptOpen, setIsTranscriptOpen] = useState(false);
  const transcriptId = useId();

  const totalDuration = duration || audioBrief.audio_duration_seconds || audioBrief.estimated_duration_seconds || 0;
  const transcript = audioBrief.script?.trim() ?? "";

  useEffect(() => {
    const audio = audioRef.current;
    if (!audio) return undefined;

    const syncTime = () => setCurrentTime(audio.currentTime);
    const syncDuration = () => setDuration(Number.isFinite(audio.duration) ? audio.duration : 0);
    const onPlay = () => setIsPlaying(true);
    const onPause = () => setIsPlaying(false);
    const onEnded = () => {
      setIsPlaying(false);
      setCurrentTime(0);
    };

    audio.addEventListener("timeupdate", syncTime);
    audio.addEventListener("loadedmetadata", syncDuration);
    audio.addEventListener("durationchange", syncDuration);
    audio.addEventListener("play", onPlay);
    audio.addEventListener("pause", onPause);
    audio.addEventListener("ended", onEnded);

    return () => {
      audio.removeEventListener("timeupdate", syncTime);
      audio.removeEventListener("loadedmetadata", syncDuration);
      audio.removeEventListener("durationchange", syncDuration);
      audio.removeEventListener("play", onPlay);
      audio.removeEventListener("pause", onPause);
      audio.removeEventListener("ended", onEnded);
    };
  }, []);

  useEffect(() => {
    const audio = audioRef.current;
    if (audio) {
      audio.pause();
      audio.removeAttribute("src");
      audio.load();
    }
    setCurrentTime(0);
    setDuration(0);
    setIsPlaying(false);
    setIsTranscriptOpen(false);
    setLoadError(null);
    setAudioSrc((previous) => {
      if (previous) URL.revokeObjectURL(previous);
      return null;
    });
    objectUrlRef.current = null;

    return () => {
      if (objectUrlRef.current) {
        URL.revokeObjectURL(objectUrlRef.current);
        objectUrlRef.current = null;
      }
    };
  }, [briefDate, audioBrief.generated_at]);

  useEffect(() => {
    if (!isTranscriptOpen) return undefined;

    const closeTranscript = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (transcriptButtonRef.current?.contains(target)) return;
      if (transcriptPanelRef.current?.contains(target)) return;
      setIsTranscriptOpen(false);
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setIsTranscriptOpen(false);
      transcriptButtonRef.current?.focus();
    };

    document.addEventListener("mousedown", closeTranscript);
    document.addEventListener("keydown", handleEscape);

    return () => {
      document.removeEventListener("mousedown", closeTranscript);
      document.removeEventListener("keydown", handleEscape);
    };
  }, [isTranscriptOpen]);

  const ensureAudioLoaded = async () => {
    if (audioSrc) return audioSrc;
    setIsLoading(true);
    setLoadError(null);
    try {
      const audioUrl = audioBrief.audio_url ?? api.getAudioSummaryUrl(briefDate);
      const requestInit = audioBrief.audio_url
        ? undefined
        : {
            credentials: "include" as const,
          };
      const response = await fetch(audioUrl, requestInit);
      if (!response.ok) {
        const message = await readAudioError(response);
        throw new Error(message || "Audio brief could not be loaded.");
      }
      const blob = await response.blob();
      const nextUrl = URL.createObjectURL(blob);
      if (objectUrlRef.current) URL.revokeObjectURL(objectUrlRef.current);
      objectUrlRef.current = nextUrl;
      setAudioSrc(nextUrl);
      return nextUrl;
    } catch (error) {
      const message =
        error instanceof TypeError && error.message === "Failed to fetch"
          ? "The audio endpoint could not be reached."
          : error instanceof Error
            ? error.message
            : "Audio brief could not be loaded.";
      setLoadError(message);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  const togglePlayback = async () => {
    const audio = audioRef.current;
    if (!audio || audioBrief.status !== "succeeded") return;
    if (audio.paused) {
      const source = await ensureAudioLoaded();
      if (audio.src !== source) audio.src = source;
      await audio.play();
      return;
    }
    audio.pause();
  };

  const handleSeek = (event: ChangeEvent<HTMLInputElement>) => {
    const nextTime = Number(event.target.value);
    setCurrentTime(nextTime);
    if (audioRef.current) {
      audioRef.current.currentTime = nextTime;
    }
  };

  if (audioBrief.status !== "succeeded") {
    return (
      <div className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-3 text-sm text-[var(--muted)]">
        {audioBrief.error ?? "Generate the voice summary to create a playable audio brief."}
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
            Audio summary {audioBrief.provider ? ` / ${audioBrief.provider}` : ""} {audioBrief.voice ? ` / ${audioBrief.voice}` : ""}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {transcript ? (
            <div
              className="relative"
              onBlur={(event) => {
                const nextTarget = event.relatedTarget;
                if (nextTarget instanceof Node && event.currentTarget.contains(nextTarget)) return;
                setIsTranscriptOpen(false);
              }}
            >
              <button
                aria-controls={transcriptId}
                aria-expanded={isTranscriptOpen}
                aria-haspopup="dialog"
                className="inline-flex items-center gap-2 rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.7)] px-3 py-1.5 text-[var(--muted)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/26 hover:text-[var(--accent)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18"
                onClick={() => setIsTranscriptOpen((open) => !open)}
                ref={transcriptButtonRef}
                type="button"
              >
                <FileText className="h-3.5 w-3.5 shrink-0" />
                <span className="font-mono text-[11px] uppercase tracking-[0.18em]">Transcript</span>
              </button>

              {isTranscriptOpen ? (
                <div
                  aria-label="Audio transcript"
                  className="absolute right-0 top-[calc(100%+0.65rem)] z-20 w-80 max-w-[calc(100vw-2.5rem)] rounded-[1.35rem] border border-[var(--ink)]/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,240,224,0.92))] p-4 shadow-[0_24px_64px_rgba(17,19,18,0.16)] backdrop-blur-md"
                  id={transcriptId}
                  ref={transcriptPanelRef}
                >
                  <p className="font-mono text-[10px] uppercase tracking-[0.24em] text-[var(--muted)]">
                    Transcript
                  </p>
                  <p className="mt-2 max-h-72 overflow-y-auto pr-2 text-sm leading-6 whitespace-pre-line text-[var(--muted-strong)]">
                    {transcript}
                  </p>
                </div>
              ) : null}
            </div>
          ) : null}

          <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
            {formatClock(currentTime)} / {formatClock(totalDuration)}
          </p>
        </div>
      </div>

      <div className="flex items-center gap-3 rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3 py-3">
        <button
          className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-[var(--ink)] text-[var(--paper)] transition hover:translate-y-[-1px] hover:shadow-[0_10px_24px_rgba(17,19,18,0.14)] disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:translate-y-0 disabled:hover:shadow-none"
          disabled={isLoading}
          onClick={() => {
            void togglePlayback();
          }}
          type="button"
        >
          {isLoading ? <LoaderCircle className="h-4 w-4 animate-spin" /> : isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
        </button>

        <input
          aria-label="Audio progress"
          className="audio-progress min-w-0 flex-1"
          max={Math.max(totalDuration, 0.1)}
          min={0}
          onChange={handleSeek}
          step={0.1}
          type="range"
          value={Math.min(currentTime, totalDuration || 0)}
        />
      </div>

      {loadError ? <p className="text-sm text-[var(--danger)]">{loadError}</p> : null}

      <audio ref={audioRef} preload="metadata" />
    </div>
  );
}
