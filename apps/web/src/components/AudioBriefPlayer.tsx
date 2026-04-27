import type { ChangeEvent } from "react";
import { useEffect, useId, useRef, useState } from "react";
import { FileText, LoaderCircle, Pause, Play } from "lucide-react";
import clsx from "clsx";

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

function audioUnavailableMessage() {
  if (typeof navigator !== "undefined" && navigator.onLine === false) {
    return "Audio is unavailable offline right now. Reconnect and try again.";
  }
  return "The audio endpoint could not be reached.";
}

export function AudioBriefPlayer({
  briefDate,
  audioBrief,
  audioUrl,
  audioRequestInit,
  mode = "default",
  showChapters = false,
}: {
  briefDate: string;
  audioBrief: AudioBrief;
  audioUrl?: string | null;
  audioRequestInit?: RequestInit;
  mode?: "default" | "published";
  showChapters?: boolean;
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
  const chapters = [...audioBrief.chapters]
    .filter((chapter) => Number.isFinite(chapter.offset_seconds))
    .sort((left, right) => left.offset_seconds - right.offset_seconds);

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
      const resolvedAudioUrl = audioUrl ?? audioBrief.audio_url ?? api.getAudioSummaryUrl(briefDate);
      const requestInit =
        audioUrl || audioBrief.audio_url
          ? audioRequestInit
          : {
              ...audioRequestInit,
              credentials: audioRequestInit?.credentials ?? "include",
            };
      const response = await fetch(resolvedAudioUrl, requestInit);
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
          ? audioUnavailableMessage()
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

  const seekToChapter = async (offsetSeconds: number) => {
    const audio = audioRef.current;
    if (!audio || audioBrief.status !== "succeeded") return;
    const source = await ensureAudioLoaded();
    if (audio.src !== source) audio.src = source;
    audio.currentTime = offsetSeconds;
    setCurrentTime(offsetSeconds);
    if (audio.paused) {
      await audio.play();
    }
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
      <div className={clsx("audio-brief-unavailable", mode === "published" && "audio-brief-unavailable-published")}>
        {audioBrief.error ?? "Generate the voice summary to create a playable audio brief."}
      </div>
    );
  }

  return (
    <div className={clsx("audio-brief-player", mode === "published" && "audio-brief-player-published")}>
      <div className="audio-brief-meta-row">
        <div className="audio-brief-copy">
          <p className="audio-brief-eyebrow">
            Audio brief
            {audioBrief.provider ? ` / ${audioBrief.provider}` : ""}
            {audioBrief.voice ? ` / ${audioBrief.voice}` : ""}
          </p>
          <p className="audio-brief-helper">
            {showChapters && chapters.length
              ? `${chapters.length} chapter markers. Jump directly to the story you want.`
              : "Listen from the top or scrub through the narrated edition."}
          </p>
        </div>

        <div className="audio-brief-meta-actions">
          {transcript ? (
            <button
              aria-controls={transcriptId}
              aria-expanded={isTranscriptOpen}
              aria-haspopup="dialog"
              className="audio-transcript-toggle"
              onClick={() => setIsTranscriptOpen((open) => !open)}
              ref={transcriptButtonRef}
              type="button"
            >
              <FileText className="h-3.5 w-3.5 shrink-0" />
              <span>Transcript</span>
            </button>
          ) : null}

          <p className="audio-brief-clock">
            {formatClock(currentTime)} / {formatClock(totalDuration)}
          </p>
        </div>
      </div>

      <div className="audio-brief-control-row">
        <button
          className={clsx("audio-brief-play-button", isPlaying && "audio-brief-play-button-active")}
          disabled={isLoading}
          onClick={() => {
            void togglePlayback();
          }}
          type="button"
        >
          {isLoading ? <LoaderCircle className="h-5 w-5 animate-spin" /> : isPlaying ? <Pause className="h-5 w-5" /> : <Play className="h-5 w-5" />}
        </button>

        <div className="audio-brief-progress-stack">
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
          <div className="audio-brief-progress-meta">
            <span>{isPlaying ? "Now listening" : "Tap play to begin"}</span>
            <span>{chapters.length ? `${chapters.length} chapters` : "Narrated edition"}</span>
          </div>
        </div>
      </div>

      {loadError ? <p className="audio-brief-error">{loadError}</p> : null}

      {showChapters && chapters.length ? (
        <div className="audio-brief-chapter-list">
          {chapters.map((chapter) => (
            <button
              className="audio-brief-chapter"
              key={`${chapter.item_id}-${chapter.offset_seconds}`}
              onClick={() => {
                void seekToChapter(chapter.offset_seconds);
              }}
              type="button"
            >
              <span className="audio-brief-chapter-time">{formatClock(chapter.offset_seconds)}</span>
              <span className="audio-brief-chapter-copy">
                <span className="audio-brief-chapter-section">{chapter.section}</span>
                <span className="audio-brief-chapter-title">{chapter.headline || chapter.item_title}</span>
              </span>
            </button>
          ))}
        </div>
      ) : null}

      {isTranscriptOpen ? (
        <>
          <button
            aria-label="Close transcript"
            className="audio-transcript-backdrop"
            onClick={() => setIsTranscriptOpen(false)}
            type="button"
          />
          <div aria-label="Audio transcript" className="audio-transcript-panel" id={transcriptId} ref={transcriptPanelRef} role="dialog">
            <div className="audio-transcript-header">
              <div>
                <p className="audio-brief-eyebrow">Transcript</p>
                <h4 className="audio-transcript-title">Read along with the brief</h4>
              </div>
              <button className="ghost-button" onClick={() => setIsTranscriptOpen(false)} type="button">
                Close
              </button>
            </div>
            <p className="audio-transcript-body">{transcript}</p>
          </div>
        </>
      ) : null}

      <audio ref={audioRef} preload="metadata" />
    </div>
  );
}
