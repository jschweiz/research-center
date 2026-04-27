import { createContext, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { ChevronUp, FileText, LoaderCircle, Pause, Play, Radio, X } from "lucide-react";
import { Link } from "react-router-dom";
import clsx from "clsx";

import { api } from "../api/client";
import type { AudioBrief, AudioBriefChapter } from "../api/types";
import type { PublishedEditionManifest, PublishedItemDetail } from "../runtime/types";

type HostedEditionAudio = {
  audioBrief: AudioBrief | null;
  audioGeneratedAt: string | null;
  briefDate: string;
  items: Record<string, PublishedItemDetail>;
  recordName: string;
  title: string;
};

type HostedAudioContextValue = {
  activeChapter: AudioBriefChapter | null;
  chapterCount: number;
  currentTime: number;
  edition: HostedEditionAudio | null;
  hasPlayableAudio: boolean;
  isLoading: boolean;
  isPlaying: boolean;
  isSheetOpen: boolean;
  loadError: string | null;
  progressRatio: number;
  registerManifest: (manifest: PublishedEditionManifest) => void;
  seek: (nextTime: number) => void;
  seekToChapter: (offsetSeconds: number) => Promise<void>;
  setSheetOpen: (open: boolean) => void;
  pause: () => void;
  togglePlayback: () => Promise<void>;
  totalDuration: number;
};

const HostedAudioContext = createContext<HostedAudioContextValue | null>(null);

function hasPlayableAudioBrief(audioBrief: AudioBrief | null | undefined): audioBrief is AudioBrief {
  return Boolean(audioBrief && audioBrief.status === "succeeded");
}

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

export function useHostedAudio() {
  const context = useContext(HostedAudioContext);
  if (!context) {
    throw new Error("Hosted audio context is not available.");
  }
  return context;
}

export function HostedAudioProvider({ children }: { children: ReactNode }) {
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const objectUrlRef = useRef<string | null>(null);
  const [edition, setEdition] = useState<HostedEditionAudio | null>(null);
  const [audioSrc, setAudioSrc] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isPlaying, setIsPlaying] = useState(false);
  const [isSheetOpen, setSheetOpen] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);

  const chapters = useMemo(() => {
    if (!hasPlayableAudioBrief(edition?.audioBrief)) return [];
    return [...edition.audioBrief.chapters]
      .filter((chapter) => Number.isFinite(chapter.offset_seconds))
      .sort((left, right) => left.offset_seconds - right.offset_seconds);
  }, [edition?.audioBrief]);

  const totalDuration = duration || edition?.audioBrief?.audio_duration_seconds || edition?.audioBrief?.estimated_duration_seconds || 0;
  const hasPlayableAudio = hasPlayableAudioBrief(edition?.audioBrief);

  const activeChapter = useMemo(() => {
    if (!chapters.length) return null;

    let nextChapter = chapters[0] ?? null;
    for (const chapter of chapters) {
      if (chapter.offset_seconds <= currentTime + 0.25) {
        nextChapter = chapter;
      } else {
        break;
      }
    }
    return nextChapter;
  }, [chapters, currentTime]);

  const progressRatio = totalDuration > 0 ? Math.min(1, currentTime / totalDuration) : 0;

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
    return () => {
      if (objectUrlRef.current) {
        URL.revokeObjectURL(objectUrlRef.current);
        objectUrlRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    const audio = audioRef.current;
    if (audio) {
      audio.pause();
      audio.removeAttribute("src");
      audio.load();
    }

    if (objectUrlRef.current) {
      URL.revokeObjectURL(objectUrlRef.current);
      objectUrlRef.current = null;
    }

    setAudioSrc(null);
    setIsLoading(false);
    setIsPlaying(false);
    setLoadError(null);
    setCurrentTime(0);
    setDuration(0);
    setSheetOpen(false);
  }, [edition?.audioGeneratedAt, edition?.recordName]);

  const registerManifest = (manifest: PublishedEditionManifest) => {
    const nextAudioBrief = manifest.digest.audio_brief as AudioBrief | null;

    setEdition((current) => {
      const nextEdition: HostedEditionAudio = {
        audioBrief: nextAudioBrief,
        audioGeneratedAt: nextAudioBrief?.generated_at ?? null,
        briefDate: manifest.digest.brief_date ?? "",
        items: manifest.items,
        recordName: manifest.edition.record_name,
        title: manifest.digest.title,
      };

      if (
        current &&
        current.recordName === nextEdition.recordName &&
        current.audioGeneratedAt === nextEdition.audioGeneratedAt &&
        current.title === nextEdition.title &&
        current.briefDate === nextEdition.briefDate &&
        current.audioBrief === nextEdition.audioBrief &&
        current.items === nextEdition.items
      ) {
        return current;
      }

      if (
        current &&
        current.recordName === nextEdition.recordName &&
        current.audioGeneratedAt === nextEdition.audioGeneratedAt
      ) {
        return {
          ...current,
          audioBrief: nextEdition.audioBrief,
          briefDate: nextEdition.briefDate,
          items: nextEdition.items,
          title: nextEdition.title,
        };
      }

      return nextEdition;
    });
  };

  const pause = () => {
    audioRef.current?.pause();
  };

  const ensureAudioLoaded = async () => {
    if (!edition || !hasPlayableAudioBrief(edition.audioBrief)) {
      throw new Error("Audio is unavailable for this edition.");
    }

    if (audioSrc) return audioSrc;

    setIsLoading(true);
    setLoadError(null);

    try {
      const resolvedAudioUrl = edition.audioBrief.audio_url ?? api.getAudioSummaryUrl(edition.briefDate);
      const requestInit = edition.audioBrief.audio_url ? undefined : { credentials: "include" as const };
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
    if (!audio || !hasPlayableAudioBrief(edition?.audioBrief)) return;

    if (audio.paused) {
      const source = await ensureAudioLoaded();
      if (audio.src !== source) audio.src = source;
      await audio.play();
      return;
    }

    audio.pause();
  };

  const seek = (nextTime: number) => {
    setCurrentTime(nextTime);
    if (audioRef.current) {
      audioRef.current.currentTime = nextTime;
    }
  };

  const seekToChapter = async (offsetSeconds: number) => {
    const audio = audioRef.current;
    if (!audio || !hasPlayableAudioBrief(edition?.audioBrief)) return;

    const source = await ensureAudioLoaded();
    if (audio.src !== source) audio.src = source;
    audio.currentTime = offsetSeconds;
    setCurrentTime(offsetSeconds);

    if (audio.paused) {
      await audio.play();
    }
  };

  const contextValue: HostedAudioContextValue = {
    activeChapter,
    chapterCount: chapters.length,
    currentTime,
    edition,
    hasPlayableAudio,
    isLoading,
    isPlaying,
    isSheetOpen,
    loadError,
    pause,
    progressRatio,
    registerManifest,
    seek,
    seekToChapter,
    setSheetOpen,
    togglePlayback,
    totalDuration,
  };

  return (
    <HostedAudioContext.Provider value={contextValue}>
      {children}
      <audio ref={audioRef} preload="metadata" />
    </HostedAudioContext.Provider>
  );
}

export function HostedEditionAudioBridge({ manifest }: { manifest: PublishedEditionManifest }) {
  const { registerManifest } = useHostedAudio();

  useEffect(() => {
    registerManifest(manifest);
  }, [manifest, registerManifest]);

  return null;
}

export function HostedAudioTeaser() {
  const { chapterCount, hasPlayableAudio, isLoading, isPlaying, togglePlayback, totalDuration, setSheetOpen } = useHostedAudio();

  if (!hasPlayableAudio) return null;

  return (
    <section className="pv-audio-teaser">
      <div className="pv-audio-teaser-copy">
        <p className="pv-eyebrow">Listen</p>
        <h3 className="pv-audio-teaser-title">Audio briefing</h3>
        <p className="pv-audio-teaser-summary">
          {Math.max(1, Math.round(totalDuration / 60) || 0)} min listen
          <span className="pv-audio-dot" />
          {chapterCount} chapters
        </p>
      </div>

      <div className="pv-audio-teaser-actions">
        <button
          className="pv-inline-player-button"
          onClick={() => {
            void togglePlayback();
          }}
          type="button"
        >
          {isLoading ? <LoaderCircle className="h-4 w-4 animate-spin" /> : isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
          <span>{isPlaying ? "Pause" : "Play"}</span>
        </button>
        <button className="ghost-button" onClick={() => setSheetOpen(true)} type="button">
          <Radio className="h-4 w-4" />
          Open player
        </button>
      </div>
    </section>
  );
}

export function HostedAudioUtilityCard() {
  const { activeChapter, chapterCount, edition, hasPlayableAudio, isLoading, isPlaying, togglePlayback, totalDuration, setSheetOpen } = useHostedAudio();

  if (!hasPlayableAudio || !edition) return null;

  const chapterTitle = activeChapter?.headline || activeChapter?.item_title || edition.title;

  return (
    <section className="pv-utility-card pv-utility-card-dark">
      <p className="pv-eyebrow pv-eyebrow-light">Now listening</p>
      <h3 className="pv-utility-audio-title">{chapterTitle}</h3>
      <p className="pv-utility-audio-summary">
        {Math.max(1, Math.round(totalDuration / 60) || 0)} min listen
        <span className="pv-audio-dot" />
        {chapterCount} chapters
      </p>
      <div className="pv-utility-audio-actions">
        <button
          className="pv-inline-player-button pv-inline-player-button-light"
          onClick={() => {
            void togglePlayback();
          }}
          type="button"
        >
          {isLoading ? <LoaderCircle className="h-4 w-4 animate-spin" /> : isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
          <span>{isPlaying ? "Pause" : "Play"}</span>
        </button>
        <button className="ghost-button pv-ghost-button-dark" onClick={() => setSheetOpen(true)} type="button">
          Open player
        </button>
      </div>
    </section>
  );
}

export function HostedAudioMiniPlayer({ pairedLocalUrl }: { pairedLocalUrl: string | null }) {
  const {
    activeChapter,
    chapterCount,
    currentTime,
    edition,
    hasPlayableAudio,
    isLoading,
    isPlaying,
    progressRatio,
    setSheetOpen,
    togglePlayback,
    totalDuration,
  } = useHostedAudio();

  if (!hasPlayableAudio || !edition) return null;

  const title = activeChapter?.headline || activeChapter?.item_title || edition.title;
  const statusLabel = isPlaying
    ? `Now listening · ${formatClock(currentTime)}`
    : `${Math.max(1, Math.round(totalDuration / 60) || 0)} min listen · ${chapterCount} chapters`;

  return (
    <div className="pv-mini-player">
      <button
        aria-label={isPlaying ? "Pause audio briefing" : "Play audio briefing"}
        className="pv-mini-player-play"
        onClick={() => {
          void togglePlayback();
        }}
        type="button"
      >
        {isLoading ? <LoaderCircle className="h-5 w-5 animate-spin" /> : isPlaying ? <Pause className="h-5 w-5" /> : <Play className="h-5 w-5" />}
      </button>

      <button className="pv-mini-player-body" onClick={() => setSheetOpen(true)} type="button">
        <span className="pv-mini-player-status">{statusLabel}</span>
        <span className="pv-mini-player-title">{title}</span>
        <span aria-hidden="true" className="pv-mini-player-progress">
          <span className="pv-mini-player-progress-bar" style={{ width: `${Math.round(progressRatio * 100)}%` }} />
        </span>
      </button>

      {pairedLocalUrl ? (
        <a className="pv-mini-player-utility" href={pairedLocalUrl}>
          Open Mac
        </a>
      ) : (
        <button
          aria-label="Open audio player"
          className="pv-mini-player-utility pv-mini-player-chevron"
          onClick={() => setSheetOpen(true)}
          type="button"
        >
          <ChevronUp className="h-4 w-4" />
        </button>
      )}
    </div>
  );
}

export function HostedAudioSheet({ pairedLocalUrl }: { pairedLocalUrl: string | null }) {
  const [showTranscript, setShowTranscript] = useState(false);
  const {
    activeChapter,
    chapterCount,
    currentTime,
    edition,
    hasPlayableAudio,
    isLoading,
    isPlaying,
    isSheetOpen,
    loadError,
    pause,
    seek,
    seekToChapter,
    setSheetOpen,
    togglePlayback,
    totalDuration,
  } = useHostedAudio();

  useEffect(() => {
    if (!isSheetOpen) return undefined;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [isSheetOpen]);

  if (!isSheetOpen || !hasPlayableAudio || !edition || !hasPlayableAudioBrief(edition.audioBrief)) {
    return null;
  }

  const transcript = edition.audioBrief.script?.trim() ?? "";
  const chapters = edition.audioBrief.chapters
    .filter((chapter) => Number.isFinite(chapter.offset_seconds))
    .sort((left, right) => left.offset_seconds - right.offset_seconds);
  const activeItemPath =
    activeChapter && edition.items[activeChapter.item_id]
      ? `/items/${activeChapter.item_id}?record=${encodeURIComponent(edition.recordName)}`
      : null;
  const title = activeChapter?.headline || activeChapter?.item_title || edition.title;

  return (
    <>
      <button
        aria-label="Close audio player"
        className="pv-listening-sheet-backdrop"
        onClick={() => setSheetOpen(false)}
        type="button"
      />

      <section aria-label="Audio briefing player" className="pv-listening-sheet" role="dialog">
        <div className="pv-sheet-header">
          <div>
            <p className="pv-eyebrow pv-eyebrow-light">Audio briefing</p>
            <h2 className="pv-sheet-title">{title}</h2>
            <p className="pv-sheet-subtitle">{edition.title}</p>
          </div>
          <button className="pv-sheet-close" onClick={() => setSheetOpen(false)} type="button">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="pv-sheet-controls">
          <button
            className={clsx("pv-sheet-play", isPlaying && "pv-sheet-play-active")}
            onClick={() => {
              void togglePlayback();
            }}
            type="button"
          >
            {isLoading ? <LoaderCircle className="h-6 w-6 animate-spin" /> : isPlaying ? <Pause className="h-6 w-6" /> : <Play className="h-6 w-6" />}
          </button>

          <div className="pv-sheet-scrubber">
            <input
              aria-label="Audio progress"
              className="audio-progress"
              max={Math.max(totalDuration, 0.1)}
              min={0}
              onChange={(event) => seek(Number(event.target.value))}
              step={0.1}
              type="range"
              value={Math.min(currentTime, totalDuration || 0)}
            />
            <div className="pv-sheet-progress-meta">
              <span>
                {formatClock(currentTime)} / {formatClock(totalDuration)}
              </span>
              <span>{chapterCount} chapters</span>
            </div>
          </div>
        </div>

        {loadError ? <p className="pv-sheet-error">{loadError}</p> : null}

        <div className="pv-sheet-actions">
          {activeItemPath ? (
            <Link
              className="secondary-button"
              onClick={() => setSheetOpen(false)}
              to={activeItemPath}
            >
              Read story
            </Link>
          ) : null}

          {transcript ? (
            <button className="ghost-button pv-ghost-button-dark" onClick={() => setShowTranscript((open) => !open)} type="button">
              <FileText className="h-4 w-4" />
              {showTranscript ? "Hide transcript" : "Transcript"}
            </button>
          ) : null}

          {pairedLocalUrl ? (
            <a className="ghost-button pv-ghost-button-dark" href={pairedLocalUrl}>
              Open Mac
            </a>
          ) : null}

          <button className="ghost-button pv-ghost-button-dark" onClick={pause} type="button">
            Pause
          </button>
        </div>

        <div className="pv-sheet-chapter-list">
          {chapters.map((chapter) => {
            const isActive = activeChapter?.offset_seconds === chapter.offset_seconds;
            return (
              <button
                className={clsx("pv-sheet-chapter", isActive && "pv-sheet-chapter-active")}
                key={`${chapter.item_id}-${chapter.offset_seconds}`}
                onClick={() => {
                  void seekToChapter(chapter.offset_seconds);
                }}
                type="button"
              >
                <span className="pv-sheet-chapter-time">{formatClock(chapter.offset_seconds)}</span>
                <span className="pv-sheet-chapter-copy">
                  <span className="pv-sheet-chapter-section">{chapter.section}</span>
                  <span className="pv-sheet-chapter-title">{chapter.headline || chapter.item_title}</span>
                </span>
              </button>
            );
          })}
        </div>

        {showTranscript ? (
          <section className="pv-sheet-transcript">
            <p className="pv-eyebrow pv-eyebrow-light">Transcript</p>
            <p className="pv-sheet-transcript-body">{transcript}</p>
          </section>
        ) : null}
      </section>
    </>
  );
}
