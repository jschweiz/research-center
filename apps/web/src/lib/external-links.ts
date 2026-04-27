const FREEDIUM_MIRROR_PREFIX = "https://freedium-mirror.cfd/";

function isMediumHostname(hostname: string) {
  return hostname === "medium.com" || hostname.endsWith(".medium.com");
}

export function resolveExternalUrl(url: string): string {
  const trimmed = url.trim();
  if (!trimmed || trimmed.startsWith(FREEDIUM_MIRROR_PREFIX)) {
    return trimmed;
  }

  try {
    const parsed = new URL(trimmed);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return trimmed;
    }

    return isMediumHostname(parsed.hostname.toLowerCase()) ? `${FREEDIUM_MIRROR_PREFIX}${trimmed}` : trimmed;
  } catch {
    return trimmed;
  }
}
