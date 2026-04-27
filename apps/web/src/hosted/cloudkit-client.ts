import {
  getCachedLatestPublishedManifest,
  getCachedPublishedArchive,
  getCachedPublishedManifest,
  setCachedPublishedArchive,
  setCachedPublishedManifest,
} from "../runtime/storage";
import type {
  CloudKitRuntimeConfig,
  PublishedArchive,
  PublishedArchiveEntry,
  PublishedEditionManifest,
} from "../runtime/types";

type CloudKitField = {
  value?: unknown;
};

type CloudKitRecord = {
  recordName?: string;
  fields?: Record<string, CloudKitField>;
  serverErrorCode?: string;
  reason?: string;
};

function readAssetUrl(record: CloudKitRecord, fieldName: string) {
  const value = record.fields?.[fieldName]?.value;
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  if (typeof payload.downloadURL === "string" && payload.downloadURL.trim()) return payload.downloadURL;
  if (typeof payload.url === "string" && payload.url.trim()) return payload.url;
  return null;
}

export class PublishedDataFetchError extends Error {}

export interface PublishedDataClient {
  getCachedLatestManifest(): PublishedEditionManifest | null;
  getCachedManifest(recordName: string): PublishedEditionManifest | null;
  fetchLatestManifest(): Promise<PublishedEditionManifest>;
  fetchManifest(recordName: string): Promise<PublishedEditionManifest>;
}

export class CloudKitPublishedDataClient implements PublishedDataClient {
  private latestRecordName: string;

  constructor(private readonly config: CloudKitRuntimeConfig) {
    this.latestRecordName = config.latestRecordName ?? "PublishedEdition::latest";
  }

  getCachedLatestManifest() {
    return getCachedLatestPublishedManifest();
  }

  getCachedManifest(recordName: string) {
    return getCachedPublishedManifest(recordName);
  }

  async fetchLatestManifest() {
    const cached = this.getCachedLatestManifest();
    try {
      return await this.fetchManifest(this.latestRecordName);
    } catch (error) {
      if (cached) return cached;
      throw error;
    }
  }

  async fetchManifest(recordName: string): Promise<PublishedEditionManifest> {
    const cached = this.getCachedManifest(recordName);

    try {
      const record = await this.lookupRecord(recordName);
      const manifestUrl = readAssetUrl(record, "manifest_asset");
      if (!manifestUrl) {
        throw new PublishedDataFetchError(`Manifest asset is missing for record ${recordName}.`);
      }

      const manifestResponse = await fetch(manifestUrl, {
        cache: "no-store",
      });
      if (!manifestResponse.ok) {
        throw new PublishedDataFetchError(`Manifest asset could not be loaded for record ${recordName}.`);
      }
      const manifest = (await manifestResponse.json()) as PublishedEditionManifest;
      const audioUrl = readAssetUrl(record, "audio_asset");
      if (manifest.digest.audio_brief && audioUrl) {
        manifest.digest.audio_brief.audio_url = audioUrl;
      }
      setCachedPublishedManifest(manifest);
      return manifest;
    } catch (error) {
      if (cached) return cached;
      throw error;
    }
  }

  private async lookupRecord(recordName: string): Promise<CloudKitRecord> {
    const response = await fetch(this.recordsUrl("records/lookup"), {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        records: [{ recordName }],
      }),
    });
    if (!response.ok) {
      throw new PublishedDataFetchError(`CloudKit lookup failed for record ${recordName}.`);
    }
    const payload = (await response.json()) as { records?: CloudKitRecord[]; serverErrorCode?: string; reason?: string };
    if (payload.serverErrorCode) {
      throw new PublishedDataFetchError(payload.reason || payload.serverErrorCode);
    }
    const record = payload.records?.[0];
    if (!record) {
      throw new PublishedDataFetchError(`CloudKit record ${recordName} was not found.`);
    }
    if (record.serverErrorCode) {
      throw new PublishedDataFetchError(record.reason || record.serverErrorCode);
    }
    return record;
  }

  private recordsUrl(path: string) {
    const { containerIdentifier, environment, database, apiToken } = this.config;
    const encodedApiToken = encodeURIComponent(apiToken);
    return `https://api.apple-cloudkit.com/database/1/${containerIdentifier}/${environment}/${database}/${path}?ckAPIToken=${encodedApiToken}`;
  }
}

function currentDocumentBase() {
  const { origin, pathname } = window.location;
  const normalizedPath = pathname.endsWith("/") ? pathname : pathname.replace(/[^/]*$/, "");
  return new URL(`${origin}${normalizedPath}`);
}

function normalizeBasePath(path: string) {
  if (!path.trim()) return "./";
  return path.endsWith("/") ? path : `${path}/`;
}

export class StaticPublishedDataClient implements PublishedDataClient {
  private archiveCache = getCachedPublishedArchive();
  private readonly rootUrl: URL;

  constructor(basePath: string) {
    this.rootUrl = new URL(normalizeBasePath(basePath), currentDocumentBase());
  }

  getCachedLatestManifest() {
    return getCachedLatestPublishedManifest();
  }

  getCachedManifest(recordName: string) {
    return getCachedPublishedManifest(recordName);
  }

  async fetchLatestManifest(): Promise<PublishedEditionManifest> {
    const cached = this.getCachedLatestManifest();

    try {
      const archive = await this.fetchArchive();
      return this.fetchManifestAtEntry(archive.latest);
    } catch (error) {
      if (cached) return cached;
      throw error;
    }
  }

  async fetchManifest(recordName: string): Promise<PublishedEditionManifest> {
    const cached = this.getCachedManifest(recordName);

    try {
      const archive = await this.fetchArchive();
      const entry = archive.editions.find((edition) => edition.record_name === recordName);
      if (!entry) {
        throw new PublishedDataFetchError(`Published edition ${recordName} was not found in archive.json.`);
      }
      return this.fetchManifestAtEntry(entry);
    } catch (error) {
      if (cached) return cached;
      throw error;
    }
  }

  private async fetchArchive(): Promise<PublishedArchive> {
    try {
      const response = await fetch(this.resolveUrl("archive.json"), {
        cache: "no-store",
      });
      if (!response.ok) {
        throw new PublishedDataFetchError("Published archive could not be loaded.");
      }
      const archive = (await response.json()) as PublishedArchive;
      this.archiveCache = archive;
      setCachedPublishedArchive(archive);
      return archive;
    } catch (error) {
      if (this.archiveCache) return this.archiveCache;
      const cached = getCachedPublishedArchive();
      if (cached) {
        this.archiveCache = cached;
        return cached;
      }
      throw error;
    }
  }

  private async fetchManifestAtEntry(entry: PublishedArchiveEntry): Promise<PublishedEditionManifest> {
    const cached = this.getCachedManifest(entry.record_name);

    try {
      const response = await fetch(this.resolveUrl(entry.manifest_path), {
        cache: "no-store",
      });
      if (!response.ok) {
        throw new PublishedDataFetchError(`Published manifest could not be loaded for ${entry.record_name}.`);
      }
      const manifest = (await response.json()) as PublishedEditionManifest;
      if (manifest.digest.audio_brief && entry.audio_path) {
        manifest.digest.audio_brief.audio_url = this.resolveUrl(entry.audio_path);
      }
      setCachedPublishedManifest(manifest);
      return manifest;
    } catch (error) {
      if (cached) return cached;
      throw error;
    }
  }

  private resolveUrl(relativePath: string) {
    return new URL(relativePath, this.rootUrl).toString();
  }
}
