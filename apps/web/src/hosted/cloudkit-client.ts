import type { CloudKitRuntimeConfig, PublishedEditionManifest } from "../runtime/types";

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

export class CloudKitFetchError extends Error {}

export class PublishedDataClient {
  private latestRecordName: string;

  constructor(private readonly config: CloudKitRuntimeConfig) {
    this.latestRecordName = config.latestRecordName ?? "PublishedEdition::latest";
  }

  async fetchLatestManifest() {
    return this.fetchManifest(this.latestRecordName);
  }

  async fetchManifest(recordName: string): Promise<PublishedEditionManifest> {
    const record = await this.lookupRecord(recordName);
    const manifestUrl = readAssetUrl(record, "manifest_asset");
    if (!manifestUrl) {
      throw new CloudKitFetchError(`Manifest asset is missing for record ${recordName}.`);
    }

    const manifestResponse = await fetch(manifestUrl, {
      cache: "no-store",
    });
    if (!manifestResponse.ok) {
      throw new CloudKitFetchError(`Manifest asset could not be loaded for record ${recordName}.`);
    }
    const manifest = (await manifestResponse.json()) as PublishedEditionManifest;
    const audioUrl = readAssetUrl(record, "audio_asset");
    if (manifest.digest.audio_brief && audioUrl) {
      manifest.digest.audio_brief.audio_url = audioUrl;
    }
    return manifest;
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
      throw new CloudKitFetchError(`CloudKit lookup failed for record ${recordName}.`);
    }
    const payload = (await response.json()) as { records?: CloudKitRecord[]; serverErrorCode?: string; reason?: string };
    if (payload.serverErrorCode) {
      throw new CloudKitFetchError(payload.reason || payload.serverErrorCode);
    }
    const record = payload.records?.[0];
    if (!record) {
      throw new CloudKitFetchError(`CloudKit record ${recordName} was not found.`);
    }
    if (record.serverErrorCode) {
      throw new CloudKitFetchError(record.reason || record.serverErrorCode);
    }
    return record;
  }

  private recordsUrl(path: string) {
    const { containerIdentifier, environment, database, apiToken } = this.config;
    const encodedApiToken = encodeURIComponent(apiToken);
    return `https://api.apple-cloudkit.com/database/1/${containerIdentifier}/${environment}/${database}/${path}?ckAPIToken=${encodedApiToken}`;
  }
}
