import { Harvester } from ".";
import { BaseHarvesterConfig } from "./base";
import { SocrataHarvester } from "./socrata";
import { type SocrataDataset } from "@/schemas/socrata";
import { PortalJsCloudDataset } from "@/schemas/portaljs-cloud";
import { getDataset, upsertDataset } from "../lib/portaljs-cloud";

@Harvester
class SocrataSingleDatasetHarvester extends SocrataHarvester {
  constructor(args: BaseHarvesterConfig) {
    super(args);
  }

  async getSourceDatasets(): Promise<SocrataDataset[]> {
    const datasetId = process.env.SOCRATA_DATASET_ID;
    if (!datasetId) {
      throw new Error("SOCRATA_DATASET_ID environment variable is required for SocrataSingleDatasetHarvester");
    }

    const url = `${this.config.source.url}/api/views/${datasetId}.json`;
    const headers: Record<string, string> = {};
    if (this.config.source.apiKey) {
      headers["X-App-Token"] = this.config.source.apiKey;
    }

    const res = await fetch(url, { headers });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Failed to fetch dataset ${datasetId}: ${res.status} ${res.statusText}\n${body}`);
    }

    const dataset = await res.json();
    return [dataset];
  }

  async upsertIntoTarget({ dataset }: { dataset: PortalJsCloudDataset }) {
    const existing = await getDataset(dataset.name);

    const merged: PortalJsCloudDataset = existing
      ? {
          ...dataset,
          // Keep existing values for all scalar fields that are already set
          title: existing.title || dataset.title,
          notes: existing.notes || dataset.notes,
          author: existing.author || dataset.author,
          author_email: existing.author_email || dataset.author_email,
          maintainer: existing.maintainer || dataset.maintainer,
          maintainer_email: existing.maintainer_email || dataset.maintainer_email,
          language: existing.language || dataset.language,
          coverage: existing.coverage || dataset.coverage,
          rights: existing.rights || dataset.rights,
          contact_point: existing.contact_point || dataset.contact_point,
          license_id: existing.license_id || dataset.license_id,
          version: existing.version || dataset.version,
          // Keep existing tags if already set
          tags: existing.tags?.length ? existing.tags : dataset.tags,
          // Merge extras: prefer existing values for matching keys, always update Last Harvested At
          extras: mergeExtras(existing.extras ?? [], dataset.extras ?? []),
          // Update resources but preserve existing descriptions
          resources: mergeResources(existing.resources ?? [], dataset.resources ?? []),
        }
      : dataset;

    return upsertDataset({ dataset: merged, dryRun: this.config.dryRun });
  }
}

function mergeResources(
  existing: { url?: string; name?: string; description?: string; format?: string }[],
  incoming: { url?: string; name?: string; description?: string; format?: string }[]
): typeof incoming {
  const existingByUrl = new Map(existing.map((r) => [r.url, r]));
  return incoming.map((r) => {
    const prev = existingByUrl.get(r.url);
    return {
      ...r,
      ...(prev?.description && { description: prev.description }),
      ...(prev?.format && { format: prev.format }),
    };
  });
}

function mergeExtras(
  existing: { key: string; value: string }[],
  incoming: { key: string; value: string }[]
): { key: string; value: string }[] {
  const merged = new Map(existing.map((e) => [e.key, e.value]));
  for (const { key, value } of incoming) {
    // Always overwrite harvesting metadata; preserve everything else
    if (key === "Last Harvested At" || !merged.has(key)) {
      merged.set(key, value);
    }
  }
  return Array.from(merged, ([key, value]) => ({ key, value }));
}

export { SocrataSingleDatasetHarvester };
