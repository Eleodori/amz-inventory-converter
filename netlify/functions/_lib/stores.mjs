import { getStore } from "@netlify/blobs";

const S = name => getStore({ name, consistency: "strong" });

export const configStore    = () => S("amz-config");
export const resultStore    = () => S("ftp-results");
export const alertStore     = () => S("amz-alerts");
export const historyStore   = () => S("conversion-history");
export const templateStore  = () => S("amz-template");
export const asinMapStore   = () => S("amz-asinmap");
export const publishedStore = () => S("amz-published");

export const CONFIG_KEY    = "app-config";
export const RESULT_KEY    = "latest";
export const PENDING_KEY   = "pending";
export const TEMPLATE_KEY  = "current";
export const ASINMAP_KEY   = "map";
export const PUBLISHED_KEY = "skus";

export const HISTORY_MAX = 180;

export async function loadConfig() {
  return await configStore().get(CONFIG_KEY, { type: "json" });
}

export async function loadPublishedSkus() {
  return (await publishedStore().get(PUBLISHED_KEY, { type: "json" })) || {};
}

export async function savePublishedSkus(map) {
  await publishedStore().setJSON(PUBLISHED_KEY, map);
}

/**
 * Potatura dello storico. Prima girava solo nell'handler POST di /api/history:
 * lo scheduled job scriveva direttamente sul blob senza mai potare, quindi i
 * record crescevano senza limite.
 */
export async function saveHistoryRecord(record) {
  const store = historyStore();
  const key = `conv-${record.created_at}-${record.id}`;
  await store.setJSON(key, record);
  try {
    const { blobs } = await store.list();
    if (blobs.length > HISTORY_MAX) {
      const sorted = [...blobs].sort((a, b) => a.key.localeCompare(b.key));
      const toDelete = sorted.slice(0, blobs.length - HISTORY_MAX);
      await Promise.all(toDelete.map(({ key }) => store.delete(key)));
    }
  } catch (e) {
    console.warn("Potatura storico non riuscita:", e.message);
  }
  return key;
}

/**
 * Aggiunge alert mantenendo i non letti.
 * Prima il filtro era (created_at > cutoff && a.read): teneva solo i letti,
 * quindi un alert non ancora aperto sparival'indomani, cioe' esattamente
 * l'opposto di quello che serve.
 */
export async function pushAlerts(newOnes, { keepDays = 30 } = {}) {
  if (!newOnes?.length) return 0;
  const store = alertStore();
  const existing = (await store.get("active", { type: "json" })) || [];
  const cutoff = new Date(Date.now() - keepDays * 86400000).toISOString();
  const kept = existing.filter(a => !a.read || a.created_at > cutoff);
  const stamped = newOnes.map(a => ({
    ...a,
    id: crypto.randomUUID(),
    created_at: new Date().toISOString(),
    read: false,
  }));
  await store.setJSON("active", [...kept, ...stamped]);
  return stamped.length;
}

/**
 * Fotografia essenziale delle statistiche del file precedente, salvata nel
 * payload nuovo. Serve al confronto giorno-su-giorno nell'interfaccia senza
 * dover interrogare lo storico. Null-safe: al primo giro previous non esiste.
 */
export function prevSnapshot(stats) {
  if (!stats) return null;
  return {
    without_asin: stats.without_asin ?? null,
    with_asin: stats.with_asin ?? null,
    total_rows: stats.total_rows ?? null,
    total_products: stats.total_products ?? null,
    created_at: stats.created_at ?? null,
  };
}

export function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

/** Record di storico a partire dalle statistiche di conversione. */
export function historyFrom(stats, source) {
  return {
    id: crypto.randomUUID(),
    created_at: new Date().toISOString(),
    marketplace: stats.marketplace,
    total_products: stats.total_products,
    total_rows: stats.total_rows,
    zeroed: stats.zeroed,
    total_read: stats.total_read,
    total_skipped: stats.total_skipped,
    bad_ean: stats.bad_ean,
    below_min_stock: stats.below_min_stock,
    duplicates_resolved: stats.duplicates_resolved,
    blacklisted: stats.blacklisted,
    with_asin: stats.with_asin,
    without_asin: stats.without_asin,
    unmapped_skipped: stats.unmapped_skipped,
    by_supplier: stats.by_supplier,
    by_tier: stats.by_tier,
    avg_price_by_tier: stats.avg_price_by_tier,
    avg_price_total: stats.avg_price_total,
    source,
  };
}
