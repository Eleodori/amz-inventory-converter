import { getStore } from "@netlify/blobs";
import { keyFor, primaryCode, migrateConfig, findMarketplace } from "./marketplace.mjs";

const S = name => getStore({ name, consistency: "strong" });

export const configStore    = () => S("amz-config");
export const resultStore    = () => S("ftp-results");
export const alertStore     = () => S("amz-alerts");
export const historyStore   = () => S("conversion-history");
export const templateStore  = () => S("amz-template");
export const asinMapStore   = () => S("amz-asinmap");
export const publishedStore = () => S("amz-published");

export const CONFIG_KEY    = "app-config";

// Chiavi storiche, di quando esisteva un solo marketplace. Restano leggibili:
// finche' non si scrive per la prima volta sulla chiave nuova, la lettura per il
// marketplace primario ricade su queste. Cosi' il passaggio al multi-marketplace
// non perde niente e non richiede uno script di migrazione.
export const RESULT_KEY    = "latest";
export const PENDING_KEY   = "pending";
export const TEMPLATE_KEY  = "current";
export const ASINMAP_KEY   = "map";
export const PUBLISHED_KEY = "skus";

/**
 * Lettura per marketplace con ricaduta sulla chiave storica.
 * La ricaduta vale SOLO per il marketplace primario: per la Germania una chiave
 * mancante significa "non configurato", non "usa i dati italiani".
 */
export async function getForMarket(store, base, code, { primary = false } = {}) {
  const v = await store.get(keyFor(base, code), { type: "json" });
  if (v !== null && v !== undefined) return v;
  if (!primary) return null;
  return await store.get(base, { type: "json" });
}

export async function setForMarket(store, base, code, value) {
  await store.setJSON(keyFor(base, code), value);
}

export async function delForMarket(store, base, code) {
  await store.delete(keyFor(base, code)).catch(() => {});
}

export const HISTORY_MAX = 180;

export async function loadConfig() {
  const raw = await configStore().get(CONFIG_KEY, { type: "json" });
  return raw ? migrateConfig(raw) : raw;
}

/**
 * Registro delle SKU pubblicate, per marketplace.
 *
 * E' il pezzo che va tenuto separato con piu' cura: il registro decide chi esce
 * con quantita' 0. Condividendolo fra IT e DE, un EAN venduto su un solo
 * marketplace risulterebbe "non piu' fornito" sull'altro e si spegnerebbe da
 * solo, oppure resterebbe acquistabile per sempre. Due cataloghi, due registri.
 */
export async function loadPublishedSkus(config, code) {
  const mk = code || primaryCode(config);
  const primary = mk === primaryCode(config);
  return (await getForMarket(publishedStore(), PUBLISHED_KEY, mk, { primary })) || {};
}

export async function savePublishedSkus(map, config, code) {
  const mk = code || primaryCode(config);
  await setForMarket(publishedStore(), PUBLISHED_KEY, mk, map);
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
    // Il confronto del banner usa questo: le righe senza ASIN che vanno
    // davvero in vendita. Sui file generati prima che esistesse il campo si
    // ricade su without_asin, che era la definizione di allora.
    without_asin_active: stats.without_asin_active ?? null,
    with_asin: stats.with_asin ?? null,
    total_rows: stats.total_rows ?? null,
    total_products: stats.total_products ?? null,
    created_at: stats.created_at ?? null,
  };
}

/**
 * Marketplace richiesto da una chiamata HTTP: ?mk=IT / ?mk=DE, altrimenti il
 * primario. Sta qui e non nei singoli handler perche' se ognuno risolvesse a
 * modo suo prima o poi due endpoint leggerebbero e scriverebbero chiavi diverse.
 */
export async function resolveMarketplaceFromRequest(req) {
  const cfg = await loadConfig();
  const asked = new URL(req.url).searchParams.get("mk");
  const code = String(asked || primaryCode(cfg)).toUpperCase();
  if (cfg?.marketplaces?.length && !findMarketplace(cfg, code)) {
    return {
      error: `Marketplace ${code} non configurato. Configurati: ` +
        (cfg.marketplaces.map(m => m.code).join(", ") || "nessuno"),
    };
  }
  return { config: cfg, code, primary: code === String(primaryCode(cfg)).toUpperCase() };
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
