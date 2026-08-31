/**
 * /api/publish
 *
 * POST {action:"promote", marketplace} → promuove il risultato in quarantena di
 *                                        quel marketplace a "ultimo pronto"
 * POST {action:"discard", marketplace} → scarta il risultato in quarantena
 *
 * Serve quando il guard-rail blocca una conversione (es. calo anomalo di
 * prodotti) e dopo verifica si decide comunque di pubblicarla. Il marketplace e'
 * obbligatorio nella sostanza: promuovere "la quarantena" senza dire quale, con
 * due cataloghi, significherebbe pubblicare a caso. Se manca si intende il
 * primario, come per tutti gli altri endpoint.
 */

import {
  resultStore, RESULT_KEY, PENDING_KEY,
  savePublishedSkus, saveHistoryRecord, historyFrom, pushAlerts, json,
  loadConfig, getForMarket, setForMarket, delForMarket,
} from "./_lib/stores.mjs";
import { primaryCode, findMarketplace } from "./_lib/marketplace.mjs";

export default async (req) => {
  if (req.method !== "POST") return json({ error: "Method not allowed" }, 405);
  const store = resultStore();

  try {
    const body = await req.json().catch(() => ({}));
    const { action } = body;
    const config = await loadConfig();
    const code = String(body.marketplace || primaryCode(config)).toUpperCase();
    if (config?.marketplaces?.length && !findMarketplace(config, code)) {
      return json({ error: `Marketplace ${code} non configurato.` }, 400);
    }
    const primary = code === String(primaryCode(config)).toUpperCase();

    if (action === "discard") {
      await delForMarket(store, PENDING_KEY, code);
      if (primary) await store.delete(PENDING_KEY).catch(() => {});
      return json({ ok: true, marketplace: code, discarded: true });
    }

    if (action !== "promote") return json({ error: 'action deve essere "promote" o "discard"' }, 400);

    const pending = await getForMarket(store, PENDING_KEY, code, { primary });
    if (!pending) return json({ error: `Nessun risultato in quarantena per ${code}` }, 404);

    const { publishedSkus, ...payload } = pending;
    await setForMarket(store, RESULT_KEY, code, { ...payload, source: (payload.source || "manual") + "-promoted" });
    if (publishedSkus) await savePublishedSkus(publishedSkus, config, code);
    await delForMarket(store, PENDING_KEY, code);
    if (primary) await store.delete(PENDING_KEY).catch(() => {});
    await saveHistoryRecord(historyFrom(payload.stats, "promoted"));
    await pushAlerts([{
      type: "info",
      title: `✅ ${code}: conversione in quarantena confermata`,
      message: `${payload.stats?.total_products ?? "?"} prodotti pubblicati manualmente nonostante l'avviso del controllo di sicurezza.`,
    }]);

    return json({ ok: true, marketplace: code, promoted: true, stats: payload.stats });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
};

export const config = { path: "/api/publish" };
