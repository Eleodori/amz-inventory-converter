/**
 * /api/publish
 *
 * POST {action:"promote"} → promuove il risultato in quarantena a "ultimo pronto"
 * POST {action:"discard"} → scarta il risultato in quarantena
 *
 * Serve quando il guard-rail blocca una conversione (es. calo anomalo di
 * prodotti) e dopo verifica si decide comunque di pubblicarla.
 */

import {
  resultStore, RESULT_KEY, PENDING_KEY,
  savePublishedSkus, saveHistoryRecord, historyFrom, pushAlerts, json,
} from "./_lib/stores.mjs";

export default async (req) => {
  if (req.method !== "POST") return json({ error: "Method not allowed" }, 405);
  const store = resultStore();

  try {
    const { action } = await req.json().catch(() => ({}));

    if (action === "discard") {
      await store.delete(PENDING_KEY).catch(() => {});
      return json({ ok: true, discarded: true });
    }

    if (action !== "promote") return json({ error: 'action deve essere "promote" o "discard"' }, 400);

    const pending = await store.get(PENDING_KEY, { type: "json" });
    if (!pending) return json({ error: "Nessun risultato in quarantena" }, 404);

    const { publishedSkus, ...payload } = pending;
    await store.setJSON(RESULT_KEY, { ...payload, source: (payload.source || "manual") + "-promoted" });
    if (publishedSkus) await savePublishedSkus(publishedSkus);
    await store.delete(PENDING_KEY).catch(() => {});
    await saveHistoryRecord(historyFrom(payload.stats, "promoted"));
    await pushAlerts([{
      type: "info",
      title: "✅ Conversione in quarantena confermata",
      message: `${payload.stats?.total_products ?? "?"} prodotti pubblicati manualmente nonostante l'avviso del controllo di sicurezza.`,
    }]);

    return json({ ok: true, promoted: true, stats: payload.stats });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
};

export const config = { path: "/api/publish" };
