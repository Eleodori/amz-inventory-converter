/**
 * /api/config
 * GET  → configurazione applicazione
 * POST → salva la configurazione
 */

import { configStore, CONFIG_KEY, json } from "./_lib/stores.mjs";

/** Valori di default per le chiavi introdotte con il nuovo formato. */
const DEFAULTS = {
  qtyMode: "catalog",
  dupMode: "price",
  minStock: 0,        // pubblica solo se lo stock fornitore e' >= a questo valore
  maxQty: 0,          // 0 = nessun tetto; es. 4 limita l'esposizione per ciclo
  floorMarginPct: 0,  // se > 0 popola il prezzo minimo consentito al venditore
  zeroKeepDays: 90,   // per quanti giorni continuare a inviare quantita' 0
  maxDeltaPct: 20,    // guard-rail: variazione massima di prodotti accettata
  minProducts: 100,   // guard-rail: sotto questa soglia il risultato e' sospetto
  alertThresholds: { productsDeltaPct: 10, supplierDeltaPct: 20 },
};

export default async (req) => {
  const store = configStore();

  if (req.method === "GET") {
    try {
      const config = await store.get(CONFIG_KEY, { type: "json" });
      if (!config) return json(null);
      return json({ ...DEFAULTS, ...config, alertThresholds: { ...DEFAULTS.alertThresholds, ...(config.alertThresholds || {}) } });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "POST") {
    try {
      const body = await req.json();
      await store.setJSON(CONFIG_KEY, { ...DEFAULTS, ...body, updated_at: new Date().toISOString() });
      return json({ ok: true });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/config" };
