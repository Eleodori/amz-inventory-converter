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
  onlyMapped: false,  // true = pubblica solo gli EAN con ASIN verificato
  alertThresholds: { productsDeltaPct: 10, supplierDeltaPct: 20 },
};

export default async (req) => {
  const store = configStore();

  if (req.method === "GET") {
    try {
      const config = await store.get(CONFIG_KEY, { type: "json" });
      if (!config) return json(null);
      return json({
        ...DEFAULTS, ...config,
        rev: config.rev || 0,
        alertThresholds: { ...DEFAULTS.alertThresholds, ...(config.alertThresholds || {}) },
      });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "POST") {
    try {
      const body = await req.json();
      const stored = await store.get(CONFIG_KEY, { type: "json" });
      const storedRev = stored?.rev || 0;

      // 1. Scrittura su versione superata.
      // Il client mostrava subito la copia di localStorage e restava scrivibile
      // mentre la GET era ancora in volo: un click nel primo secondo salvava la
      // configurazione vecchia sopra quella buona, e la GET (partita prima)
      // rimetteva in pagina il dato vecchio. Nessun segnale, e 113 EAN di
      // blacklist spariti in silenzio. Ora un POST che non porta la rev corrente
      // viene rifiutato e il client ricarica.
      if (stored && body.rev !== undefined && body.rev !== storedRev) {
        return json({
          error: "conflitto: la configurazione sul server e' piu' recente di quella che stai salvando",
          conflict: true, storedRev, sentRev: body.rev,
          current: { ...DEFAULTS, ...stored, rev: storedRev },
        }, 409);
      }

      // 2. La blacklist non si accorcia per sbaglio.
      // Solo un'azione esplicita (togli un EAN, svuota, pulisci i doppioni) puo'
      // ridurla: qualsiasi altro salvataggio che la trovi piu' corta e' un
      // sintomo di stato stantio, non una scelta.
      const prima = stored?.blacklist?.length || 0;
      const dopo = body.blacklist?.length || 0;
      if (prima > dopo && !body.allowBlacklistShrink) {
        const persi = (stored.blacklist || []).filter(e => !(body.blacklist || []).includes(e));
        return json({
          error: `rifiutato: la blacklist passerebbe da ${prima} a ${dopo} EAN senza che tu l'abbia chiesto`,
          blacklistShrink: { prima, dopo, persi: persi.slice(0, 20), persiTotale: persi.length },
          current: { ...DEFAULTS, ...stored, rev: storedRev },
        }, 409);
      }

      const { rev, allowBlacklistShrink, ...clean } = body;
      const next = { ...DEFAULTS, ...clean, rev: storedRev + 1, updated_at: new Date().toISOString() };
      await store.setJSON(CONFIG_KEY, next);
      return json({ ok: true, rev: next.rev });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/config" };
