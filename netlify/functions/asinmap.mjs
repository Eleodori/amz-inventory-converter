/**
 * /api/asinmap — mappa verificata EAN → ASIN
 *
 * GET                → statistiche della mappa in uso
 * GET ?full=1        → la mappa completa
 * POST (text/csv)    → carica/unisce il CSV prodotto da audit_asin.mjs
 *                      (colonne: ean,asin[,marca,misura,titolo_amazon])
 * POST ?replace=1    → sostituisce invece di unire
 * DELETE             → svuota la mappa (si torna all'abbinamento fatto da Amazon)
 */

import { asinMapStore, ASINMAP_KEY, json } from "./_lib/stores.mjs";
import { parseAsinMapCsv, loadAsinMap, saveAsinMap, asinMapInfo } from "./_lib/asinmap.mjs";

export default async (req) => {
  if (req.method === "GET") {
    try {
      const map = await loadAsinMap();
      const url = new URL(req.url);
      if (url.searchParams.get("full") === "1") return json({ info: asinMapInfo(map), map });
      return json({ info: asinMapInfo(map) });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "POST") {
    try {
      const url = new URL(req.url);
      const replace = url.searchParams.get("replace") === "1";
      const text = await req.text();
      if (!text?.trim()) return json({ error: "Nessun contenuto ricevuto" }, 400);

      const { map: incoming, errors, skipped } = parseAsinMapCsv(text);
      if (errors.length) return json({ error: errors.join(" · ") }, 400);
      const added = Object.keys(incoming).length;
      if (!added) return json({ error: `Nessuna coppia EAN/ASIN valida trovata (${skipped} righe scartate)` }, 400);

      const current = replace ? {} : await loadAsinMap();
      const before = Object.keys(current).length;
      let changed = 0;
      for (const [ean, v] of Object.entries(incoming)) {
        if (current[ean]?.asin && current[ean].asin !== v.asin) changed++;
        current[ean] = v;
      }
      await saveAsinMap(current);

      return json({
        ok: true,
        info: asinMapInfo(current),
        caricate: added,
        righe_scartate: skipped,
        prima: before,
        abbinamenti_cambiati: changed,
        modalita: replace ? "sostituzione" : "unione",
      });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "DELETE") {
    try {
      await asinMapStore().delete(ASINMAP_KEY);
      return json({ ok: true, info: asinMapInfo({}) });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/asinmap" };
