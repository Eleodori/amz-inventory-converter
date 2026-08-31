/**
 * /api/ftp-convert
 *
 * GET                → per ogni marketplace: ultimo risultato pronto + eventuale
 *                      risultato in quarantena
 * GET ?rows=0        → solo statistiche, senza i record (risposta leggera)
 * GET ?mk=DE         → solo quel marketplace
 * POST               → scarica i CSV dall'FTP adesso e converte per TUTTI i
 *                      marketplace attivi; i CSV si scaricano una volta sola
 * POST {marketplace} → converte solo quel marketplace
 * POST {force:1}     → pubblica anche se il guard-rail blocca
 *
 * Il payload salvato contiene record compatti; il file .xlsx e il .txt li
 * costruisce il browser leggendo il template da /api/template. Cosi' le
 * functions restano leggere e non serve una libreria Excel lato server.
 */

import { fetchAllCSVsFromFTP } from "./_lib/ftp.mjs";
import { convertForMarket, loadResults } from "./_lib/convertRun.mjs";
import { activeMarketplaces, findMarketplace, primaryCode } from "./_lib/marketplace.mjs";
import { loadConfig, pushAlerts, json } from "./_lib/stores.mjs";

export default async (req) => {
  if (req.method === "GET") {
    try {
      const config = await loadConfig();
      const url = new URL(req.url);
      const withRows = url.searchParams.get("rows") !== "0";
      const solo = url.searchParams.get("mk");

      const codes = (solo ? [String(solo).toUpperCase()] : activeMarketplaces(config).map(m => String(m.code).toUpperCase()));
      if (!codes.length) return json({ marketplaces: [], primary: null, byMarket: {}, latest: null, pending: null });

      const strip = r => {
        if (!r) return null;
        if (withRows) return r;
        const { records, ...rest } = r;
        return { ...rest, rowCount: records?.length ?? 0 };
      };

      const byMarket = {};
      for (const code of codes) {
        const { latest, pending } = await loadResults(config, code);
        byMarket[code] = { latest: strip(latest), pending: strip(pending) };
      }
      const prim = String(primaryCode(config)).toUpperCase();
      return json({
        marketplaces: codes,
        primary: prim,
        byMarket,
        // Compatibilita': un client non aggiornato legge ancora latest/pending
        // del marketplace primario e continua a funzionare.
        latest: byMarket[prim]?.latest ?? null,
        pending: byMarket[prim]?.pending ?? null,
      });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method !== "POST") return json({ error: "Method not allowed" }, 405);

  try {
    const body = await req.json().catch(() => ({}));
    const force = !!body.force;

    const config = await loadConfig();
    if (!config?.suppliers?.length) {
      return json({ error: "Nessuna configurazione trovata. Configura prima i fornitori." }, 400);
    }

    let codes;
    if (body.marketplace) {
      const code = String(body.marketplace).toUpperCase();
      if (!findMarketplace(config, code)) return json({ error: `Marketplace ${code} non configurato.` }, 400);
      codes = [code];
    } else {
      codes = activeMarketplaces(config).map(m => String(m.code).toUpperCase());
    }
    if (!codes.length) return json({ error: "Nessun marketplace attivo configurato." }, 400);

    // Un solo scarico per tutti i marketplace: gli articoli del fornitore sono
    // gli stessi, e chiedere due volte lo stesso file all'FTP di Deldo
    // raddoppierebbe i tempi senza cambiare una riga del risultato.
    const { csvMap, problems } = await fetchAllCSVsFromFTP(config.suppliers);
    if (Object.keys(csvMap).length === 0) {
      return json({ error: "Nessun CSV scaricato dall'FTP. " + (problems.join(" | ") || "Verifica le cartelle in /fornitori.") }, 400);
    }

    const results = [];
    const alerts = [];
    for (const code of codes) {
      const r = await convertForMarket({
        config, code, csvMap, problems, force,
        source: "manual",
        dupMode: body.dupMode, qtyMode: body.qtyMode,
      });
      results.push(r);
      if (r.quarantined) {
        alerts.push({
          type: "warning",
          title: `⏸️ ${code}: conversione messa in quarantena`,
          message: r.safety.blockers.join(" · ") + " — controlla e conferma dal tab Auto.",
        });
      } else if (r.error) {
        alerts.push({ type: "error", title: `❌ ${code}: conversione non riuscita`, message: r.error });
      }
    }
    if (alerts.length) await pushAlerts(alerts);

    const riusciti = results.filter(r => r.ok);
    return json({
      ok: riusciti.length > 0,
      results,
      quarantined: results.some(r => r.quarantined),
      // Compatibilita' con un client non aggiornato.
      safety: results[0]?.safety ?? null,
      stats: results[0]?.stats ?? null,
    });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
};

export const config = { path: "/api/ftp-convert" };
