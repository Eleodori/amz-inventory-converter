/**
 * /api/ftp-convert
 *
 * GET             → ultimo risultato pubblicato + eventuale risultato in quarantena
 * GET ?rows=0     → solo statistiche, senza i record (risposta leggera)
 * POST            → scarica i CSV dall'FTP adesso, converte, applica il guard-rail
 * POST {force:1}  → converte e pubblica anche se il guard-rail blocca
 *
 * Il payload salvato contiene record compatti; il file .xlsx e il .txt li
 * costruisce il browser leggendo il template da /api/template. Cosi' le
 * functions restano leggere e non serve una libreria Excel lato server.
 */

import { fetchAllCSVsFromFTP } from "./_lib/ftp.mjs";
import { runConversion, safetyCheck } from "./_lib/converter.mjs";
import { loadTemplate } from "./_lib/template.mjs";
import {
  resultStore, RESULT_KEY, PENDING_KEY,
  loadConfig, loadPublishedSkus, savePublishedSkus,
  saveHistoryRecord, pushAlerts, historyFrom, json,
} from "./_lib/stores.mjs";

export default async (req) => {
  const store = resultStore();

  if (req.method === "GET") {
    try {
      const withRows = new URL(req.url).searchParams.get("rows") !== "0";
      const [latest, pending] = await Promise.all([
        store.get(RESULT_KEY, { type: "json" }),
        store.get(PENDING_KEY, { type: "json" }),
      ]);
      const strip = r => {
        if (!r) return null;
        if (withRows) return r;
        const { records, ...rest } = r;
        return { ...rest, rowCount: records?.length ?? 0 };
      };
      return json({ latest: strip(latest), pending: strip(pending) });
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

    const template = await loadTemplate();
    const marketplace = body.marketplace || config.marketplaces?.[0]?.code || "IT";

    const { csvMap, problems } = await fetchAllCSVsFromFTP(config.suppliers);
    if (Object.keys(csvMap).length === 0) {
      return json({ error: "Nessun CSV scaricato dall'FTP. " + (problems.join(" | ") || "Verifica le cartelle in /fornitori.") }, 400);
    }

    const publishedSkus = await loadPublishedSkus();
    const { records, stats, publishedSkus: nextPublished } = runConversion(config, csvMap, template, {
      marketplace,
      dupMode: body.dupMode || "price",
      qtyMode: body.qtyMode || config.qtyMode || "catalog",
      publishedSkus,
    });
    if (problems.length) stats.errors = [...(stats.errors || []), ...problems];

    const previous = await store.get(RESULT_KEY, { type: "json" });
    const check = safetyCheck(stats, previous?.stats, config);

    const payload = {
      records,
      stats,
      filename: `Offerte_${marketplace}_${new Date().toISOString().slice(0, 10)}`,
      safety: check,
      source: force ? "manual-forced" : "manual",
    };

    // Guard-rail: se il risultato e' sospetto non sovrascriviamo l'ultimo buono.
    if (!check.ok && !force) {
      // publishedSkus viaggia col payload in quarantena: se poi confermi da
      // /api/publish, il tracciamento delle SKU resta coerente.
      await store.setJSON(PENDING_KEY, { ...payload, publishedSkus: nextPublished });
      await pushAlerts([{
        type: "warning",
        title: "⏸️ Conversione messa in quarantena",
        message: check.blockers.join(" · ") + " — controlla e conferma dal tab Auto.",
      }]);
      return json({ ok: false, quarantined: true, safety: check, stats });
    }

    await store.setJSON(RESULT_KEY, payload);
    await store.delete(PENDING_KEY).catch(() => {});
    await savePublishedSkus(nextPublished);
    await saveHistoryRecord(historyFrom(stats, payload.source));

    return json({ ok: true, safety: check, stats });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
};

export const config = { path: "/api/ftp-convert" };
