/**
 * Scheduled Function — conversione automatica giornaliera.
 *
 * 1. scarica i CSV dei fornitori dall'FTP
 * 2. converte nel formato del template Amazon in uso
 * 3. GUARD-RAIL: se il risultato e' anomalo NON sovrascrive l'ultimo buono,
 *    lo mette in quarantena e alza un alert
 * 4. salva storico e alert di variazione
 *
 * Il guard-rail e' la differenza principale rispetto a prima: il risultato
 * veniva salvato subito e gli alert calcolati dopo, quindi un CSV fornitore
 * troncato produceva un file mutilato gia' pronto da caricare.
 */

import { fetchAllCSVsFromFTP } from "./_lib/ftp.mjs";
import { runConversion, safetyCheck, buildAlerts } from "./_lib/converter.mjs";
import { loadTemplate } from "./_lib/template.mjs";
import { loadAsinMap } from "./_lib/asinmap.mjs";
import { primaryCode } from "./_lib/marketplace.mjs";
import {
  resultStore, RESULT_KEY, PENDING_KEY,
  loadConfig, loadPublishedSkus, savePublishedSkus,
  saveHistoryRecord, pushAlerts, historyFrom, prevSnapshot,
} from "./_lib/stores.mjs";

export default async () => {
  const started = new Date().toISOString();
  console.log("🕐 Scheduled job avviato:", started);

  try {
    const config = await loadConfig();
    if (!config?.suppliers?.length) {
      console.warn("Nessuna configurazione trovata, skip.");
      await pushAlerts([{ type: "warning", title: "⚠️ Job saltato", message: "Nessun fornitore configurato." }]);
      return;
    }

    const marketplace = String(primaryCode(config)).toUpperCase();
    const template = await loadTemplate(config, marketplace);
    if (!template) {
      await pushAlerts([{
        type: "error", title: "❌ Template mancante",
        message: `Nessun template caricato per il marketplace ${marketplace}: non posso generare il file.`,
      }]);
      return;
    }
    if (template.isSeed) {
      console.log("Uso il template di default incluso nel repo (nessun template caricato dall'utente).");
    }

    const store = resultStore();
    const previous = await store.get(RESULT_KEY, { type: "json" });
    const previousStats = previous?.stats || null;

    console.log("📥 Scarico i CSV dall'FTP...");
    const { csvMap, problems } = await fetchAllCSVsFromFTP(config.suppliers);
    console.log(`✓ CSV scaricati: ${Object.keys(csvMap).join(", ") || "nessuno"}`);

    if (Object.keys(csvMap).length === 0) {
      await pushAlerts([{
        type: "error",
        title: "❌ Nessun CSV scaricato dall'FTP",
        message: (problems.join(" | ") || "Nessun file trovato.") + " — l'ultimo file pronto NON e' stato toccato.",
      }]);
      return;
    }

    const [publishedSkus, asinMap] = await Promise.all([loadPublishedSkus(config, marketplace), loadAsinMap(config, marketplace)]);
    console.log("⚙️ Conversione in corso...");
    const { records, stats, publishedSkus: nextPublished } = runConversion(config, csvMap, template, {
      marketplace,
      dupMode: config.dupMode || "price",
      qtyMode: config.qtyMode || "catalog",
      publishedSkus,
      asinMap,
    });
    if (problems.length) stats.errors = [...(stats.errors || []), ...problems];
    console.log(`✓ ${stats.total_products} prodotti attivi, ${stats.zeroed} disattivati, ${stats.total_rows} righe totali`);
    console.log(`  ASIN verificato su ${stats.with_asin}/${stats.total_rows} righe · senza ASIN ${stats.without_asin}${stats.unmapped_skipped ? ` · esclusi perche' non mappati ${stats.unmapped_skipped}` : ""}`);

    const payload = {
      records,
      stats,
      prev: prevSnapshot(previousStats),
      filename: `Offerte_${marketplace}_${new Date().toISOString().slice(0, 10)}`,
      source: "scheduled",
    };

    const check = safetyCheck(stats, previousStats, config);
    payload.safety = check;

    if (!check.ok) {
      await store.setJSON(PENDING_KEY, { ...payload, publishedSkus: nextPublished });
      await pushAlerts([{
        type: "error",
        title: "⏸️ Conversione bloccata dal controllo di sicurezza",
        message: check.blockers.join(" · ") + " — l'ultimo file pronto NON e' stato sovrascritto. Verifica dal tab Auto e conferma se e' corretto.",
      }]);
      await saveHistoryRecord(historyFrom(stats, "scheduled-quarantined"));
      console.warn("⏸️ Quarantena:", check.blockers.join(" | "));
      return;
    }

    await store.setJSON(RESULT_KEY, payload);
    await store.delete(PENDING_KEY).catch(() => {});
    await savePublishedSkus(nextPublished, config, marketplace);
    await saveHistoryRecord(historyFrom(stats, "scheduled"));

    const alerts = buildAlerts(stats, previousStats, config.alertThresholds);
    if (alerts.length) {
      const n = await pushAlerts(alerts);
      console.log(`⚠️ ${n} alert generati`);
    } else {
      console.log("✓ Nessuna variazione significativa");
    }

    console.log("✅ Job completato con successo");
  } catch (e) {
    console.error("❌ Errore nello scheduled job:", e.message);
    try {
      await pushAlerts([{
        type: "error",
        title: "❌ Errore nel job automatico",
        message: e.message,
      }]);
    } catch {}
  }
};

export const config = {
  // 8:15 UTC = 9:15 in ora solare, 10:15 in ora legale.
  // Cron non conosce il DST: d'estate il job parte un'ora piu' tardi. Anticiparlo
  // avrebbe senso solo dopo aver verificato a che ora Deldo pubblica il CSV,
  // altrimenti si rischia di elaborare il file del giorno prima.
  schedule: "15 8 * * 1-6", // Lun-Sab, domenica esclusa
};
