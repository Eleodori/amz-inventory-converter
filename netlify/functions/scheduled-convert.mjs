/**
 * Scheduled Function — conversione automatica giornaliera.
 *
 * 1. scarica i CSV dei fornitori dall'FTP, UNA volta sola
 * 2. per ogni marketplace attivo converte nel formato del suo template
 * 3. GUARD-RAIL: se un risultato e' anomalo NON sovrascrive l'ultimo buono di
 *    quel marketplace, lo mette in quarantena e alza un alert
 * 4. salva storico e alert di variazione, separati per marketplace
 *
 * Un marketplace che fallisce non ferma gli altri: il file italiano deve uscire
 * anche se quello tedesco ha un template mancante o sbagliato.
 */

import { fetchAllCSVsFromFTP } from "./_lib/ftp.mjs";
import { buildAlerts } from "./_lib/converter.mjs";
import { convertForMarket, loadResults } from "./_lib/convertRun.mjs";
import { activeMarketplaces, settingsFor } from "./_lib/marketplace.mjs";
import { loadConfig, pushAlerts } from "./_lib/stores.mjs";

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

    const marketplaces = activeMarketplaces(config);
    if (!marketplaces.length) {
      await pushAlerts([{ type: "warning", title: "⚠️ Job saltato", message: "Nessun marketplace attivo configurato." }]);
      return;
    }
    console.log("Marketplace attivi:", marketplaces.map(m => m.code).join(", "));

    console.log("📥 Scarico i CSV dall'FTP...");
    const { csvMap, problems } = await fetchAllCSVsFromFTP(config.suppliers);
    console.log(`✓ CSV scaricati: ${Object.keys(csvMap).join(", ") || "nessuno"}`);

    if (Object.keys(csvMap).length === 0) {
      await pushAlerts([{
        type: "error",
        title: "❌ Nessun CSV scaricato dall'FTP",
        message: (problems.join(" | ") || "Nessun file trovato.") + " — nessun file pronto e' stato toccato.",
      }]);
      return;
    }

    const alerts = [];
    for (const m of marketplaces) {
      const code = String(m.code).toUpperCase();
      // Le statistiche precedenti servono per gli alert di variazione, e vanno
      // lette PRIMA della conversione, che sovrascrive l'ultimo risultato.
      const { latest: prima } = await loadResults(config, code);
      const r = await convertForMarket({ config, code, csvMap, problems, source: "scheduled" });

      if (r.error) {
        console.error(`❌ ${code}: ${r.error}`);
        alerts.push({ type: "error", title: `❌ ${code}: conversione non riuscita`, message: r.error });
        continue;
      }
      if (r.quarantined) {
        console.warn(`⏸️ ${code} in quarantena:`, r.safety.blockers.join(" | "));
        alerts.push({
          type: "error",
          title: `⏸️ ${code}: conversione bloccata dal controllo di sicurezza`,
          message: r.safety.blockers.join(" · ") + ` — l'ultimo file pronto di ${code} NON e' stato sovrascritto. Verifica dal tab Auto e conferma se e' corretto.`,
        });
        continue;
      }

      const s = r.stats;
      console.log(`✓ ${code}: ${s.total_products} prodotti attivi, ${s.zeroed} disattivati, ${s.total_rows} righe`);
      console.log(`  ASIN verificato su ${s.with_asin}/${s.total_rows} · senza ASIN ${s.without_asin}${s.unmapped_skipped ? ` · esclusi perche' non mappati ${s.unmapped_skipped}` : ""}`);

      const mk = settingsFor(config, code);
      for (const a of buildAlerts(s, prima?.stats, mk.alertThresholds)) {
        alerts.push({ ...a, title: `[${code}] ${a.title}` });
      }
    }

    if (alerts.length) {
      const n = await pushAlerts(alerts);
      console.log(`⚠️ ${n} alert generati`);
    } else {
      console.log("✓ Nessuna variazione significativa");
    }

    console.log("✅ Job completato");
  } catch (e) {
    console.error("❌ Errore nello scheduled job:", e.message);
    try {
      await pushAlerts([{ type: "error", title: "❌ Errore nel job automatico", message: e.message }]);
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
