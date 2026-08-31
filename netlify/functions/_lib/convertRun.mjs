/**
 * Una conversione per un marketplace, dall'inizio alla fine.
 *
 * Sta qui e non nei due handler perche' il job automatico e il pulsante "Genera
 * adesso" devono fare esattamente la stessa cosa. Finche' erano due copie della
 * stessa sequenza, una correzione applicata a una sola delle due passava
 * inosservata: il file generato a mano e quello notturno divergevano.
 *
 * I CSV dei fornitori si scaricano UNA volta sola e si passano qui: sono gli
 * stessi per tutti i marketplace, e scaricarli due volte raddoppierebbe il tempo
 * del job e il carico sull'FTP di Deldo per niente.
 */

import { runConversion, safetyCheck } from "./converter.mjs";
import { loadTemplate } from "./template.mjs";
import { loadAsinMap } from "./asinmap.mjs";
import { settingsFor, primaryCode } from "./marketplace.mjs";
import {
  resultStore, RESULT_KEY, PENDING_KEY,
  loadPublishedSkus, savePublishedSkus,
  saveHistoryRecord, historyFrom, prevSnapshot,
  getForMarket, setForMarket, delForMarket,
} from "./stores.mjs";

/** Ultimo risultato pronto e risultato in quarantena, per un marketplace. */
export async function loadResults(config, code) {
  const store = resultStore();
  const primary = String(code).toUpperCase() === String(primaryCode(config)).toUpperCase();
  const [latest, pending] = await Promise.all([
    getForMarket(store, RESULT_KEY, code, { primary }),
    getForMarket(store, PENDING_KEY, code, { primary }),
  ]);
  return { latest: latest || null, pending: pending || null };
}

/**
 * @param opts.config    configurazione (gia' migrata)
 * @param opts.code      codice marketplace
 * @param opts.csvMap    { fornitore: testoCSV } — scaricati una volta sola
 * @param opts.problems  problemi incontrati sull'FTP, da allegare alle statistiche
 * @param opts.force     true = pubblica anche se il guard-rail blocca
 * @param opts.source    etichetta per lo storico ("scheduled" | "manual" | ...)
 * @returns { marketplace, ok, quarantined, stats, safety, error }
 */
export async function convertForMarket({ config, code, csvMap, problems = [], force = false, source = "manual", dupMode, qtyMode }) {
  const marketplace = String(code).toUpperCase();
  const store = resultStore();

  const template = await loadTemplate(config, marketplace);
  if (!template) {
    return {
      marketplace, ok: false, quarantined: false,
      error: `Nessun template caricato per ${marketplace}. Scaricalo da Seller Central con il marketplace impostato su ${marketplace} e caricalo dal tab Template.`,
    };
  }

  const mk = settingsFor(config, marketplace);
  const [publishedSkus, asinMap] = await Promise.all([
    loadPublishedSkus(config, marketplace),
    loadAsinMap(config, marketplace),
  ]);

  let records, stats, nextPublished;
  try {
    ({ records, stats, publishedSkus: nextPublished } = runConversion(config, csvMap, template, {
      marketplace,
      dupMode: dupMode || mk.dupMode,
      qtyMode: qtyMode || mk.qtyMode,
      publishedSkus,
      asinMap,
    }));
  } catch (e) {
    // Un marketplace che fallisce non deve fermare gli altri: il file italiano
    // deve uscire anche se quello tedesco ha un template sbagliato.
    return { marketplace, ok: false, quarantined: false, error: e.message };
  }
  if (problems.length) stats.errors = [...(stats.errors || []), ...problems];

  const previous = await getForMarket(store, RESULT_KEY, marketplace, {
    primary: marketplace === String(primaryCode(config)).toUpperCase(),
  });

  // Il guard-rail confronta con il file precedente DELLO STESSO marketplace:
  // confrontare il primo file tedesco con l'ultimo italiano darebbe una
  // variazione del 100% e bloccherebbe sempre.
  const check = safetyCheck(stats, previous?.stats, mk);

  const payload = {
    records, stats,
    marketplace,
    prev: prevSnapshot(previous?.stats),
    filename: `Offerte_${marketplace}_${new Date().toISOString().slice(0, 10)}`,
    safety: check,
    source: force ? `${source}-forced` : source,
  };

  if (!check.ok && !force) {
    // publishedSkus viaggia col payload in quarantena: se poi si conferma da
    // /api/publish, il tracciamento delle SKU resta coerente.
    await setForMarket(store, PENDING_KEY, marketplace, { ...payload, publishedSkus: nextPublished });
    await saveHistoryRecord(historyFrom(stats, `${source}-quarantined`));
    return { marketplace, ok: false, quarantined: true, safety: check, stats };
  }

  await setForMarket(store, RESULT_KEY, marketplace, payload);
  await delForMarket(store, PENDING_KEY, marketplace);
  await savePublishedSkus(nextPublished, config, marketplace);
  await saveHistoryRecord(historyFrom(stats, payload.source));

  return { marketplace, ok: true, quarantined: false, safety: check, stats };
}
