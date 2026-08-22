/**
 * /api/published — registro delle SKU pubblicate
 *
 * GET              → statistiche del registro
 * POST (text/tsv)  → inizializza/allinea il registro dal report "offerte attive"
 *                    di Seller Central (colonne seller-sku, asin1, item-name, price)
 * DELETE           → svuota il registro
 *
 * Perche' serve. Il registro decide quali SKU escono con quantita' 0. Finora lo
 * costruivamo dal listino del fornitore: le offerte attive su Amazon il cui EAN
 * il fornitore non manda piu' non entravano mai nel registro, quindi non venivano
 * mai disattivate e restavano acquistabili su merce non ordinabile.
 * Inizializzandolo dalle offerte attive, quelle SKU entrano nel registro e al
 * primo giro utile escono a quantita' 0.
 */

import { json } from "./_lib/stores.mjs";
import { loadPublishedSkus, savePublishedSkus, publishedStore, PUBLISHED_KEY } from "./_lib/stores.mjs";

function parseActiveOffers(text) {
  let src = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const lines = src.split(/\r?\n/).filter(l => l.trim() !== "");
  if (lines.length < 2) return { rows: [], error: "Nessuna riga di dati" };
  const delimiter = lines[0].includes("\t") ? "\t" : (lines[0].includes(";") ? ";" : ",");
  const header = lines[0].split(delimiter).map(h => h.trim().toLowerCase().replace(/^"|"$/g, ""));
  const iSku = header.findIndex(h => /^(seller-sku|sku)$/.test(h));
  const iPrice = header.findIndex(h => /^(price|prezzo)$/.test(h));
  if (iSku < 0) return { rows: [], error: 'Colonna "seller-sku" non trovata (trovate: ' + header.join(", ") + ")" };
  const rows = [];
  for (const line of lines.slice(1)) {
    const c = line.split(delimiter).map(x => x.trim().replace(/^"|"$/g, ""));
    const sku = c[iSku];
    if (!sku) continue;
    const price = iPrice >= 0 ? parseFloat(String(c[iPrice]).replace(",", ".")) : NaN;
    rows.push({ sku, price: Number.isFinite(price) ? price : null });
  }
  return { rows, error: null };
}

export default async (req) => {
  if (req.method === "GET") {
    try {
      const map = await loadPublishedSkus();
      const entries = Object.values(map);
      return json({
        info: {
          count: entries.length,
          attive: entries.filter(e => !e.zeroSince).length,
          a_zero: entries.filter(e => e.zeroSince).length,
          da_offerte_amazon: entries.filter(e => e.source === "offerte-attive").length,
        },
      });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "POST") {
    try {
      const text = await req.text();
      if (!text?.trim()) return json({ error: "Nessun contenuto ricevuto" }, 400);
      const { rows, error } = parseActiveOffers(text);
      if (error) return json({ error }, 400);
      if (!rows.length) return json({ error: "Nessuna SKU trovata nel report" }, 400);

      const map = await loadPublishedSkus();
      const today = new Date().toISOString().slice(0, 10);
      let added = 0, updated = 0;
      for (const { sku, price } of rows) {
        if (map[sku]) {
          // Gia' tracciata: aggiorniamo solo il prezzo se non lo conoscevamo.
          if (price != null && !map[sku].price) { map[sku].price = price; updated++; }
          continue;
        }
        // Attiva su Amazon ma sconosciuta al registro: la tracciamo come da
        // disattivare, cosi' al prossimo file esce con quantita' 0.
        map[sku] = {
          sku, price: price ?? 0.01,
          firstSeen: today, lastSeen: today,
          zeroSince: today, source: "offerte-attive",
        };
        added++;
      }
      await savePublishedSkus(map);
      return json({
        ok: true,
        righe_nel_report: rows.length,
        aggiunte_al_registro: added,
        prezzi_completati: updated,
        totale_registro: Object.keys(map).length,
        nota: added
          ? `${added} offerte attive su Amazon non erano tracciate: al prossimo file usciranno con quantità 0 se il fornitore non le rifornisce.`
          : "Registro già allineato con le offerte attive.",
      });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "DELETE") {
    try {
      await publishedStore().delete(PUBLISHED_KEY);
      return json({ ok: true });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/published" };
