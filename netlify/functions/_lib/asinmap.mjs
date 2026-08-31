/**
 * Mappa verificata EAN → ASIN.
 *
 * Il template offerte permette di identificare il prodotto con l'ASIN **oppure**
 * con un identificativo esterno (EAN). Le istruzioni di Amazon sono esplicite:
 * "Ti consigliamo di usare un ASIN per assicurarti che la tua offerta appaia sul
 * prodotto giusto nel nostro negozio."
 *
 * Finche' inviavamo solo l'EAN, l'abbinamento lo decideva Amazon: su 5.571 offerte
 * attive 94 erano finite sulla pagina di un pneumatico diverso (misura o marca),
 * e due sono state vendute. Con l'ASIN esplicito l'offerta e' inchiodata al
 * prodotto giusto.
 *
 * La mappa si costruisce con audit_asin.mjs, che confronta marca e misura del
 * listino fornitore con il titolo della pagina Amazon e tiene solo le coppie
 * coerenti.
 */

import { primaryCode } from "./marketplace.mjs";
import { asinMapStore, ASINMAP_KEY , getForMarket, setForMarket } from "./stores.mjs";

const ASIN_RE = /^[A-Z0-9]{10}$/;

/**
 * Legge il CSV prodotto da audit_asin.mjs (ean,asin,marca,misura,titolo_amazon).
 * Accetta anche un CSV con solo le due colonne ean,asin.
 */
export function parseAsinMapCsv(text) {
  if (!text) return { map: {}, errors: ["File vuoto"], skipped: 0 };
  let src = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const rows = parseCsvRows(src);
  if (rows.length < 2) return { map: {}, errors: ["Nessuna riga di dati"], skipped: 0 };

  const header = rows[0].map(h => h.trim().toLowerCase());
  const iEan = header.findIndex(h => /^(ean|sku|seller-sku)$/.test(h));
  const iAsin = header.findIndex(h => /^(asin|asin1)$/.test(h));
  if (iEan < 0 || iAsin < 0) {
    return { map: {}, errors: ['Servono le colonne "ean" e "asin" (trovate: ' + header.join(", ") + ")"], skipped: 0 };
  }
  const iBrand = header.findIndex(h => /^(marca|brand)$/.test(h));
  const iSize = header.findIndex(h => /^(misura|size)$/.test(h));
  const iTitle = header.findIndex(h => /titolo|item-name|title/.test(h));

  const map = {};
  let skipped = 0;
  const now = new Date().toISOString().slice(0, 10);
  for (const r of rows.slice(1)) {
    const ean = String(r[iEan] ?? "").trim();
    const asin = String(r[iAsin] ?? "").trim().toUpperCase();
    if (!/^\d{8,14}$/.test(ean) || !ASIN_RE.test(asin)) { skipped++; continue; }
    map[ean] = {
      asin,
      brand: iBrand >= 0 ? String(r[iBrand] ?? "").trim() : undefined,
      size: iSize >= 0 ? String(r[iSize] ?? "").trim() : undefined,
      title: iTitle >= 0 ? String(r[iTitle] ?? "").trim().slice(0, 160) : undefined,
      verifiedAt: now,
    };
  }
  return { map, errors: [], skipped };
}

/** CSV con virgolette, virgola come separatore. */
function parseCsvRows(src) {
  const rows = [];
  let field = "", row = [], q = false;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (q) {
      if (ch === '"') { if (src[i + 1] === '"') { field += '"'; i++; } else q = false; }
      else field += ch;
      continue;
    }
    if (ch === '"' && field === "") { q = true; continue; }
    if (ch === ",") { row.push(field); field = ""; continue; }
    if (ch === "\r") continue;
    if (ch === "\n") { row.push(field); rows.push(row); row = []; field = ""; continue; }
    field += ch;
  }
  if (field !== "" || row.length) { row.push(field); rows.push(row); }
  return rows.filter(r => r.some(c => String(c).trim() !== ""));
}

/**
 * Mappa EAN→ASIN per marketplace. Gli ASIN esistono su entrambi i cataloghi
 * europei, ma la verifica e' stata fatta sui titoli italiani: riusarla su DE
 * senza ricontrollarla rimetterebbe in gioco proprio l'errore che la mappa
 * serve a evitare. Quindi due mappe, e la copia si fa con un'azione esplicita.
 */
export async function loadAsinMap(config, code) {
  const mk = code || primaryCode(config);
  const primary = mk === primaryCode(config);
  return (await getForMarket(asinMapStore(), ASINMAP_KEY, mk, { primary })) || {};
}

export async function saveAsinMap(map, config, code) {
  const mk = code || primaryCode(config);
  await setForMarket(asinMapStore(), ASINMAP_KEY, mk, map);
}

export function asinMapInfo(map) {
  const entries = Object.entries(map || {});
  const asins = new Set(entries.map(([, v]) => v.asin));
  // Due EAN sullo stesso ASIN: uno dei due e' necessariamente sbagliato.
  const byAsin = {};
  for (const [ean, v] of entries) (byAsin[v.asin] = byAsin[v.asin] || []).push(ean);
  const collisions = Object.entries(byAsin).filter(([, e]) => e.length > 1);
  const dates = entries.map(([, v]) => v.verifiedAt).filter(Boolean).sort();
  return {
    count: entries.length,
    distinctAsins: asins.size,
    collisions: collisions.length,
    collisionSample: collisions.slice(0, 5).map(([asin, eans]) => ({ asin, eans })),
    verifiedFrom: dates[0] || null,
    verifiedTo: dates[dates.length - 1] || null,
  };
}
