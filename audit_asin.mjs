/**
 * Verifica l'abbinamento EAN → ASIN.
 *
 * Per ogni offerta attiva confronta marca e misura del titolo della pagina Amazon
 * con quelle che il listino Deldo associa a quell'EAN. Se non coincidono,
 * l'offerta e' su una pagina prodotto sbagliata: il cliente compra un pneumatico
 * diverso da quello che spediremmo.
 *
 * Approccio: invece di "interpretare" il titolo (i venditori li scrivono in venti
 * formati diversi: 225/55/R18, 235/60Hr20, 60/265/R18, 205/65R16C...), estraiamo
 * TUTTE le misure plausibili presenti nel titolo e verifichiamo se quella di Deldo
 * e' fra loro. Riduce quasi a zero i falsi allarmi.
 *
 * Due sorgenti possibili per il primo argomento:
 *
 *   node audit_asin.mjs <report-offerte-attive.txt> <listino-deldo.csv>
 *       verifica le offerte GIA' attive su Amazon.
 *
 *   node audit_asin.mjs <ListingLoader-precompilato.xlsm> <listino-deldo.csv> [mappa-esistente.csv]
 *       verifica i risultati della RICERCA PRODOTTI di Seller Central. Il file
 *       precompilato che Amazon restituisce dopo una ricerca per EAN contiene
 *       ::your_search_term (l'EAN cercato), merchant_suggested_asin#1.value
 *       (l'ASIN) e ::amazon_title (il titolo): esattamente cio' che serve per
 *       mappare gli EAN che non hanno ancora un ASIN, senza farlo a mano.
 *       Il terzo argomento, se presente, e' la mappa verificata esistente: le
 *       coppie nuove ci vengono unite invece di sostituirla.
 *
 * Amazon stessa, nelle istruzioni di quel file, scrive: "Ti consigliamo di
 * verificare i dettagli precompilati per assicurarti che la tua offerta appaia
 * sul prodotto giusto nel nostro negozio." Questo script e' quella verifica.
 */
import fs from "node:fs";

const [, , OFF_PATH, DELDO_PATH, MAP_PATH] = process.argv;

if (!OFF_PATH || !DELDO_PATH) {
  console.error("Uso: node audit_asin.mjs <offerte-attive.txt | ListingLoader-precompilato.xlsm> <listino-deldo.csv> [mappa-esistente.csv]");
  process.exit(1);
}

function readDelimited(path, delimiter) {
  let text = fs.readFileSync(path, "utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  const lines = text.split(/\r?\n/).filter(l => l.trim() !== "");
  const headers = lines[0].split(delimiter).map(h => h.trim());
  return lines.slice(1).map(line => {
    const cols = line.split(delimiter);
    const o = {};
    headers.forEach((h, i) => { o[h] = (cols[i] ?? "").trim(); });
    return o;
  });
}

// ─── Marche ───────────────────────────────────────────────────────────────────
/** Abbreviazioni usate nei titoli Amazon per la stessa marca. */
const BRAND_ALIASES = {
  BRIDGESTONE: ["BSTONE", "BRIDGESTON", "BR"],
  CONTINENTAL: ["CONTI"],
  GOODYEAR: ["GY"],
  MICHELIN: ["MICH"],
  PIRELLI: ["PIRELL"],
  VREDESTEIN: ["VREDE"],
  HANKOOK: ["HANK"],
  UNIROYAL: ["UNIROY"],
  FIRESTONE: ["FSTONE"],
};
const letters = s => String(s || "").toUpperCase().replace(/[^A-Z]/g, "");
const deldoBrand = b => String(b || "").trim().split(/\s+/)[0].toUpperCase();

function brandInTitle(brand, title) {
  const T = letters(title);
  if (!brand) return null;
  const names = [brand, ...(BRAND_ALIASES[brand] || [])];
  return names.some(n => T.includes(letters(n)));
}

// ─── Misure ───────────────────────────────────────────────────────────────────
const plausible = (w, h, r) => w >= 125 && w <= 405 && h >= 25 && h <= 85 && r >= 12 && r <= 24;

/**
 * Tutte le misure plausibili contenute nel titolo, incluse le scritture
 * "235/60Hr20" (codice velocità in mezzo) e "60/265/R18" (numeri invertiti).
 */
function sizesInTitle(title) {
  const t = String(title || "");
  const found = new Set();
  const push = (a, b, c) => {
    if (plausible(a, b, c)) found.add(`${a}/${b}/${c}`);
    else if (plausible(b, a, c)) found.add(`${b}/${a}/${c}`); // titolo con i primi due invertiti
  };
  // a/b <eventuali lettere> <eventuale R> c
  const re = /(\d{2,3})\s*[\/\-]\s*(\d{2,3})\s*[\/\s\-]*[A-Z]{0,4}\s*[\/\s\-]*(\d{2})(?!\d)/gi;
  let m;
  while ((m = re.exec(t)) !== null) push(+m[1], +m[2], +m[3]);
  return found;
}

const deldoSizeKey = d => {
  const w = parseInt(d.Width, 10), h = parseInt(d.Height, 10), r = parseInt(d.Rim, 10);
  return (w && h && r) ? `${w}/${h}/${r}` : null;
};
/** Indice di carico: "102H", "(108W)", "107/105T" → prende il primo numero. */
const deldoLoad = d => {
  const m = String(d.Loadindex || "").replace(/[()]/g, "").match(/(\d{2,3})/);
  return m ? +m[1] : null;
};

// ─── Caricamento ──────────────────────────────────────────────────────────────

/** true se la stringa e' un EAN/GTIN plausibile (solo cifre, lunghezza nota). */
const isEan = v => /^\d+$/.test(String(v || "")) && [8, 12, 13, 14].includes(String(v).length);

/**
 * Legge il ListingLoader precompilato dalla ricerca prodotti di Seller Central
 * e lo riporta alla stessa forma del report offerte attive, cosi' la
 * classificazione qui sotto non cambia di una riga.
 *
 * Le righe cercate per ASIN invece che per EAN non sono mappabili (non sappiamo
 * a quale EAN corrispondono) e vengono contate a parte, non scartate in silenzio.
 */
let searchedByAsin = 0, searchNoAsin = 0;
async function loadSearchResults(path) {
  const XLSX = (await import("xlsx")).default;
  const wb = XLSX.read(fs.readFileSync(path), { type: "buffer" });
  // Cerchiamo la riga che contiene ::your_search_term E l'ASIN suggerito: il
  // foglio "Definizioni dati" nomina ::your_search_term in una colonna di
  // descrizioni e da solo trarrebbe in inganno.
  let sheet = null, attrRow = -1;
  for (const n of wb.SheetNames) {
    const sh = wb.Sheets[n];
    if (!sh || !sh["!ref"]) continue;
    const rng = XLSX.utils.decode_range(sh["!ref"]);
    for (let r = 0; r <= Math.min(rng.e.r, 8); r++) {
      let hasSearch = false, hasAsin = false;
      for (let c = 0; c <= rng.e.c; c++) {
        const k = XLSX.utils.encode_cell({ r, c });
        const v = sh[k] ? String(sh[k].v).trim() : "";
        if (v === "::your_search_term") hasSearch = true;
        else if (v === "merchant_suggested_asin#1.value") hasAsin = true;
      }
      if (hasSearch && hasAsin) { sheet = n; attrRow = r; break; }
    }
    if (sheet) break;
  }
  if (!sheet) {
    console.error(`In ${path} non trovo la colonna "::your_search_term": non e' un ListingLoader precompilato da una ricerca prodotti.`);
    process.exit(1);
  }
  const sh = wb.Sheets[sheet], rng = XLSX.utils.decode_range(sh["!ref"]);
  const at = (r, c) => { const k = XLSX.utils.encode_cell({ r, c }); return sh[k] ? String(sh[k].v).trim() : ""; };
  const attrs = []; for (let c = 0; c <= rng.e.c; c++) attrs.push(at(attrRow, c));
  const col = name => attrs.indexOf(name);
  const iSearch = col("::your_search_term"), iAsin = col("merchant_suggested_asin#1.value"), iTitle = col("::amazon_title");
  if (iAsin < 0 || iTitle < 0) {
    console.error("Nel file mancano merchant_suggested_asin#1.value o ::amazon_title.");
    process.exit(1);
  }
  // La riga di esempio (quella con "BXXXXXXXXX" / "ABC123") non e' un risultato:
  // si parte da dataRow dichiarato in A1, o dalla riga dopo l'esempio.
  let a1 = "";
  for (const c of ["A1", "B1", "C1"]) { const k = sh[c]; if (k) a1 += String(k.v); }
  const mDataRow = a1.match(/dataRow=(\d+)/);
  const firstData = mDataRow ? parseInt(mDataRow[1], 10) - 1 : attrRow + 2;

  const out = [];
  for (let r = firstData; r <= rng.e.r; r++) {
    const term = at(r, iSearch), asin = at(r, iAsin), title = at(r, iTitle);
    if (!term && !asin) continue;
    if (!/^[A-Z0-9]{10}$/.test(asin)) { if (term) searchNoAsin++; continue; }
    if (!isEan(term)) { searchedByAsin++; continue; }   // riga di esempio o ricerca per ASIN
    out.push({ "seller-sku": term, asin1: asin, "item-name": title, price: "" });
  }
  return out;
}

const fromSearch = /\.xlsm?$|\.xlsx$/i.test(OFF_PATH);
const offers = fromSearch ? await loadSearchResults(OFF_PATH) : readDelimited(OFF_PATH, "\t");
if (fromSearch) {
  console.log(`Sorgente: risultati della ricerca prodotti di Seller Central (${OFF_PATH})`);
  console.log(`  coppie EAN→ASIN da verificare : ${offers.length}`);
  if (searchedByAsin) console.log(`  righe cercate per ASIN, non per EAN (non mappabili): ${searchedByAsin}`);
  if (searchNoAsin) console.log(`  righe senza ASIN restituito da Amazon: ${searchNoAsin}`);
  if (offers.length === 0) {
    console.log("\nNessuna coppia EAN→ASIN da verificare.");
    if (searchedByAsin) console.log("Questo file viene da una ricerca fatta per ASIN: per mappare gli EAN la ricerca va rifatta incollando gli EAN.");
    process.exit(0);
  }
}
const deldoRows = readDelimited(DELDO_PATH, ";");
const deldo = new Map();
for (const r of deldoRows) if (r.EAN) deldo.set(r.EAN, r);

// ─── Classificazione ──────────────────────────────────────────────────────────
const G = { ok: [], sizeWrong: [], brandWrong: [], brandOmitted: [], loadWrong: [], noSizeInTitle: [], noDeldo: [] };

for (const o of offers) {
  const sku = o["seller-sku"], asin = o.asin1, title = o["item-name"], price = o.price;
  const d = deldo.get(sku);
  if (!d) { G.noDeldo.push({ sku, asin, title, price }); continue; }

  const dKey = deldoSizeKey(d);
  const brand = deldoBrand(d.Brand);
  const rec = {
    sku, asin, price, title,
    deldo: `${brand} ${d.Pattern} ${d.Width}/${d.Height} R${d.Rim} ${d.Loadindex}`.replace(/\s+/g, " ").trim(),
    dKey, brand, cost: d.Price,
  };

  const titleSizes = sizesInTitle(title);
  const brandOk = brandInTitle(brand, title);

  if (!dKey || titleSizes.size === 0) {
    rec.motivo = brandOk === false ? "titolo senza misura e marca diversa" : "titolo senza misura leggibile";
    G.noSizeInTitle.push(rec);
    continue;
  }
  if (!titleSizes.has(dKey)) {
    rec.motivo = "misura diversa";
    rec.titleSizes = [...titleSizes].join(" ");
    G.sizeWrong.push(rec);
    continue;
  }
  if (brandOk === false) {
    // Molti titoli non ripetono la marca ma citano il battistrada ("Blizzak 6",
    // "PZero", "Cinturato"): con la misura giusta e il battistrada giusto
    // l'abbinamento e' corretto, il titolo e' solo sintetico.
    const pat = String(d.Pattern || "").replace(/[^A-Za-z0-9]/g, " ").split(/\s+/)
      .filter(w => w.length >= 4).map(letters);
    const T = letters(title);
    const patternOk = pat.length > 0 && pat.some(w => T.includes(w));
    if (!patternOk) {
      rec.motivo = "marca diversa";
      G.brandWrong.push(rec);
      continue;
    }
    rec.motivo = "marca non citata nel titolo (battistrada e misura coincidono)";
    G.brandOmitted = G.brandOmitted || [];
    G.brandOmitted.push(rec);
    continue;
  }
  // La portata puo' essere scritta "102H" o doppia "112/110R": accettiamo entrambe.
  const dl = deldoLoad(d);
  const titleHasLoad = /\b\d{2,3}\s*(?:[\/]\s*\d{2,3}\s*)?[A-Z]\b/.test(title);
  const dlPresent = dl !== null && new RegExp(`\\b${dl}\\s*(?:[\/]\\s*\\d{2,3}\\s*)?[A-Z]?\\b`, "i").test(title);
  if (dl && titleHasLoad && !dlPresent) {
    rec.motivo = "indice di carico diverso";
    G.loadWrong.push(rec);
    continue;
  }
  G.ok.push(rec);
}

// ─── Collisioni ───────────────────────────────────────────────────────────────
const byAsin = new Map();
for (const o of offers) {
  if (!byAsin.has(o.asin1)) byAsin.set(o.asin1, []);
  byAsin.get(o.asin1).push(o["seller-sku"]);
}
const collisions = [...byAsin.entries()].filter(([, s]) => s.length > 1);

// ─── Report ───────────────────────────────────────────────────────────────────
const pct = n => offers.length ? (n / offers.length * 100).toFixed(1) + "%" : "—";
const line = (lbl, n, note = "") => console.log(`  ${lbl.padEnd(34)}${String(n).padStart(5)}  ${pct(n).padStart(6)}  ${note}`);
console.log(`offerte attive: ${offers.length} · listino Deldo: ${deldo.size} EAN\n`);
console.log("╔═══ ESITO ═══╗");
line("✓ abbinamento coerente", G.ok.length);
line("✗ MISURA DIVERSA", G.sizeWrong.length, "← vende il pneumatico sbagliato");
line("✗ MARCA DIVERSA", G.brandWrong.length, "← vende il pneumatico sbagliato");
line("~ marca non citata nel titolo", (G.brandOmitted||[]).length, "misura e battistrada coincidono → ok");
line("⚠ indice di carico diverso", G.loadWrong.length, "portata diversa");
line("? titolo senza misura", G.noSizeInTitle.length, "non verificabile dal titolo");
line("– EAN non nel listino Deldo", G.noDeldo.length, "offerte non piu' rifornite");
console.log(`\n  ASIN con più di una nostra offerta: ${collisions.length}`);

const show = (label, arr, n = 10) => {
  if (!arr.length) return;
  console.log(`\n╔═══ ${label} — primi ${Math.min(n, arr.length)} di ${arr.length} ═══╗`);
  arr.slice(0, n).forEach(r => {
    console.log(`  ${r.sku}  ${r.asin}  venduto a ${r.price} €  (costo Deldo ${r.cost} €)`);
    console.log(`     spediremmo : ${r.deldo}`);
    console.log(`     pagina dice: ${String(r.title).slice(0, 92)}`);
  });
};
show("MISURA DIVERSA", G.sizeWrong);
show("MARCA DIVERSA", G.brandWrong);
show("INDICE DI CARICO DIVERSO", G.loadWrong, 5);
show("TITOLO SENZA MISURA + MARCA DIVERSA", G.noSizeInTitle.filter(r => r.motivo.includes("marca")), 5);

if (collisions.length) {
  console.log(`\n╔═══ COLLISIONI: due nostre offerte sullo stesso ASIN ═══╗`);
  collisions.forEach(([asin, skus]) => {
    console.log(`  ${asin} — ${offers.find(o => o.asin1 === asin)["item-name"].slice(0, 72)}`);
    skus.forEach(s => {
      const d = deldo.get(s);
      console.log(`     ${s} → ${d ? deldoBrand(d.Brand) + " " + d.Width + "/" + d.Height + " R" + d.Rim : "?"}`);
    });
  });
}

// ─── Export ───────────────────────────────────────────────────────────────────
fs.mkdirSync("report", { recursive: true });
const esc = v => `"${String(v ?? "").replace(/"/g, '""')}"`;
const HEAD = "ean,asin,motivo,prezzo_amazon,costo_deldo,cosa_spediremmo,cosa_dice_la_pagina,link_sku_central\n";
const toCsv = arr => HEAD + arr.map(r => [r.sku, r.asin, r.motivo, r.price, r.cost, r.deldo, r.title,
  `https://sellercentral.amazon.it/skucentral?mSku=${r.sku}&condition=New`].map(esc).join(",")).join("\n") + "\n";

const wrong = [...G.sizeWrong, ...G.brandWrong];
fs.writeFileSync("report/asin_sbagliati.csv", toCsv(wrong));
fs.writeFileSync("report/asin_carico_diverso.csv", toCsv(G.loadWrong));
fs.writeFileSync("report/asin_non_verificabili.csv", toCsv(G.noSizeInTitle));
// Offerte attive il cui EAN non e' piu' nel listino Deldo: il tool non le conosce
// (il registro e' stato inizializzato dal listino), quindi non le azzerera' mai.
fs.writeFileSync("report/ean_non_piu_forniti.csv",
  "ean,asin,prezzo_amazon,titolo_amazon,link_sku_central\n" +
  G.noDeldo.map(r => [r.sku, r.asin, r.price, r.title,
    `https://sellercentral.amazon.it/skucentral?mSku=${r.sku}&condition=New`].map(esc).join(",")).join("\n") + "\n");
fs.writeFileSync("report/blacklist_asin_sbagliati.txt",
  [...new Set([...wrong, ...G.loadWrong].map(r => r.sku))].sort().join("\n") + "\n");
console.log(`\nscritti in report/:`);
console.log(`  asin_sbagliati.csv          ${wrong.length}`);
console.log(`  asin_carico_diverso.csv     ${G.loadWrong.length}`);
console.log(`  asin_non_verificabili.csv   ${G.noSizeInTitle.length}`);
console.log(`  ean_non_piu_forniti.csv     ${G.noDeldo.length}`);
console.log(`  blacklist_asin_sbagliati.txt ${new Set([...wrong, ...G.loadWrong].map(r => r.sku)).size} EAN da bloccare`);

// Mappa verificata: 1 EAN → 1 ASIN, marca e misura confermate dal titolo.
// E' la base per pinnare l'ASIN nel file quotidiano invece di lasciare indovinare Amazon.
// La mappa verificata. Se e' stata passata una mappa esistente le coppie nuove
// ci vengono UNITE: verificando i risultati di una ricerca su 600 EAN non si
// devono perdere le 4.800 coppie gia' buone.
const nuove = new Map();
for (const r of [...G.ok, ...(G.brandOmitted || [])]) nuove.set(r.sku, [r.sku, r.asin, r.brand, r.dKey, r.title]);

let esistenti = new Map(), conflitti = [];
if (MAP_PATH) {
  if (!fs.existsSync(MAP_PATH)) { console.error(`Mappa esistente non trovata: ${MAP_PATH}`); process.exit(1); }
  const righe = fs.readFileSync(MAP_PATH, "utf8").split(/\r?\n/).slice(1).filter(l => l.trim());
  for (const l of righe) {
    const campi = l.split(/","/).map(x => x.replace(/^"|"$/g, ""));
    if (campi.length >= 2 && campi[0]) esistenti.set(campi[0], campi);
  }
  for (const [ean, r] of nuove) {
    const vecchia = esistenti.get(ean);
    if (vecchia && vecchia[1] !== r[1]) conflitti.push({ ean, prima: vecchia[1], adesso: r[1] });
  }
}
const unita = new Map([...esistenti, ...nuove]);   // le nuove vincono sui doppioni

fs.writeFileSync("report/mappa_ean_asin_verificata.csv",
  "ean,asin,marca,misura,titolo_amazon\n" +
  [...unita.values()].map(c => c.map(esc).join(",")).join("\n") + "\n");
console.log(`  mappa_ean_asin_verificata.csv ${unita.size} coppie` +
  (MAP_PATH ? `  (${esistenti.size} esistenti + ${nuove.size} verificate adesso, ${nuove.size - [...nuove.keys()].filter(k => !esistenti.has(k)).length} gia' presenti)` : ` verificate`));
if (conflitti.length) {
  console.log(`\n  ⚠️  ${conflitti.length} EAN cambiano ASIN rispetto alla mappa esistente — controllali a mano:`);
  for (const c of conflitti.slice(0, 15)) console.log(`      ${c.ean}  ${c.prima} → ${c.adesso}`);
  if (conflitti.length > 15) console.log(`      … e altri ${conflitti.length - 15}`);
  fs.writeFileSync("report/asin_cambiati.csv",
    "ean,asin_prima,asin_adesso\n" + conflitti.map(c => [c.ean, c.prima, c.adesso].map(esc).join(",")).join("\n") + "\n");
}

console.log("\n╔═══ I DUE EAN DEGLI ORDINI ═══╗");
for (const sku of ["3286342052717", "8808563590424"]) {
  const o = offers.find(x => x["seller-sku"] === sku);
  const d = deldo.get(sku);
  console.log(`  ${sku}`);
  console.log(`     pagina Amazon : ${o ? o.asin1 + "  " + o.price + " €  " + o["item-name"].slice(0, 78) : "non attiva"}`);
  console.log(`     spediremmo    : ${d ? deldoBrand(d.Brand) + " " + d.Pattern + " " + d.Width + "/" + d.Height + " R" + d.Rim + " " + d.Loadindex + "   costo " + d.Price + " €" : "non nel listino"}`);
}
