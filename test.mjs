/**
 * Test end-to-end della conversione: CSV sintetico → record → righe template
 * → .txt e .xlsx, con verifica delle posizioni delle colonne.
 * Eseguire con: node test.mjs
 */
import assert from "node:assert";
import fs from "node:fs";
import XLSX from "xlsx";
import { runConversion, expandRows, buildTxt, normalizeEAN, applyTiers, parseCSV, safetyCheck, priceColumns } from "./netlify/functions/_lib/converter.mjs";
import { seedTemplate, resolveColumns, vocabFor, vocabFromLists, extractVocabLists } from "./netlify/functions/_lib/template.mjs";
import { parseAsinMapCsv, asinMapInfo } from "./netlify/functions/_lib/asinmap.mjs";
import { keyFor, primaryCode, activeMarketplaces, findMarketplace, settingsFor, tiersFor, migrateConfig, marketplaceSummary, MK_DEFAULTS } from "./netlify/functions/_lib/marketplace.mjs";
import { getForMarket, setForMarket } from "./netlify/functions/_lib/stores.mjs";
import { prevSnapshot } from "./netlify/functions/_lib/stores.mjs";

let pass = 0, fail = 0;
const t = (name, fn) => {
  try {
    const r = fn();
    if (r && typeof r.then === "function")
      throw new Error("test asincrono passato a t(): usa `await ta(...)`, altrimenti un fallimento passa come riuscito");
    console.log("  ✓ " + name); pass++;
  } catch (e) { console.log("  ✗ " + name + "\n      " + e.message); fail++; }
};
/**
 * Variante per i test asincroni. t() non aspettava la promise: un test async che
 * lanciava un errore veniva contato come riuscito, perche' il rigetto arrivava
 * dopo che t() aveva gia' stampato la spunta. Va usata con await.
 */
const ta = async (name, fn) => {
  try { await fn(); console.log("  ✓ " + name); pass++; }
  catch (e) { console.log("  ✗ " + name + "\n      " + e.message); fail++; }
};

function ean13(base12) {
  let sum = 0;
  for (let i = 11, w = 3; i >= 0; i--, w = w === 3 ? 1 : 3) sum += Number(base12[i]) * w;
  return base12 + String((10 - (sum % 10)) % 10);
}
const E1 = ean13("318864982159"); // 3188649821594
const E2 = ean13("328634027361");
const E3 = ean13("401923878695");

console.log("\n── normalizeEAN ──");
t("accetta EAN-13 valido", () => assert.equal(normalizeEAN(E1), E1));
t("rifiuta cifra di controllo sbagliata", () => assert.equal(normalizeEAN("3188649821595"), null));
t("rifiuta non numerico", () => assert.equal(normalizeEAN("ABC1234567890"), null));
t("rifiuta lunghezza 10", () => assert.equal(normalizeEAN("1234567890"), null));
t("tollera spazi", () => assert.equal(normalizeEAN(" " + E1 + " "), E1));

console.log("\n── parseCSV con campi quotati ──");
t("campo quotato col delimitatore non slitta le colonne", () => {
  const csv = 'EAN;Descr;Price;Stock\n' + E1 + ';"Pirelli; P Zero";145,10;76\n';
  const { headers, rows } = parseCSV(csv, ";");
  assert.deepEqual(headers, ["EAN", "Descr", "Price", "Stock"]);
  assert.equal(rows[0].Descr, "Pirelli; P Zero");
  assert.equal(rows[0].Price, "145,10");
  assert.equal(rows[0].Stock, "76");
});
t("strippa il BOM", () => {
  const { headers } = parseCSV("﻿EAN;Price\n" + E1 + ";10\n", ";");
  assert.equal(headers[0], "EAN");
});

console.log("\n── applyTiers ──");
t("scaglioni disordinati danno lo stesso risultato di quelli ordinati", () => {
  const asc = [{ upTo: 50, markupPct: 18, flatFee: 7 }, { upTo: null, markupPct: 10, flatFee: 30 }];
  const desc = [...asc].reverse();
  assert.equal(applyTiers(40, asc).toFixed(4), applyTiers(40, desc).toFixed(4));
  assert.equal(applyTiers(40, asc).toFixed(2), (40 * 1.18 + 7).toFixed(2));
});

// ─── Conversione completa ─────────────────────────────────────────────────────
const template = seedTemplate();
const cols0 = resolveColumns(template).cols;
const config = {
  suppliers: [{ name: "Deldo", delimiter: ";", skuCol: "EAN", eanCol: "EAN", priceCol: "Price", stockCol: "Stock", ftpFolder: "DLD", tiers: [{ upTo: null, markupPct: 10, flatFee: 12 }] }],
  marketplaces: [{ code: "IT", quantity: 8, leadtime: 4 }],
  blacklist: [E3],
  minStock: 0, maxQty: 0, floorMarginPct: 0, zeroKeepDays: 90, minProducts: 1, maxDeltaPct: 20,
};
const csv = [
  "EAN;Descr;Price;Stock",
  `${E1};"Pirelli; P Zero";145,10;76`,
  `${E2};Michelin Primacy;201,20;46`,
  `${E3};Continental (in blacklist);100,00;10`,
  `1234567890;EAN non valido;50,00;5`,
  `${ean13("999999999999")};Prezzo zero;0;5`,
].join("\n");

console.log("\n── runConversion ──");
const r1 = runConversion(config, { Deldo: csv }, template, { marketplace: "IT", publishedSkus: {} });
t("2 prodotti validi", () => assert.equal(r1.stats.total_products, 2));
t("1 in blacklist", () => assert.equal(r1.stats.blacklisted, 1));
t("1 EAN non valido", () => assert.equal(r1.stats.bad_ean, 1));
t("1 scartato per prezzo <= 0", () => assert.equal(r1.stats.total_skipped, 2)); // bad ean + prezzo 0
t("prezzo = costo*1.10 + 12", () => {
  const rec = r1.records.find(x => x.ean === E1);
  assert.equal(rec.price.toFixed(2), (145.10 * 1.10 + 12).toFixed(2));
});
t("quantita' dal catalogo fornitore", () => assert.equal(r1.records.find(x => x.ean === E1).qty, 76));
t("nessuna riga a zero al primo giro", () => assert.equal(r1.stats.zeroed, 0));

console.log("\n── disattivazione a quantita' 0 ──");
const csvDay2 = ["EAN;Descr;Price;Stock", `${E1};Pirelli;150,00;30`].join("\n");
const r2 = runConversion(config, { Deldo: csvDay2 }, template, { marketplace: "IT", publishedSkus: r1.publishedSkus });
t("E2 sparito dal listino esce con quantita' 0", () => {
  const rec = r2.records.find(x => x.ean === E2);
  assert.ok(rec, "riga per E2 assente");
  assert.equal(rec.qty, 0);
});
t("conteggio disattivati = 1", () => assert.equal(r2.stats.zeroed, 1));
t("il prezzo della riga a zero e' l'ultimo noto", () => {
  assert.equal(r2.records.find(x => x.ean === E2).price, r1.records.find(x => x.ean === E2).price);
});
t("E1 resta attivo con la nuova quantita'", () => assert.equal(r2.records.find(x => x.ean === E1).qty, 30));
t("zeroSince tracciato", () => assert.ok(r2.publishedSkus[E2].zeroSince));

console.log("\n── zeroKeepDays ──");
const oldPub = { [E2]: { sku: E2, price: 100, zeroSince: "2020-01-01", firstSeen: "2020-01-01", lastSeen: "2020-01-01" } };
const r3 = runConversion({ ...config, zeroKeepDays: 90 }, { Deldo: csvDay2 }, template, { marketplace: "IT", publishedSkus: oldPub });
t("SKU a zero da troppo tempo esce dal tracciamento", () => {
  assert.equal(r3.stats.dropped_from_tracking, 1);
  assert.ok(!r3.records.find(x => x.ean === E2));
});

console.log("\n── minStock / maxQty / floorMarginPct ──");
const r4 = runConversion({ ...config, minStock: 50, maxQty: 4, floorMarginPct: 5 }, { Deldo: csv }, template, { marketplace: "IT", publishedSkus: {} });
t("esclude sotto la soglia di stock", () => assert.equal(r4.stats.below_min_stock, 1));
t("applica il tetto alla quantita'", () => assert.equal(r4.records[0].qty, 4));
t("popola il prezzo minimo", () => assert.equal(r4.records[0].minPrice.toFixed(2), (145.10 * 1.05).toFixed(2)));

console.log("\n── mappa EAN → ASIN ──");
const CSV_MAP = [
  "ean,asin,marca,misura,titolo_amazon",
  `${E1},B00DMCD2NU,DUNLOP,225/55/18,"Dunlop SPORT 4D XL 225/55/R18 102 H"`,
  `${E2},B0BTHGKH5C,BRIDGESTONE,235/55/18,"Bridgestone TURANZA 6"`,
  "9999999999999,NONVALIDO,X,,riga da scartare",
  "abc,B00DMCD2NU,X,,riga da scartare",
].join("\n");
const parsed = parseAsinMapCsv(CSV_MAP);
t("legge le coppie valide", () => assert.equal(Object.keys(parsed.map).length, 2));
t("scarta ASIN e EAN non validi", () => assert.equal(parsed.skipped, 2));
t("conserva marca e misura", () => assert.equal(parsed.map[E1].brand, "DUNLOP"));
t("rifiuta un CSV senza le colonne giuste", () => {
  assert.ok(parseAsinMapCsv("pippo,pluto\n1,2").errors.length > 0);
});
t("asinMapInfo trova le collisioni", () => {
  const info = asinMapInfo({ a: { asin: "B00000000A" }, b: { asin: "B00000000A" }, c: { asin: "B00000000B" } });
  assert.equal(info.count, 3);
  assert.equal(info.distinctAsins, 2);
  assert.equal(info.collisions, 1);
});

console.log("\n── ASIN nel file al posto dell'EAN ──");
const rMapped = runConversion(config, { Deldo: csv }, template, {
  marketplace: "IT", publishedSkus: {}, asinMap: parsed.map,
});
t("il record porta l'ASIN verificato", () => {
  assert.equal(rMapped.records.find(r => r.ean === E1).asin, "B00DMCD2NU");
});
t("statistiche con/senza ASIN", () => {
  assert.equal(rMapped.stats.with_asin, 2);
  assert.equal(rMapped.stats.without_asin, 0);
  assert.equal(rMapped.stats.asin_map_size, 2);
});
t("riga con ASIN: colonna ASIN piena e colonne EAN vuote", () => {
  const rows = expandRows(rMapped.records, template, { marketplace: "IT", leadtime: 4 });
  const row = rows[0];
  assert.equal(row[cols0.asin], "B00DMCD2NU");
  assert.equal(row[cols0.extId], "", "l'EAN non va inviato insieme all'ASIN");
  assert.equal(row[cols0.extIdType], "");
});
t("riga senza ASIN: torna a usare l'EAN", () => {
  const r = runConversion(config, { Deldo: csv }, template, { marketplace: "IT", publishedSkus: {}, asinMap: {} });
  const rows = expandRows(r.records, template, { marketplace: "IT", leadtime: 4 });
  assert.equal(rows[0][cols0.asin], "");
  assert.equal(rows[0][cols0.extId], r.records[0].ean);
  assert.equal(r.stats.without_asin, 2);
});
t("onlyMapped esclude gli EAN senza ASIN verificato", () => {
  const r = runConversion({ ...config, onlyMapped: true }, { Deldo: csv }, template,
    { marketplace: "IT", publishedSkus: {}, asinMap: { [E1]: { asin: "B00DMCD2NU" } } });
  assert.equal(r.records.filter(x => x.qty > 0).length, 1);
  assert.equal(r.stats.unmapped_skipped, 1);
});
t("anche la riga a quantita' 0 porta l'ASIN", () => {
  const day1 = runConversion(config, { Deldo: csv }, template, { marketplace: "IT", publishedSkus: {}, asinMap: parsed.map });
  const day2 = runConversion(config, { Deldo: csvDay2 }, template,
    { marketplace: "IT", publishedSkus: day1.publishedSkus, asinMap: parsed.map });
  const zero = day2.records.find(r => r.qty === 0);
  assert.ok(zero, "nessuna riga a zero");
  assert.equal(zero.asin, parsed.map[zero.ean].asin);
});

console.log("\n── guard-rail ──");
t("blocca il crollo di prodotti", () => {
  const c = safetyCheck({ total_products: 10, errors: [] }, { total_products: 7000 }, { maxDeltaPct: 20, minProducts: 1 });
  assert.equal(c.ok, false);
  assert.ok(/Variazione prodotti/.test(c.blockers.join(" ")));
});
t("blocca sotto il minimo di prodotti", () => {
  assert.equal(safetyCheck({ total_products: 5, errors: [] }, null, { minProducts: 100 }).ok, false);
});
t("blocca se ci sono errori CSV", () => {
  assert.equal(safetyCheck({ total_products: 7000, errors: ["Deldo: nessun CSV"] }, { total_products: 7000 }, {}).ok, false);
});
t("passa in condizioni normali", () => {
  assert.equal(safetyCheck({ total_products: 7100, errors: [] }, { total_products: 7000 }, {}).ok, true);
});

// ─── Struttura del file generato ──────────────────────────────────────────────
console.log("\n── righe nel formato template ──");
const { cols } = resolveColumns(template);
const rows = expandRows(r2.records, template, { marketplace: "IT", leadtime: 4 });
t("larghezza riga = nCols", () => assert.equal(rows[0].length, template.nCols));
t("SKU in colonna A", () => assert.equal(cols.sku, 0));
t("azione = 'Crea o modifica'", () => assert.equal(rows[0][cols.action], "Crea o modifica"));
t("tipo ID = EAN", () => assert.equal(rows[0][cols.extIdType], "EAN"));
t("condizione = Nuovo", () => assert.equal(rows[0][cols.condition], "Nuovo"));
t("canale = Gestito dal venditore (default)", () => assert.equal(rows[0][cols.channel], "Gestito dal venditore (default)"));
// Regressione dell'errore Amazon 13006: il prezzo nell'.xlsx deve essere un NUMERO.
t("prezzo e' un numero, non una stringa", () => {
  assert.equal(typeof rows[0][cols.price], "number");
  assert.ok(!/,/.test(String(rows[0][cols.price])), "il prezzo non deve contenere virgole");
});
t("priceColumns individua le colonne importo", () => {
  const pc = priceColumns(template);
  assert.deepEqual(pc, [cols.price, cols.minPrice, cols.maxPrice].filter(i => i !== undefined));
  assert.ok(pc.includes(cols.price));
});
t("quantita' e tempo di gestione numerici", () => {
  assert.equal(typeof rows[0][cols.quantity], "number");
  assert.equal(rows[0][cols.leadtime], 4);
});
t("le colonne non usate restano vuote", () => {
  const used = new Set(Object.values(cols));
  const dirty = rows[0].map((v, i) => (!used.has(i) && v !== "" ? i : null)).filter(x => x !== null);
  assert.deepEqual(dirty, []);
});

console.log("\n── .txt tab-delimited ──");
const txt = buildTxt(template, rows, { marketplace: "IT" });
const lines = txt.split("\r\n");
t("A1 conserva la stringa settings", () => assert.ok(lines[0].startsWith("settings=feedType=256")));
t("riga 1 ha settings2 e settings3", () => {
  const f = lines[0].split("\t");
  assert.ok(f[1].startsWith("settings2="), "settings2 assente");
  assert.ok(f[2].startsWith("settings3="), "settings3 assente");
});
t("riga 5 contiene i nomi tecnici dei campi", () => {
  assert.equal(lines[4].split("\t")[0], "contribution_sku#1.value");
});
t("i dati iniziano alla riga 7", () => {
  assert.equal(lines[6].split("\t")[0], r2.records[0].sku);
});
t("ogni riga ha nCols campi", () => {
  lines.filter(l => l !== "").forEach((l, i) => assert.equal(l.split("\t").length, template.nCols, "riga " + (i + 1)));
});
t("nessun tab o newline dentro i valori", () => assert.equal(lines.length, 6 + rows.length + 1));
t("nel .txt il prezzo torna testo con la virgola", () => {
  assert.match(lines[6].split("\t")[cols.price], /^\d+,\d{2}$/);
});
t("nel .txt su UK il prezzo usa il punto", () => {
  const ukRows = expandRows([{ sku: "X", ean: E1, qty: 1, price: 12.5 }], template, { marketplace: "UK", leadtime: 2 });
  const ukTxt = buildTxt(template, ukRows, { marketplace: "UK" });
  assert.equal(ukTxt.split("\r\n")[6].split("\t")[cols.price], "12.50");
});

console.log("\n── .xlsx sparso ──");
function buildXlsx(t_, dataRows) {
  const ws = {}, n = t_.nCols;
  const DEC = new Set(priceColumns(t_));
  const set = (r, c, v) => {
    if (v === "" || v === null || v === undefined) return;
    const addr = XLSX.utils.encode_cell({ r, c });
    if (typeof v === "number") ws[addr] = DEC.has(c) ? { t: "n", v, z: "0.00" } : { t: "n", v };
    else ws[addr] = { t: "s", v: String(v) };
  };
  const hdr = t_.headerRows || [];
  hdr.forEach((row, ri) => { for (let c = 0; c < n; c++) set(ri, c, row[c]); });
  dataRows.forEach((row, ri) => { for (let c = 0; c < n; c++) set(hdr.length + ri, c, row[c]); });
  ws["!ref"] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: hdr.length + dataRows.length - 1, c: n - 1 } });
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, (t_.sheetName || "Modello").slice(0, 31));
  return XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
}
const buf = buildXlsx(template, rows);
fs.writeFileSync("/tmp/test_offerte.xlsx", buf);
const back = XLSX.read(buf, { type: "buffer" });
t("foglio chiamato Modello", () => assert.deepEqual(back.SheetNames, ["Modello"]));
t("riletto: A1 = settings", () => assert.ok(String(back.Sheets.Modello["A1"].v).startsWith("settings=")));
t("riletto: A5 = contribution_sku#1.value", () => assert.equal(back.Sheets.Modello["A5"].v, "contribution_sku#1.value"));
t("riletto: A7 = primo SKU", () => assert.equal(String(back.Sheets.Modello["A7"].v), r2.records[0].sku));
t("riletto: quantita' e' numerica", () => {
  const addr = XLSX.utils.encode_cell({ r: 6, c: cols.quantity });
  assert.equal(back.Sheets.Modello[addr].t, "n");
});
t("riletto: prezzo e' una cella NUMERICA (errore 13006)", () => {
  const addr = XLSX.utils.encode_cell({ r: 6, c: cols.price });
  assert.equal(back.Sheets.Modello[addr].t, "n", "il prezzo deve essere numerico, non testo");
  assert.equal(typeof back.Sheets.Modello[addr].v, "number");
});
t("range copre tutte le colonne", () => {
  assert.equal(XLSX.utils.decode_range(back.Sheets.Modello["!ref"]).e.c, template.nCols - 1);
});

console.log("\n── confronto col file compilato a mano da Michele ──");
const REF = process.env.AMZ_TEMPLATE_REF || "fixtures/template_amazon_riferimento.xlsm";
const REF_DE = process.env.AMZ_TEMPLATE_REF_DE || "fixtures/template_amazon_de_riferimento.xlsm";
if (fs.existsSync(REF)) {
  const ref = XLSX.read(fs.readFileSync(REF), { type: "buffer", sheetRows: 8 });
  const rs = ref.Sheets["Modello"];
  const refAttrs = [];
  for (let c = 0; c < template.nCols; c++) {
    const cell = rs[XLSX.utils.encode_cell({ r: 4, c })];
    refAttrs.push(cell ? String(cell.v) : "");
  }
  t("stessa riga attributi del file di riferimento", () => {
    assert.deepEqual(refAttrs, template.headerRows[4]);
  });
  t("stesse colonne valorizzate del file di riferimento", () => {
    const refUsed = [];
    for (let c = 0; c < template.nCols; c++) {
      const cell = rs[XLSX.utils.encode_cell({ r: 6, c })];
      if (cell && cell.v !== "") refUsed.push(c);
    }
    const ourUsed = Object.values(cols).filter(i => rows[0][i] !== "").sort((a, b) => a - b);
    assert.deepEqual(ourUsed, refUsed);
  });
} else {
  console.log("  – file di riferimento non disponibile, salto");
}

console.log("\n── audit dai risultati della ricerca prodotti ──");
{
  // ListingLoader precompilato sintetico, nella forma che restituisce Seller
  // Central: 3 colonne di riferimento davanti, riga 5 gli attributi, riga 6
  // l'esempio, riga 7+ i risultati.
  const ATTRS = ["::your_search_term", "::recommended_action", "::amazon_title", "::record_action",
                 "contribution_sku#1.value", "merchant_suggested_asin#1.value"];
  const RIGHE = [
    ["BXXXXXXXXX", "Pronto per l'offerta", "Maglione da uomo", "Aggiungi prodotto", "ABC123", "B007KQBXN0"], // esempio
    [E1,             "Pronto per l'offerta", "Dunlop SPORT 4D XL 225/55/R18 102 H",       "Aggiungi prodotto", "x", "B00DMCD2NU"], // coerente
    [E2,             "Pronto per l'offerta", "Bridgestone Turanza 6 235/60 R17 102V",     "Aggiungi prodotto", "x", "B0BRSWN92Z"], // misura sbagliata
    ["B0G2MZ7SRH",   "Pronto per l'offerta", "ATLAS 195/70 R14 91T",                      "Aggiungi prodotto", "x", "B0G2MZ7SRH"], // cercato per ASIN
    [E3,             "Non disponibile",       "",                                          "Ignorato",          "x", ""],           // nessun ASIN
  ];
  const ws = {};
  const set = (r, c, v) => { if (v !== "" && v != null) ws[XLSX.utils.encode_cell({ r, c })] = { t: "s", v: String(v) } };
  set(0, 0, "settings=labelRow=4&attributeRow=5&dataRow=7&contentLanguageTag=it_IT");
  ATTRS.forEach((a, c) => set(4, c, a));
  RIGHE.forEach((row, i) => row.forEach((v, c) => set(5 + i, c, v)));
  ws["!ref"] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: 4 + RIGHE.length, c: ATTRS.length - 1 } });
  const wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, ws, "Modello");
  fs.mkdirSync("/tmp/audit-test", { recursive: true });
  XLSX.writeFile(wb, "/tmp/audit-test/prefilled.xlsx");

  // listino Deldo minimo, con le colonne che l'audit usa
  const csv = ["Article;Brand;Width;Height;Speed;Rim;Loadindex;Pattern;Stock;Price;EAN",
    `A1;DUNLOP;225;55;R;18;102H;SPORT 4D;10;100.00;${E1}`,
    `A2;BRIDGESTONE;215;65;R;17;99V;Turanza 6;10;120.00;${E2}`,
    `A3;MICHELIN;205;55;R;16;91V;Primacy 4;10;90.00;${E3}`].join("\n");
  fs.writeFileSync("/tmp/audit-test/deldo.csv", csv);

  // mappa esistente da preservare
  fs.writeFileSync("/tmp/audit-test/mappa.csv",
    'ean,asin,marca,misura,titolo_amazon\n"9999999999994","B0OLDOLD01","PIRELLI","205/55/16","vecchia coppia"\n');

  const { execFileSync } = await import("node:child_process");
  const path = await import("node:path");
  // audit_asin.mjs scrive in "report/" della CWD. Eseguendolo nella cartella del
  // repo il test sovrascriveva report/mappa_ean_asin_verificata.csv, cioe' un
  // file di dati reale, con l'output sintetico. Si esegue in una cartella isolata.
  const SCRIPT = path.resolve("audit_asin.mjs");
  const CWD = "/tmp/audit-test";
  let out = "";
  try {
    out = execFileSync(process.execPath,
      [SCRIPT, "/tmp/audit-test/prefilled.xlsx", "/tmp/audit-test/deldo.csv", "/tmp/audit-test/mappa.csv"],
      { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], cwd: CWD });
  } catch (e) { out = (e.stdout || "") + (e.stderr || "") }

  t("riconosce la sorgente e conta le coppie da verificare", () => {
    assert.match(out, /ricerca prodotti di Seller Central/);
    assert.match(out, /coppie EAN→ASIN da verificare\s*:\s*2/);
    assert.match(out, /cercate per ASIN, non per EAN \(non mappabili\): 1/);
    assert.match(out, /senza ASIN restituito da Amazon: 1/);
  });
  t("la coppia coerente entra in mappa, quella con la misura sbagliata no", () => {
    const m = fs.readFileSync(CWD + "/report/mappa_ean_asin_verificata.csv", "utf8");
    assert.ok(m.includes("B00DMCD2NU"), "la coppia coerente manca");
    assert.ok(!m.includes("B0BRSWN92Z"), "la coppia con la misura sbagliata e' entrata");
  });
  t("la mappa esistente non viene persa", () => {
    const m = fs.readFileSync(CWD + "/report/mappa_ean_asin_verificata.csv", "utf8");
    assert.ok(m.includes("B0OLDOLD01"), "le coppie preesistenti sono state buttate");
    assert.match(out, /1 esistenti \+ 1 verificate adesso/);
  });
  t("il test non tocca report/ del repo", () => {
    // Regressione: la prima versione di questo test eseguiva lo script nella
    // cartella del repo e sovrascriveva report/mappa_ean_asin_verificata.csv.
    const src = fs.readFileSync("test.mjs", "utf8");
    assert.ok(/cwd: CWD/.test(src), "lo script viene eseguito senza cwd isolata");
    assert.ok(!/readFileSync\("report\//.test(src), "il test legge ancora da report/ del repo");
  });
  t("il file di testo tab-delimited resta la sorgente di default", () => {
    // nessun .xlsx nel nome → percorso report offerte attive
    let err = "";
    try {
      execFileSync(process.execPath, [SCRIPT, "/tmp/audit-test/deldo.csv", "/tmp/audit-test/deldo.csv"],
        { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], cwd: CWD });
    } catch (e) { err = (e.stdout || "") + (e.stderr || "") }
    assert.ok(!/ricerca prodotti/.test(err), "ha usato il parser sbagliato");
  });
}

console.log("\n── handler: nessuna variabile usata prima di essere dichiarata ──");
// Scrivendo il supporto multi-marketplace ho spostato loadTemplate(config, marketplace)
// SOPRA la riga che dichiara `marketplace`: un ReferenceError che avrebbe fermato
// il job del mattino. I test non eseguono gli handler (servirebbero i blob di
// Netlify), quindi il controllo e' statico — ma fatto col parser, non con le
// espressioni regolari: la prima versione segnalava le parole nei commenti.
{
  const acorn = await import("acorn");
  const FUNZIONI = new Set(["FunctionDeclaration", "FunctionExpression", "ArrowFunctionExpression"]);

  /**
   * Un identificatore usato prima della sua dichiarazione const/let nello stesso
   * scope, o in uno scope racchiuso senza attraversare un confine di funzione.
   *
   * La prima versione raccoglieva le dichiarazioni solo dal corpo diretto della
   * funzione, e negli handler tutto il codice sta dentro un try: le dichiarazioni
   * non venivano viste e il controllo passava anche sul codice rotto. Serve una
   * pila di scope, con i blocchi che contano come scope.
   */
  function controlla(src) {
    const ast = acorn.parse(src, { ecmaVersion: 2023, sourceType: "module" });
    const problemi = [];

    const dichiarazioniDi = corpo => {
      const m = new Map();
      for (const st of corpo || []) {
        if (st.type === "VariableDeclaration" && st.kind !== "var") {
          for (const d of st.declarations)
            if (d.id?.type === "Identifier" && !m.has(d.id.name)) m.set(d.id.name, d.id.start);
        }
      }
      return m;
    };

    // pila: [{ dich: Map, funzione: bool }] dall'esterno verso l'interno
    (function vai(n, pila, salta) {
      if (!n || typeof n !== "object") return;
      if (Array.isArray(n)) { n.forEach(x => vai(x, pila, salta)); return }
      if (!n.type) return;

      if (n.type === "Identifier") {
        if (salta) return;
        for (let i = pila.length - 1; i >= 0; i--) {
          const pos = pila[i].dich.get(n.name);
          if (pos !== undefined) {
            if (n.start < pos) {
              const r = k => src.slice(0, k).split("\n").length;
              problemi.push(`"${n.name}" usata alla riga ${r(n.start)}, dichiarata alla riga ${r(pos)}`);
            }
            return;                       // trovata la dichiarazione piu' vicina
          }
          if (pila[i].funzione) return;   // oltre il confine di funzione e' una chiusura: legittimo
        }
        return;
      }

      // nuovo scope: corpo di funzione, blocco, Program
      if (n.type === "Program") { vai(n.body, [...pila, { dich: dichiarazioniDi(n.body), funzione: true }], false); return }
      if (n.type === "BlockStatement") { vai(n.body, [...pila, { dich: dichiarazioniDi(n.body), funzione: false }], false); return }
      if (FUNZIONI.has(n.type)) {
        const corpo = n.body?.type === "BlockStatement" ? n.body.body : null;
        const nuova = [...pila, { dich: corpo ? dichiarazioniDi(corpo) : new Map(), funzione: true }];
        vai(n.params, nuova, true);       // i parametri non sono usi
        if (corpo) vai(corpo, nuova, false); else vai(n.body, nuova, false);
        return;
      }

      // posizioni che non sono usi di variabile
      if (n.type === "MemberExpression") { vai(n.object, pila, salta); if (n.computed) vai(n.property, pila, salta); return }
      if (n.type === "Property") { if (n.computed) vai(n.key, pila, salta); vai(n.value, pila, salta); return }
      if (n.type === "VariableDeclarator") { vai(n.init, pila, salta); return }   // l'id non e' un uso
      if (n.type === "ImportDeclaration" || n.type === "ExportSpecifier" || n.type === "ImportSpecifier") return;
      if (n.type === "LabeledStatement" || n.type === "BreakStatement" || n.type === "ContinueStatement") return;

      for (const k of Object.keys(n)) if (k !== "type" && k !== "start" && k !== "end") vai(n[k], pila, salta);
    })(ast, [], false);

    return problemi;
  }

  // controprova: il codice rotto deve essere segnalato
  t("il controllo riconosce l'errore che avevo fatto", () => {
    const rotto = `export default async (req) => {
      const t = await carica(config, marketplace);
      const marketplace = "IT";
    };`;
    const p = controlla(rotto);
    assert.ok(p.some(x => /"marketplace" usata alla riga 2/.test(x)), "non l'ha visto: " + JSON.stringify(p));
  });
  t("il controllo non segnala una chiusura che usa una variabile dichiarata dopo", () => {
    const ok = `const f = () => tardi; const tardi = 1; export default f;`;
    assert.deepEqual(controlla(ok), []);
  });

  for (const f of fs.readdirSync("netlify/functions").filter(x => x.endsWith(".mjs"))
       .map(x => "netlify/functions/" + x)
       .concat(fs.readdirSync("netlify/functions/_lib").filter(x => x.endsWith(".mjs")).map(x => "netlify/functions/_lib/" + x))) {
    t("uso prima della dichiarazione: " + f.split("/").pop(), () => {
      const p = controlla(fs.readFileSync(f, "utf8"));
      assert.deepEqual(p, [], p.join(" · "));
    });
  }
}

console.log("\n── impostazioni per marketplace ──");
// La configurazione "storica": un solo marketplace, tutto ai livelli globali.
const CFG_VECCHIA = {
  suppliers: [{ name: "Deldo", tiers: [{ upTo: 120, markupPct: 10, flatFee: 12 }, { upTo: null, markupPct: 10, flatFee: 16 }] }],
  marketplaces: [{ code: "IT", quantity: 8, leadtime: 4 }],
  blacklist: ["1111111111116"],
  minStock: 5, maxQty: 0, floorMarginPct: 0, zeroKeepDays: 90, onlyMapped: false,
};
t("config storica: le impostazioni risolte sono quelle globali", () => {
  const s = settingsFor(CFG_VECCHIA, "IT");
  assert.equal(s.minStock, 5);
  assert.equal(s.leadtime, 4);
  assert.equal(s.zeroKeepDays, 90);
  assert.equal(s.onlyMapped, false);
  assert.deepEqual(s.blacklist, ["1111111111116"], "il primario non eredita la blacklist storica");
});
t("l'override sul marketplace vince sul globale", () => {
  const cfg = { ...CFG_VECCHIA, marketplaces: [{ code: "IT", leadtime: 4 }, { code: "DE", leadtime: 6, minStock: 10, onlyMapped: true }] };
  const it = settingsFor(cfg, "DE");
  assert.equal(it.leadtime, 6);
  assert.equal(it.minStock, 10, "non ha preso l'override");
  assert.equal(it.onlyMapped, true);
  assert.equal(settingsFor(cfg, "IT").minStock, 5, "IT ha perso il valore globale");
});
t("default quando ne' marketplace ne' config fissano il valore", () => {
  const s = settingsFor({ marketplaces: [{ code: "IT" }] }, "IT");
  assert.equal(s.minStock, MK_DEFAULTS.minStock);
  assert.equal(s.zeroKeepDays, MK_DEFAULTS.zeroKeepDays);
});
t("BLACKLIST: assente eredita, vuota resta vuota, DE non eredita mai", () => {
  const cfg = { blacklist: ["1111111111116"], marketplaces: [{ code: "IT" }, { code: "DE" }] };
  assert.deepEqual(settingsFor(cfg, "IT").blacklist, ["1111111111116"], "il primario deve ereditare");
  assert.deepEqual(settingsFor(cfg, "DE").blacklist, [], "DE ha ereditato la blacklist italiana");
  const svuotata = { blacklist: ["1111111111116"], marketplaces: [{ code: "IT", blacklist: [] }] };
  assert.deepEqual(settingsFor(svuotata, "IT").blacklist, [],
    "una blacklist svuotata a mano e' stata resuscitata dal campo storico");
});
t("marketplace sconosciuto: errore esplicito", () => {
  assert.throws(() => settingsFor(CFG_VECCHIA, "FR"), /FR non trovato/);
});
t("chiavi dei blob per marketplace", () => {
  assert.equal(keyFor("skus", "DE"), "skus-DE");
  assert.equal(keyFor("current", "it"), "current-IT", "il codice va normalizzato in maiuscolo");
  assert.throws(() => keyFor("skus", ""), /codice marketplace mancante/);
});
t("primario e marketplace attivi", () => {
  const cfg = { marketplaces: [{ code: "IT" }, { code: "DE", enabled: false }, { code: "FR" }] };
  assert.equal(primaryCode(cfg), "IT");
  assert.deepEqual(activeMarketplaces(cfg).map(m => m.code), ["IT", "FR"], "enabled:false deve escludere");
  assert.equal(findMarketplace(cfg, "de").code, "DE", "la ricerca deve ignorare il caso");
});
t("fasce per marketplace: tiersByMarket vince, altrimenti quelle del fornitore", () => {
  const sup = { name: "Deldo", tiers: [{ upTo: null, markupPct: 10, flatFee: 16 }],
                tiersByMarket: { DE: [{ upTo: null, markupPct: 6, flatFee: 8 }] } };
  assert.equal(tiersFor(sup, "DE")[0].flatFee, 8);
  assert.equal(tiersFor(sup, "IT")[0].flatFee, 16, "IT deve usare le fasce storiche");
  assert.equal(tiersFor({ ...sup, tiersByMarket: { DE: [] } }, "DE")[0].flatFee, 16,
    "una lista vuota non e' una configurazione: si torna alle fasce del fornitore");
});
t("migrateConfig sposta la blacklist sul primario ed e' idempotente", () => {
  const a = migrateConfig({ blacklist: ["1111111111116"], marketplaces: [{ code: "IT" }, { code: "DE" }] });
  assert.deepEqual(a.marketplaces[0].blacklist, ["1111111111116"]);
  assert.deepEqual(a.marketplaces[1].blacklist, []);
  const b = migrateConfig(a);
  assert.deepEqual(b.marketplaces[0].blacklist, a.marketplaces[0].blacklist);
  // una lista svuotata a mano non deve tornare indietro passando dalla migrazione
  const c = migrateConfig({ blacklist: ["1111111111116"], marketplaces: [{ code: "IT", blacklist: [] }] });
  assert.deepEqual(c.marketplaces[0].blacklist, []);
  assert.deepEqual(c.blacklist, [], "il campo storico deve seguire il primario");
});
t("migrateConfig regge input degeneri", () => {
  assert.equal(migrateConfig(null), null);
  assert.deepEqual(migrateConfig({ marketplaces: [] }).marketplaces, []);
});

console.log("\n── ricaduta sulle chiavi storiche ──");
// Store finto: getForMarket deve ricadere sulla chiave senza suffisso SOLO per
// il marketplace primario. Per la Germania una chiave mancante significa
// "non configurato", non "usa i dati italiani".
function storeFinto(contenuto) {
  return {
    dati: { ...contenuto },
    async get(k) { return k in this.dati ? this.dati[k] : null },
    async setJSON(k, v) { this.dati[k] = v },
    async delete(k) { delete this.dati[k] },
  };
}
await ta("primario: la chiave nuova manca → legge quella storica", async () => {
  const st = storeFinto({ skus: { A: 1 } });
  assert.deepEqual(await getForMarket(st, "skus", "IT", { primary: true }), { A: 1 });
});
await ta("primario: la chiave nuova esiste → vince quella nuova", async () => {
  const st = storeFinto({ skus: { A: 1 }, "skus-IT": { B: 2 } });
  assert.deepEqual(await getForMarket(st, "skus", "IT", { primary: true }), { B: 2 });
});
await ta("NON primario: mai la chiave storica", async () => {
  const st = storeFinto({ skus: { A: 1 } });
  assert.equal(await getForMarket(st, "skus", "DE", { primary: false }), null,
    "DE ha letto il registro italiano: i due cataloghi si incrocerebbero");
});
await ta("la scrittura va sempre sulla chiave del marketplace", async () => {
  const st = storeFinto({ skus: { A: 1 } });
  await setForMarket(st, "skus", "IT", { C: 3 });
  assert.deepEqual(st.dati["skus-IT"], { C: 3 });
  assert.deepEqual(st.dati["skus"], { A: 1 }, "la chiave storica non va toccata: e' la copia di sicurezza");
});
await ta("una voce nulla non blocca la ricaduta", async () => {
  const st = storeFinto({ skus: { A: 1 }, "skus-IT": null });
  assert.deepEqual(await getForMarket(st, "skus", "IT", { primary: true }), { A: 1 });
});

console.log("\n── il template deve appartenere al marketplace ──");
t("template di un altro marketplace: la generazione si ferma", () => {
  const tpl = { ...seedTemplate(), marketplaceId: "APJ6JRA9NG5V4" };
  const cfg = { ...CFG_VECCHIA, marketplaces: [{ code: "IT", marketplaceId: "APJ6JRA9NG5V4" }, { code: "DE", marketplaceId: "A1PA6795UKMFR9" }] };
  assert.throws(() => runConversion(cfg, { Deldo: "" }, tpl, { marketplace: "DE" }),
    /del marketplace APJ6JRA9NG5V4, ma stai generando per DE/);
  // sullo stesso marketplace non deve lamentarsi
  assert.doesNotThrow(() => runConversion(cfg, { Deldo: "" }, tpl, { marketplace: "IT" }));
});
t("template assente: errore chiaro invece di un file sbagliato", () => {
  assert.throws(() => runConversion(CFG_VECCHIA, { Deldo: "" }, null, { marketplace: "IT" }),
    /Nessun template caricato per il marketplace IT/);
});
t("riepilogo per marketplace", () => {
  const cfg = { ...CFG_VECCHIA, marketplaces: [{ code: "IT" }, { code: "DE", minStock: 10, enabled: false }] };
  const r = marketplaceSummary(cfg);
  assert.equal(r.length, 2);
  assert.equal(r[0].code, "IT"); assert.equal(r[0].enabled, true); assert.equal(r[0].blacklist, 1);
  assert.equal(r[1].code, "DE"); assert.equal(r[1].enabled, false); assert.equal(r[1].minStock, 10);
  assert.equal(r[1].blacklist, 0);
});

console.log("\n── due marketplace nello stesso giro ──");
{
  // Template DE vero, letto dalla fixture come lo leggerebbe il browser.
  function templateDE() {
    const F = "fixtures/template_amazon_de_riferimento.xlsm";
    if (!fs.existsSync(F)) return null;
    const wb = XLSX.read(fs.readFileSync(F), { type: "buffer" });
    const sh = wb.Sheets["Vorlage"], rng = XLSX.utils.decode_range(sh["!ref"]);
    const at = (r, c) => { const k = XLSX.utils.encode_cell({ r, c }); return sh[k] ? String(sh[k].v) : "" };
    let nCols = 0;
    for (const ri of [3, 4]) for (let c = rng.e.c; c >= 0; c--) if (at(ri, c).trim() !== "") { nCols = Math.max(nCols, c + 1); break }
    const pad = r => { const a = []; for (let c = 0; c < nCols; c++) a.push(at(r, c)); return a };
    return {
      source: "ListingLoader.xlsm", sheetName: "Vorlage", nCols,
      labelRow: 4, attributeRow: 5, dataRow: 7,
      marketplaceId: "A1PA6795UKMFR9", contentLanguageTag: "de_DE",
      vocabLists: extractVocabLists(wb, XLSX),
      headerRows: [pad(0), pad(1), pad(2), pad(3), pad(4), pad(5)],
    };
  }

  const tplIT = seedTemplate();
  const tplDE = templateDE();

  const CSV2 = [
    "sku;ean;stock;price",
    `A1;${E1};40;100.00`,   // costo 100 → sotto la soglia 120
    `A2;${E2};40;200.00`,   // costo 200 → sopra la soglia
    `A3;${E3};40;150.00`,
  ].join("\n");

  const CFG2 = {
    suppliers: [{
      name: "Deldo", delimiter: ";", skuCol: "sku", eanCol: "ean", stockCol: "stock", priceCol: "price",
      tiers: [{ upTo: 120, markupPct: 10, flatFee: 12 }, { upTo: null, markupPct: 10, flatFee: 16 }],
      // La Germania e' piu' competitiva: fasce piu' basse.
      tiersByMarket: { DE: [{ upTo: null, markupPct: 5, flatFee: 5 }] },
    }],
    marketplaces: [
      { code: "IT", quantity: 8, leadtime: 4, marketplaceId: "APJ6JRA9NG5V4", blacklist: [E3] },
      { code: "DE", quantity: 6, leadtime: 6, marketplaceId: "A1PA6795UKMFR9", blacklist: [E1] },
    ],
    minStock: 0, maxQty: 0, floorMarginPct: 0, zeroKeepDays: 90, minProducts: 1, maxDeltaPct: 100,
  };

  const it = runConversion(CFG2, { Deldo: CSV2 }, tplIT, { marketplace: "IT", publishedSkus: {}, asinMap: {} });
  const de = tplDE ? runConversion(CFG2, { Deldo: CSV2 }, tplDE, { marketplace: "DE", publishedSkus: {}, asinMap: {} }) : null;

  t("le fasce per marketplace producono prezzi diversi", () => {
    const pIT = new Map(it.records.map(r => [r.ean, r.price]));
    assert.equal(pIT.get(E1), 122, "IT: 100 → 10% + 12");
    assert.equal(pIT.get(E2), 236, "IT: 200 → 10% + 16");
    if (!de) return;
    const pDE = new Map(de.records.map(r => [r.ean, r.price]));
    assert.equal(pDE.get(E2), 215, "DE: 200 → 5% + 5");
    assert.equal(pDE.get(E3), 162.5, "DE: 150 → 5% + 5");
  });
  t("ogni marketplace applica la SUA blacklist", () => {
    const skuIT = it.records.map(r => r.ean);
    assert.ok(!skuIT.includes(E3), "IT non ha rispettato la sua blacklist");
    assert.ok(skuIT.includes(E1), "IT ha bloccato un EAN della blacklist tedesca");
    if (!de) return;
    const skuDE = de.records.map(r => r.ean);
    assert.ok(!skuDE.includes(E1), "DE non ha rispettato la sua blacklist");
    assert.ok(skuDE.includes(E3), "DE ha bloccato un EAN della blacklist italiana");
  });
  t("il tempo di gestione e' quello del marketplace", () => {
    const rowsIT = expandRows(it.records, tplIT, { marketplace: "IT", leadtime: settingsFor(CFG2, "IT").leadtime });
    const colsIT = resolveColumns(tplIT).cols;
    assert.equal(rowsIT[0][colsIT.leadtime], 4);
    if (!de) return;
    const rowsDE = expandRows(de.records, tplDE, { marketplace: "DE", leadtime: settingsFor(CFG2, "DE").leadtime });
    const colsDE = resolveColumns(tplDE).cols;
    assert.equal(rowsDE[0][colsDE.leadtime], 6);
  });
  t("le righe DE usano il vocabolario tedesco e la colonna prezzo DE", () => {
    if (!de) { console.log("      (template DE assente, salto)"); return }
    const cols = resolveColumns(tplDE).cols;
    const row = expandRows(de.records, tplDE, { marketplace: "DE", leadtime: 6 })[0];
    assert.equal(row[cols.action], "Erstellen oder Bearbeiten");
    assert.equal(row[cols.condition], "Neu");
    assert.equal(row[cols.channel], "Versand durch Händler (Standard)");
    assert.match(tplDE.headerRows[4][cols.price], /marketplace_id=A1PA6795UKMFR9/);
  });
  t("i registri sono indipendenti: quello IT non spegne le offerte DE", () => {
    if (!de) return;
    // Su IT c'e' una SKU storica che il fornitore non manda piu': va a zero.
    const regIT = { "9999999999994": { sku: "9999999999994", price: 50, firstSeen: "2026-01-01", lastSeen: "2026-01-01" } };
    const itZ = runConversion(CFG2, { Deldo: CSV2 }, tplIT, { marketplace: "IT", publishedSkus: regIT, asinMap: {} });
    assert.equal(itZ.stats.zeroed, 1, "IT non ha disattivato la SKU storica");
    assert.ok(itZ.records.some(r => r.ean === "9999999999994" && r.qty === 0));
    // Lo stesso registro NON deve arrivare a DE: passandogli il suo, vuoto,
    // la Germania non deve avere nessuna disattivazione.
    const deZ = runConversion(CFG2, { Deldo: CSV2 }, tplDE, { marketplace: "DE", publishedSkus: {}, asinMap: {} });
    assert.equal(deZ.stats.zeroed, 0, "DE ha ereditato disattivazioni dal registro italiano");
  });
  t("le mappe ASIN sono indipendenti", () => {
    if (!de) return;
    const mappaIT = { [E2]: { asin: "B00ITITIT1" } };
    const a = runConversion(CFG2, { Deldo: CSV2 }, tplIT, { marketplace: "IT", publishedSkus: {}, asinMap: mappaIT });
    assert.equal(a.records.find(r => r.ean === E2).asin, "B00ITITIT1");
    assert.equal(a.stats.with_asin, 1);
    // A DE si passa la sua mappa, vuota: nessuna riga pinnata
    const b = runConversion(CFG2, { Deldo: CSV2 }, tplDE, { marketplace: "DE", publishedSkus: {}, asinMap: {} });
    assert.equal(b.stats.with_asin, 0, "DE ha usato la mappa italiana");
  });
  t("statistiche etichettate col marketplace giusto", () => {
    assert.equal(it.stats.marketplace, "IT");
    if (de) assert.equal(de.stats.marketplace, "DE");
  });
}

console.log("\n── vocabolario preso dal template, non indovinato ──");
t("dal template IT esce esattamente il vocabolario italiano", () => {
  const v = vocabFor(seedTemplate());
  assert.equal(v.create, "Crea o modifica");
  assert.equal(v.delete, "Elimina");
  assert.equal(v.ean, "EAN");
  assert.equal(v.conditionNew, "Nuovo");
  assert.equal(v.channelMerchant, "Gestito dal venditore (default)");
  assert.equal(v.origine, "template", "sta ancora usando la tabella hardcodata");
});
t("le liste si estraggono dal foglio Dropdown Lists del file reale", () => {
  if (!fs.existsSync(REF)) { console.log("      (file di riferimento assente, salto)"); return }
  const wb = XLSX.read(fs.readFileSync(REF), { type: "buffer" });
  const l = extractVocabLists(wb, XLSX);
  assert.ok(l, "nessuna lista estratta");
  assert.deepEqual(l.action, ["Crea o modifica", "Elimina"]);
  assert.equal(l.condition[0], "Nuovo");
  assert.equal(l.channel[0], "Gestito dal venditore (default)");
  assert.ok(l.extIdType.includes("EAN"));
});
t("template DE reale: il vocabolario esce giusto dal file", () => {
  if (!fs.existsSync(REF_DE)) { console.log("      (template DE assente, salto)"); return }
  const wb = XLSX.read(fs.readFileSync(REF_DE), { type: "buffer" });
  const l = extractVocabLists(wb, XLSX);
  assert.ok(l, "nessuna lista estratta dal template DE");
  const sh = wb.Sheets["Vorlage"];
  const rng = XLSX.utils.decode_range(sh["!ref"]);
  const at = (r, c) => { const k = XLSX.utils.encode_cell({ r, c }); return sh[k] ? String(sh[k].v).trim() : "" };
  const attrs = []; for (let c = 0; c <= rng.e.c; c++) attrs.push(at(4, c));
  const esempi = []; for (let c = 0; c <= rng.e.c; c++) esempi.push(at(5, c));
  const t2 = { nCols: attrs.length, labelRow: 4, attributeRow: 5, dataRow: 7, sheetName: "Vorlage",
               marketplaceId: "A1PA6795UKMFR9", contentLanguageTag: "de_DE", vocabLists: l,
               headerRows: [[], [], [], [], attrs, esempi] };
  const v = vocabFor(t2);
  assert.equal(v.origine, "template");
  assert.equal(v.create, "Erstellen oder Bearbeiten");
  assert.equal(v.delete, "Löschen");
  assert.equal(v.conditionNew, "Neu");
  assert.equal(v.ean, "EAN");
  assert.equal(v.channelMerchant, "Versand durch Händler (Standard)");
  // e le colonne si risolvono senza toccare i pattern
  const { missing, cols } = resolveColumns(t2);
  assert.deepEqual(missing, [], "colonne obbligatorie mancanti sul template DE");
  assert.match(attrs[cols.price], /marketplace_id=A1PA6795UKMFR9/);
});
t("LA TRAPPOLA: la lista del canale su DE e' rovesciata rispetto a IT", () => {
  // IT: ["Gestito dal venditore (default)", "Logistica di Amazon (UE)"]
  // DE: ["Versand durch Amazon (EU)", "Versand durch Händler (Standard)"]
  // Prendere il primo elemento darebbe Logistica di Amazon su tutte le offerte,
  // con zero merce nei magazzini Amazon.
  const de = vocabFromLists({
    action: ["Erstellen oder Bearbeiten", "Löschen"],
    condition: ["Neu", "Gebraucht"],
    channel: ["Versand durch Amazon (EU)", "Versand durch Händler (Standard)"],
    extIdType: ["EAN", "GTIN"],
  });
  assert.equal(de.channelMerchant, "Versand durch Händler (Standard)");
  assert.ok(!/amazon/i.test(de.channelMerchant));
  const it = vocabFromLists({
    action: ["Crea o modifica", "Elimina"],
    condition: ["Nuovo"],
    channel: ["Gestito dal venditore (default)", "Logistica di Amazon (UE)"],
    extIdType: ["EAN"],
  });
  assert.equal(it.channelMerchant, "Gestito dal venditore (default)");
});
t("canale ambiguo: si rifiuta di indovinare", () => {
  const base = { action: ["a","b"], condition: ["c"], extIdType: ["EAN"] };
  assert.equal(vocabFromLists({ ...base, channel: ["Amazon EU", "Amazon Prime"] }), null, "due opzioni Amazon: doveva fermarsi");
  assert.equal(vocabFromLists({ ...base, channel: ["Venditore", "Händler"] }), null, "nessuna opzione Amazon: doveva fermarsi");
});
t("la riga di esempio disambigua la creazione", () => {
  const l = { action: ["Löschen", "Erstellen oder Bearbeiten"],   // ordine invertito
              condition: ["Gebraucht", "Neu"], channel: ["Händler", "Amazon"], extIdType: ["EAN"] };
  const v = vocabFromLists(l, { action: "(Standard) Erstellen oder Bearbeiten", condition: "Neu" });
  assert.equal(v.create, "Erstellen oder Bearbeiten", "non ha usato la riga di esempio");
  assert.equal(v.delete, "Löschen");
  assert.equal(v.conditionNew, "Neu");
});
t("il convertitore si ferma se il canale nomina Amazon", () => {
  const tpl = seedTemplate();
  const rotto = { ...tpl, vocabLists: undefined, contentLanguageTag: "xx_XX" };
  // forziamo un vocabolario sbagliato passando per il fallback e sovrascrivendo
  const cols = resolveColumns(tpl).cols;
  let errore = null;
  try {
    expandRows([{ sku: "1", ean: "1", qty: 1, price: 10 }], tpl,
      { vocab: { create: "X", delete: "Y", ean: "EAN", conditionNew: "Nuovo", channelMerchant: "Logistica di Amazon (UE)" } });
  } catch (e) { errore = e.message }
  assert.ok(errore && /nomina Amazon/.test(errore), "non si e' fermato: " + errore);
});

t("liste incomplete: si ripiega sulla tabella e lo dichiara", () => {
  assert.equal(vocabFromLists(null), null);
  assert.equal(vocabFromLists({ action: ["solo uno"], condition: ["x"], channel: ["y"], extIdType: ["EAN"] }), null,
    "con una sola azione non si puo' distinguere creazione e cancellazione");
  assert.equal(vocabFromLists({ action: ["a","b"], condition: ["x"], channel: ["y"], extIdType: ["UPC"] }), null,
    "senza EAN nella lista non possiamo identificare il prodotto");
  const v = vocabFor({ contentLanguageTag: "de_DE" });
  assert.ok(v.origine.startsWith("tabella:"), "non dichiara che sta indovinando");
});
t("lingua sconosciuta: ripiega sull'italiano dicendolo", () => {
  const v = vocabFor({ contentLanguageTag: "pl_PL" });
  assert.equal(v.create, "Crea o modifica");
  assert.ok(/lingua non riconosciuta/.test(v.origine));
});
t("il seed porta le liste con se'", () => {
  const t2 = seedTemplate();
  assert.ok(t2.vocabLists, "il seed non ha vocabLists: su un template vecchio si torna a indovinare");
  assert.equal(t2.vocabLists.condition.length, 13);
});

console.log("\n── /api/config: scritture stantie e blacklist che si accorcia ──");
// Riproduzione della tabella di decisione dell'handler POST. La corsa vera:
// il client mostrava subito la copia di localStorage ed era gia' scrivibile
// mentre la GET era in volo; un click nel primo secondo salvava la config
// vecchia sopra quella buona, e 113 EAN di blacklist sparivano in silenzio.
function decidi(stored, body) {
  const storedRev = stored?.rev || 0;
  if (stored && body.rev !== undefined && body.rev !== storedRev) return { esito: "conflitto", storedRev };
  const prima = stored?.blacklist?.length || 0;
  const dopo = body.blacklist?.length || 0;
  if (prima > dopo && !body.allowBlacklistShrink) return { esito: "shrink", prima, dopo };
  return { esito: "ok", rev: storedRev + 1 };
}
const SRV = { rev: 7, blacklist: ["a", "b", "c"] };
t("scrittura con la rev corrente passa", () => {
  assert.equal(decidi(SRV, { rev: 7, blacklist: ["a","b","c","d"] }).esito, "ok");
});
t("scrittura con rev vecchia viene rifiutata", () => {
  const d = decidi(SRV, { rev: 3, blacklist: ["a","b","c","d"] });
  assert.equal(d.esito, "conflitto");
  assert.equal(d.storedRev, 7);
});
t("blacklist piu' corta senza intento: rifiutata", () => {
  const d = decidi(SRV, { rev: 7, blacklist: ["a"] });
  assert.equal(d.esito, "shrink");
  assert.equal(d.prima, 3); assert.equal(d.dopo, 1);
});
t("blacklist piu' corta con intento esplicito: passa", () => {
  assert.equal(decidi(SRV, { rev: 7, blacklist: ["a"], allowBlacklistShrink: true }).esito, "ok");
});
t("un salvataggio da un altro tab che non tocca la blacklist passa", () => {
  assert.equal(decidi(SRV, { rev: 7, blacklist: ["a","b","c"], minStock: 8 }).esito, "ok");
});
t("primo salvataggio in assoluto (server vuoto) passa", () => {
  assert.equal(decidi(null, { blacklist: [] }).esito, "ok");
});
t("la rev avanza a ogni scrittura accettata", () => {
  assert.equal(decidi(SRV, { rev: 7, blacklist: ["a","b","c"] }).rev, 8);
});
t("l'handler e il client hanno davvero le guardie", () => {
  const srv = fs.readFileSync("netlify/functions/config.mjs", "utf8");
  assert.ok(/body\.rev !== storedRev/.test(srv), "config.mjs non controlla la rev");
  assert.ok(/!body\.allowBlacklistShrink/.test(srv), "config.mjs non protegge la blacklist");
  assert.ok(/rev: storedRev \+ 1/.test(srv), "config.mjs non incrementa la rev");
  const html = fs.readFileSync("index.html", "utf8");
  assert.ok(/if\(!pronto\)\{/.test(html), "il client scrive ancora prima della sincronizzazione");
  assert.ok(/allowBlacklistShrink:!!opts\.shrinkOk/.test(html), "il client non dichiara l'intento di ridurre");
  assert.ok(/\{shrinkOk:true\}/.test(html), "nessuna azione dichiara l'intento di ridurre");
  assert.ok(/r\.status===409/.test(html), "il client non gestisce il rifiuto del server");
});

console.log("\n── blacklist: separatori e duplicati ──");
t("le righe senza ASIN a quantita' 0 non contano come righe in lotteria", () => {
  // Un file con "solo ASIN verificato" attivo puo' contenere righe col solo EAN:
  // sono le disattivazioni. Contarle fra le righe che vanno in vendita col solo
  // EAN faceva comparire l'avviso arancione su un file corretto.
  const recs = [
    { ean: "1", asin: "B01", qty: 5 },
    { ean: "2", asin: "B02", qty: 0 },
    { ean: "3", qty: 0 },              // disattivazione, senza ASIN
    { ean: "4", qty: 0 },
    { ean: "5", qty: 3 },              // questa si' e' in lotteria
  ];
  const without_asin = recs.filter(r => !r.asin).length;
  const active = recs.filter(r => !r.asin && Number(r.qty) > 0).length;
  const zeroed = recs.filter(r => !r.asin && !(Number(r.qty) > 0)).length;
  assert.equal(without_asin, 3);
  assert.equal(active, 1);
  assert.equal(zeroed, 2);
  assert.equal(active + zeroed, without_asin);
  const conv = fs.readFileSync("netlify/functions/_lib/converter.mjs", "utf8");
  assert.ok(/without_asin_active: records\.filter\(r => !r\.asin && Number\(r\.qty\) > 0\)\.length/.test(conv), "il converter non separa le righe senza ASIN in vendita");
  assert.ok(/without_asin_zeroed/.test(conv), "manca il conteggio delle disattivazioni senza ASIN");
  const st = fs.readFileSync("netlify/functions/_lib/stores.mjs", "utf8");
  assert.ok(/without_asin_active: stats\.without_asin_active/.test(st), "prevSnapshot non porta il nuovo campo: il confronto col giorno prima resterebbe disomogeneo");
  const html = fs.readFileSync("index.html", "utf8");
  assert.ok(/without_asin_active/.test(html), "il banner non usa il conteggio delle sole righe in vendita");
  // La frase che dava per certa una causa che non possiamo conoscere.
  assert.ok(!/Sono EAN nuovi che il fornitore ha iniziato a mandare/.test(html), "il banner afferma ancora una causa non verificabile");
});

t("il design non ha piu' colori scritti a mano nei componenti", () => {
  const html = fs.readFileSync("index.html", "utf8");
  // I quattro colori dell'app vecchia erano ripetuti 85 volte come esadecimali:
  // non cambiavano col tema e in chiaro alcuni erano illeggibili.
  for (const hex of ["#6366f1", "#10b981", "#ef4444", "#f59e0b"])
    assert.ok(!html.includes(hex), `il colore ${hex} e' ancora scritto a mano`);
  // Il carattere di sistema e le cifre tabulari sono il cuore del restyle.
  assert.ok(/--font:-apple-system/.test(html), "manca il carattere di sistema");
  assert.ok(/font-variant-numeric:tabular-nums/.test(html), "mancano le cifre a larghezza fissa");
  // Le etichette maiuscole spaziate sono state tolte da tutti i campi.
  assert.ok(!/textTransform:"uppercase"/.test(html), "restano etichette in maiuscolo spaziato");
  // I tab sono un controllo segmentato con lo stato leggibile da chi non vede.
  assert.ok(/className="seg" role="tab" aria-selected=\{tab===t\.id\}/.test(html), "i tab non dichiarano quale e' selezionato");
});

t("l'inventory rimanda alla pagina fatture, e le fatture tornano indietro", () => {
  const idx = fs.readFileSync("index.html", "utf8");
  assert.ok(/<a href="fatture\.html" className="navlink"/.test(idx), "manca il link alle fatture nell'header");
  assert.ok(/\.navlink\{color:var\(--ghost-text\)/.test(idx), "manca lo stile .navlink");
  // La regola non deve finire dentro la media query: ci era finita alla prima
  // stesura e il pulsante restava senza bordo sopra i 600px.
  const css = idx.match(/<style>([\s\S]*?)<\/style>/)[1];
  // Il punto di rottura puo' cambiare: cerchiamo la prima media query "stretta"
  // qualunque sia la soglia. La regola non deve finirci dentro, altrimenti il
  // pulsante resta senza bordo sui monitor.
  const iNav = css.indexOf(".navlink{"), iMedia = css.search(/@media\(max-width:/);
  assert.ok(iNav > 0, "manca la regola .navlink");
  assert.ok(iMedia > 0, "nessuna media query per lo schermo stretto");
  assert.ok(iNav < iMedia, ".navlink e' dentro la media query dello schermo stretto");
  assert.equal(css.split("{").length, css.split("}").length, "graffe del CSS non bilanciate");
  const fat = fs.readFileSync("fatture.html", "utf8");
  assert.ok(/<a href="index\.html" className="navlink"/.test(fat), "manca il ritorno all'inventory");
  // Le due pagine usano chiavi localStorage diverse: l'archivio fatture non
  // deve poter sovrascrivere la configurazione dell'inventory.
  assert.ok(/const INV_KEY="gw-invoicing-v1"/.test(fat), "chiave dell'archivio fatture cambiata");
  assert.ok(!/localStorage\.setItem\("amz-config/.test(fat), "la pagina fatture scrive sulla chiave dell'inventory");
});

t("l'archivio fatture si puo' esportare e reimportare", () => {
  const fat = fs.readFileSync("fatture.html", "utf8");
  assert.ok(/function ArchivioCard/.test(fat), "manca la scheda dell'archivio");
  assert.ok(/download=`fatturazione_/.test(fat), "manca l'esportazione");
  assert.ok(/_formato:"gw-invoicing-v1"/.test(fat), "l'esportazione non marca il formato");
  assert.ok(/accept=".json"/.test(fat), "manca l'importazione");
  // L'unione non deve duplicare ne' far tornare indietro la numerazione.
  assert.ok(/numeri\.has\(String\(x\.number\)\)/.test(fat), "l'unione non deduplica per numero fattura");
  assert.ok(/Math\.max\(Number\(inv\.lastInvoiceNumber\)/.test(fat), "la numerazione puo' tornare indietro");
  // riproduzione della logica di unione, tenuta in sincrono col commento sopra
  const unisci = (qui, dal) => {
    const numeri = new Set(qui.map(x => String(x.number)));
    return [...qui, ...dal.filter(x => !numeri.has(String(x.number)))];
  };
  const qui = [{ number: "2026/1" }, { number: "2026/2" }];
  assert.equal(unisci(qui, [{ number: "2026/1" }, { number: "2026/2" }]).length, 2, "reimportare lo stesso file duplica");
  assert.equal(unisci(qui, [{ number: "2026/7" }]).length, 3, "un archivio diverso non si unisce");
});

t("il tab ASIN offre la sostituzione della mappa, non solo l'unione", () => {
  const html = fs.readFileSync("index.html", "utf8");
  const tab = html.slice(html.indexOf("function AsinTab"), html.indexOf("// ─── Tab Regole") > 0 ? html.indexOf("// ─── Tab Regole") : html.indexOf("function RulesTab"));
  // L'interfaccia passava sempre replace=false: per togliere una coppia dalla
  // mappa non c'era modo se non svuotarla, e nel frattempo un file generato con
  // "solo ASIN verificato" sarebbe uscito con zero prodotti.
  assert.ok(!/upload\("map",e\.target\.files\[0\],false\)/.test(tab), "il caricamento della mappa è ancora fisso su unione");
  assert.ok(/upload\("map",e\.target\.files\[0\],sostituisci\)/.test(tab), "non passa la modalità scelta");
  assert.ok(/setSostituisci/.test(tab), "manca la spunta per sostituire");
  assert.ok(/replace\?"&replace=1":""/.test(tab), "non manda replace=1 all'API");
});

t("l'esito di un caricamento non viene azzerato dal ricarico", () => {
  const html = fs.readFileSync("index.html", "utf8");
  const tab = html.slice(html.indexOf("function AsinTab"), html.indexOf("function RulesTab"));
  // load() comincia con setMsg(null): impostare il messaggio PRIMA di await
  // load() lo faceva sparire, e un caricamento sembrava non aver fatto niente.
  const iAwait = tab.indexOf("await load();\n      setMsg(esito)");
  assert.ok(iAwait > 0, "il messaggio non viene impostato dopo load()");
  assert.ok(!/setMsg\(`Mappa /.test(tab), "imposta ancora il messaggio prima del ricarico");
});

t("lo split della blacklist accetta qualsiasi separatore", () => {
  const html = fs.readFileSync("index.html", "utf8");
  const tab = html.slice(html.indexOf("function BlacklistTab"), html.indexOf("// ─── ConvertTab"));
  // Il vecchio /[\n,;\s]+/ non spezzava su "x" moltiplicativo: una lista
  // incollata con quel separatore diventava un unico token accettato perche'
  // lungo >= 8, e la blacklist non bloccava piu' niente.
  assert.ok(!/inp\.split\(\/\[\\n,;\\s\]\+\/\)/.test(tab), "usa ancora il vecchio split");
  assert.ok(/inp\.split\(\/\\D\+\/\)/.test(tab), "non spezza su tutti i non-cifra");
  assert.ok(/normalizeEAN\(g\)/.test(tab), "non valida gli EAN prima di inserirli");
  // La lista non arriva piu' da c.blacklist ma da mkSettings(c,code): e' per
  // marketplace. La dedupe in scrittura deve restare comunque.
  assert.ok(/new Set\(\[\.\.\.lista,\.\.\.nuovi\]\)/.test(tab), "non deduplica in scrittura");
  assert.ok(/mkSettings\(c,code\)\.blacklist/.test(tab), "non legge la blacklist del marketplace scelto");
  assert.ok(/salvaLista\(/.test(tab), "non scrive sul marketplace scelto");
  assert.ok(!/e\.length>=8/.test(tab), "accetta ancora qualsiasi token lungo 8+");
});
t("la logica di add non duplica: stessa lista due volte", () => {
  // riproduzione della logica del tab, tenuta in sincrono col commento sopra
  const norm = e => (/^\d+$/.test(e) && [8,12,13,14].includes(e.length)) ? e : null;
  const run = (lista, esistenti) => {
    const presenti = new Set(esistenti), visti = new Set(), nuovi = [];
    let gia = 0, rip = 0;
    for (const g of lista.split(/\D+/).filter(Boolean)) {
      const e = norm(g); if (!e) continue;
      if (visti.has(e)) { rip++; continue }
      visti.add(e);
      if (presenti.has(e)) { gia++; continue }
      nuovi.push(e);
    }
    return { nuovi, gia, rip, finale: [...new Set([...esistenti, ...nuovi])] };
  };
  const L = "3286342052717\u00d78808563590424\u00d73286342052717"; // uno ripetuto
  const a = run(L, []);
  assert.deepEqual(a.nuovi, ["3286342052717", "8808563590424"]);
  assert.equal(a.rip, 1);
  assert.equal(a.finale.length, 2);
  const b = run(L, a.finale);       // stessa lista incollata di nuovo
  assert.equal(b.nuovi.length, 0);
  assert.equal(b.gia, 2);
  assert.equal(b.finale.length, 2); // niente duplicati
});

console.log("\n── confronto col file precedente (banner solo-EAN) ──");
t("prevSnapshot e' null-safe al primo giro", () => {
  assert.equal(prevSnapshot(null), null);
  assert.equal(prevSnapshot(undefined), null);
});
t("prevSnapshot porta i campi del confronto", () => {
  const snap = prevSnapshot({ without_asin: 3280, with_asin: 4800, total_rows: 8080, total_products: 7739, altro: "ignorato" });
  assert.equal(snap.without_asin, 3280);
  assert.equal(snap.with_asin, 4800);
  assert.equal(snap.total_rows, 8080);
  assert.equal(snap.total_products, 7739);
  assert.equal(snap.altro, undefined);
});
t("prevSnapshot mette null sui campi assenti, non undefined", () => {
  const snap = prevSnapshot({ total_rows: 10 });
  assert.equal(snap.without_asin, null);
  assert.ok("without_asin" in snap);
});
t("il banner non e' piu' arancione a prescindere", () => {
  const html = fs.readFileSync("index.html", "utf8");
  // la vecchia versione apriva la Card con accent fisso subito dopo il test su without_asin
  assert.ok(!/without_asin>0&&\(\s*<Card accent="#f59e0b"/.test(html),
    "il banner usa ancora un accent arancione fisso");
  assert.ok(html.includes("prev?.without_asin"), "il banner non legge il valore precedente");
});
t("la conversione vive in un solo posto e salva prev", () => {
  // Prima la sequenza era duplicata nei due handler: una correzione applicata a
  // una sola delle due copie faceva divergere il file notturno da quello
  // generato a mano. Ora sta in convertRun.mjs e gli handler la chiamano.
  const run = fs.readFileSync("netlify/functions/_lib/convertRun.mjs", "utf8");
  assert.ok(/prev: prevSnapshot\(/.test(run), "convertRun non salva prev");
  assert.ok(/safetyCheck\(stats, previous\?\.stats, mk\)/.test(run),
    "il guard-rail non confronta con il precedente dello stesso marketplace");
  for (const f of ["netlify/functions/ftp-convert.mjs", "netlify/functions/scheduled-convert.mjs"]) {
    const src = fs.readFileSync(f, "utf8");
    assert.ok(/convertForMarket\(/.test(src), f + " non usa la routine condivisa");
    assert.ok(!/runConversion\(/.test(src), f + " chiama ancora runConversion direttamente: la logica e' di nuovo duplicata");
    assert.ok(/fetchAllCSVsFromFTP\(config\.suppliers\)/.test(src), f + " non scarica i CSV");
    assert.equal((src.match(/fetchAllCSVsFromFTP\(/g) || []).length, 1,
      f + ": i CSV vanno scaricati una volta sola, non per marketplace");
  }
});

console.log(`\n${fail === 0 ? "✅" : "❌"}  ${pass} test passati, ${fail} falliti\n`);
process.exit(fail ? 1 : 0);
