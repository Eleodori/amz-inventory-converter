/**
 * Test end-to-end della conversione: CSV sintetico → record → righe template
 * → .txt e .xlsx, con verifica delle posizioni delle colonne.
 * Eseguire con: node test.mjs
 */
import assert from "node:assert";
import fs from "node:fs";
import XLSX from "xlsx";
import { runConversion, expandRows, buildTxt, normalizeEAN, applyTiers, parseCSV, safetyCheck, priceColumns } from "./netlify/functions/_lib/converter.mjs";
import { seedTemplate, resolveColumns } from "./netlify/functions/_lib/template.mjs";
import { parseAsinMapCsv, asinMapInfo } from "./netlify/functions/_lib/asinmap.mjs";
import { prevSnapshot } from "./netlify/functions/_lib/stores.mjs";

let pass = 0, fail = 0;
const t = (name, fn) => {
  try { fn(); console.log("  ✓ " + name); pass++; }
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
t("lo split della blacklist accetta qualsiasi separatore", () => {
  const html = fs.readFileSync("index.html", "utf8");
  const tab = html.slice(html.indexOf("function BlacklistTab"), html.indexOf("// ─── ConvertTab"));
  // Il vecchio /[\n,;\s]+/ non spezzava su "x" moltiplicativo: una lista
  // incollata con quel separatore diventava un unico token accettato perche'
  // lungo >= 8, e la blacklist non bloccava piu' niente.
  assert.ok(!/inp\.split\(\/\[\\n,;\\s\]\+\/\)/.test(tab), "usa ancora il vecchio split");
  assert.ok(/inp\.split\(\/\\D\+\/\)/.test(tab), "non spezza su tutti i non-cifra");
  assert.ok(/normalizeEAN\(g\)/.test(tab), "non valida gli EAN prima di inserirli");
  assert.ok(/new Set\(\[\.\.\.c\.blacklist,\.\.\.nuovi\]\)/.test(tab), "non deduplica in scrittura");
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
t("entrambi gli handler scrivono prev nel payload", () => {
  for (const f of ["netlify/functions/ftp-convert.mjs", "netlify/functions/scheduled-convert.mjs"]) {
    const src = fs.readFileSync(f, "utf8");
    assert.ok(/prev: prevSnapshot\(/.test(src), f + " non salva prev");
    assert.ok(/prevSnapshot[,\s]/.test(src.split("stores.mjs")[0]), f + " non importa prevSnapshot");
  }
});

console.log(`\n${fail === 0 ? "✅" : "❌"}  ${pass} test passati, ${fail} falliti\n`);
process.exit(fail ? 1 : 0);
