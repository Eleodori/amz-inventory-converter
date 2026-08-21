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
const REF = "/root/.claude/uploads/dc3f1e41-03a7-5194-aef8-50015387be7f/bbe65d22-InventoryLoader_IT_20260820New_Version.xlsm";
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

console.log(`\n${fail === 0 ? "✅" : "❌"}  ${pass} test passati, ${fail} falliti\n`);
process.exit(fail ? 1 : 0);
