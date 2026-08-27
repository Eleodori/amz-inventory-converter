/**
 * Verifica il percorso browser: parseTemplateFile() sul .xlsm reale deve
 * ricostruire lo stesso template del seed, e expandRecords/buildTxt/buildXlsx
 * lato client devono dare lo stesso risultato del server.
 */
import fs from "node:fs";
import assert from "node:assert";
import XLSX from "xlsx";
import * as babel from "@babel/core";
import { seedTemplate, vocabFor as srvVocabFor } from "./netlify/functions/_lib/template.mjs";
import { expandRows, buildTxt } from "./netlify/functions/_lib/converter.mjs";

const html = fs.readFileSync("index.html", "utf8");
const code = html.match(/<script type="text\/babel">([\s\S]*?)<\/script>/)[1];
// estrai solo il blocco helper (dal marcatore fino a HistoryTab)
const start = code.indexOf("// ─── Formato template Amazon");
const end = code.indexOf("// ─── Mini bar chart");
assert.ok(start > 0 && end > start, "blocco helper non individuato");
const helpers = code.slice(start, end);
const out = babel.transformSync(helpers + "\nglobalThis.__H={parseTemplateFile,expandRecords,buildTxt,resolveColumns,normalizeEAN:typeof normalizeEAN!=='undefined'?normalizeEAN:null,fmtPrice,vocabFor,vocabFromLists,extractVocabLists};",
  { presets: [["@babel/preset-react", { runtime: "classic" }]], configFile: false, babelrc: false }).code;
globalThis.XLSX = XLSX;
globalThis.fetch = async () => { throw new Error("no fetch in test"); };
new Function(out)();
const H = globalThis.__H;

let pass = 0, fail = 0;
const t = (n, f) => { try { f(); console.log("  ✓ " + n); pass++; } catch (e) { console.log("  ✗ " + n + "\n      " + e.message); fail++; } };

const REF = process.env.AMZ_TEMPLATE_REF || "fixtures/template_amazon_riferimento.xlsm";
if (!fs.existsSync(REF)) {
  // Prima qui c'era un percorso assoluto della sandbox in cui il file era stato
  // caricato: su qualsiasi altra macchina `npm test` moriva con ENOENT invece di
  // saltare il gruppo di test, e sembrava che il codice fosse rotto.
  console.log("\n\u2500\u2500 template di riferimento assente (" + REF + "), salto i test client \u2500\u2500");
  process.exit(0);
}
const bytes = fs.readFileSync(REF);
const fakeFile = { name: "InventoryLoader_IT_20260820New_Version.xlsm", arrayBuffer: async () => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) };

const parsed = await H.parseTemplateFile(fakeFile);
const seed = seedTemplate();

console.log("\n── parseTemplateFile sul .xlsm reale ──");
t("nCols identico al seed", () => assert.equal(parsed.nCols, seed.nCols));
t("labelRow/attributeRow/dataRow identici", () => {
  assert.equal(parsed.labelRow, seed.labelRow);
  assert.equal(parsed.attributeRow, seed.attributeRow);
  assert.equal(parsed.dataRow, seed.dataRow);
});
t("marketplaceId e lingua letti da A1", () => {
  assert.equal(parsed.marketplaceId, "APJ6JRA9NG5V4");
  assert.equal(parsed.contentLanguageTag, "it_IT");
});
t("foglio Modello riconosciuto", () => assert.equal(parsed.sheetName, "Modello"));
t("riga attributi identica al seed", () => assert.deepEqual(parsed.headerRows[4], seed.headerRows[4]));
t("riga etichette identica al seed", () => assert.deepEqual(parsed.headerRows[3], seed.headerRows[3]));
t("settings A1/B1/C1 identici al seed", () => {
  assert.deepEqual(parsed.headerRows[0].slice(0, 3), seed.headerRows[0].slice(0, 3));
});
t("tutte le colonne obbligatorie risolte", () => assert.deepEqual(H.resolveColumns(parsed).missing, []));

console.log("\n── client vs server: stesse righe ──");
const recs = [
  { sku: "3188649821594", ean: "3188649821594", qty: 76, price: 171.61 },
  { sku: "3286340273619", ean: "3286340273619", qty: 0, price: 233.32 },
];
const srv = expandRows(recs, seed, { marketplace: "IT", leadtime: 4 });
const cli = H.expandRecords(recs, parsed, { marketplace: "IT", leadtime: 4 });
t("righe espanse identiche", () => assert.deepEqual(cli, srv));
t("txt identico", () => assert.equal(H.buildTxt(parsed, cli, {marketplace:"IT"}), buildTxt(seed, srv, {marketplace:"IT"})));
t("prezzo numerico anche lato client", () => {
  assert.equal(typeof cli[0][H.resolveColumns(parsed).cols.price], "number");
});
t("prezzo formattato con la virgola", () => assert.equal(H.fmtPrice(171.61, "IT"), "171,61"));
t("prezzo col punto su UK", () => assert.equal(H.fmtPrice(171.61, "UK"), "171.61"));

console.log("\n── rifiuto di file non validi ──");
const notTemplate = XLSX.write((() => { const wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([["ciao"]]), "Foglio1"); return wb; })(), { bookType: "xlsx", type: "buffer" });
t("un xlsx qualsiasi viene rifiutato", async () => {
  return H.parseTemplateFile({ name: "x.xlsx", arrayBuffer: async () => notTemplate.buffer.slice(notTemplate.byteOffset, notTemplate.byteOffset + notTemplate.byteLength) })
    .then(() => { throw new Error("avrebbe dovuto lanciare"); }, () => {});
});

console.log("\n── vocabolario: client e server d'accordo ──");
t("parseTemplateFile cattura le liste dei valori validi", () => {
  assert.ok(parsed.vocabLists, "vocabLists non catturate dal file");
  assert.deepEqual(parsed.vocabLists.action, ["Crea o modifica", "Elimina"]);
  assert.equal(parsed.vocabLists.condition.length, 13);
});
t("client e server derivano lo stesso vocabolario", () => {
  const a = H.vocabFor(parsed), b = srvVocabFor(seed);
  assert.equal(a.create, b.create);
  assert.equal(a.delete, b.delete);
  assert.equal(a.ean, b.ean);
  assert.equal(a.conditionNew, b.conditionNew);
  assert.equal(a.channelMerchant, b.channelMerchant);
  assert.equal(a.origine, "template");
  assert.equal(b.origine, "template");
});
t("il client rifiuta un template senza le liste", async () => {
  // stesso file ma senza il foglio Dropdown Lists: deve fallire con un messaggio
  // chiaro invece di ripiegare in silenzio su stringhe scritte a mano.
  const wb2 = XLSX.read(bytes, { type: "buffer" });
  delete wb2.Sheets["Dropdown Lists"];
  wb2.SheetNames = wb2.SheetNames.filter(n => n !== "Dropdown Lists");
  const buf = XLSX.write(wb2, { type: "buffer", bookType: "xlsx" });
  const f = { name: "senza_liste.xlsx", arrayBuffer: async () => buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) };
  let msg = null;
  try { await H.parseTemplateFile(f) } catch (e) { msg = e.message }
  assert.ok(msg && /Dropdown Lists/.test(msg), "non segnala le liste mancanti: " + msg);
});

console.log(`\n${fail === 0 ? "✅" : "❌"}  ${pass} test passati, ${fail} falliti\n`);
process.exit(fail ? 1 : 0);
