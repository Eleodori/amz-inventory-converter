/**
 * Confronta le mappe EAN→ASIN di due marketplace e segnala le contraddizioni.
 *
 *   node check_mappe.mjs report/mappa_ean_asin_verificata.csv report24/mappa_ean_asin_DE_riconciliata.csv
 *
 * Perche' serve: audit_asin.mjs verifica una mappa alla volta confrontando il
 * titolo della pagina Amazon con marca e misura del listino. Ma il titolo di una
 * pagina PUO' ESSERE DIVERSO DA UN MARKETPLACE ALL'ALTRO, e puo' contraddire gli
 * attributi tecnici della pagina stessa. Caso reale del 07/09/2026:
 *
 *   ASIN B0BRCJZLTN
 *     titolo su amazon.de : "245/35Yr20 Goodyear Tl F1 Asym 5 Mo Xl (Eu) 95Y"
 *     titolo su amazon.it : "HANKOOK 275/35 R21 103Y ION EVO SUV IK01A"
 *     attributi (entrambi): Hersteller HANKOOK, Modellnummer 1031391, 275-35-21
 *
 * L'audit tedesco ha accettato la coppia perche' il titolo tedesco combaciava col
 * Goodyear del listino. Su Amazon.it lo stesso ASIN e' un Hankook, ed e' stato
 * venduto: due pneumatici sbagliati, 377,40 €.
 *
 * Due segnali si vedono solo incrociando le due mappe, senza aprire nessuna pagina:
 *
 *   1. STESSO ASIN, EAN DIVERSI  → la pagina non puo' essere due prodotti:
 *      una delle due mappe e' sbagliata. E' il caso di sopra.
 *   2. STESSO EAN, ASIN DIVERSI  → legittimo in teoria (cataloghi separati), ma
 *      Amazon rifiuta la riga con l'errore 101077 perche' la SKU e' la stessa.
 */
import fs from "node:fs";

const [, , A_PATH, B_PATH] = process.argv;
if (!A_PATH || !B_PATH) {
  console.error("Uso: node check_mappe.mjs <mappa-A.csv> <mappa-B.csv>");
  process.exit(1);
}

function leggi(path) {
  const m = new Map();
  const righe = fs.readFileSync(path, "utf8").split(/\r?\n/).slice(1);
  for (const r of righe) {
    const x = r.match(/^"?(\d{8,14})"?,"?([A-Z0-9]{10})"?(?:,"?([^"]*)"?,"?([^"]*)"?,"?(.*?)"?)?$/);
    if (x) m.set(x[1], { asin: x[2], marca: x[3] || "", misura: x[4] || "", titolo: x[5] || "" });
  }
  return m;
}
const nome = p => p.split("/").pop();
const A = leggi(A_PATH), B = leggi(B_PATH);
console.log(`${nome(A_PATH)}: ${A.size} coppie`);
console.log(`${nome(B_PATH)}: ${B.size} coppie`);

const perAsin = m => {
  const o = new Map();
  for (const [ean, v] of m) { if (!o.has(v.asin)) o.set(v.asin, []); o.get(v.asin).push(ean) }
  return o;
};
const aA = perAsin(A), aB = perAsin(B);

// ── 1. stesso ASIN, EAN diversi ──────────────────────────────────────────────
const conflitti = [];
for (const [asin, eansB] of aB) {
  const eansA = aA.get(asin);
  if (!eansA) continue;
  if (eansB.some(e => !eansA.includes(e))) conflitti.push({ asin, A: eansA, B: eansB });
}
console.log(`\n╔═══ STESSO ASIN, EAN DIVERSI: ${conflitti.length} ═══╗`);
if (!conflitti.length) console.log("  nessuno");
for (const c of conflitti) {
  console.log(`  ${c.asin}`);
  for (const e of c.A) console.log(`     ${nome(A_PATH).slice(0, 12).padEnd(12)} ${e}  ${A.get(e).marca} ${A.get(e).misura}  ${A.get(e).titolo.slice(0, 48)}`);
  for (const e of c.B) console.log(`     ${nome(B_PATH).slice(0, 12).padEnd(12)} ${e}  ${B.get(e).marca} ${B.get(e).misura}  ${B.get(e).titolo.slice(0, 48)}`);
}

// ── 2. stesso EAN, ASIN diversi ──────────────────────────────────────────────
const incoerenti = [];
for (const [ean, va] of A) {
  const vb = B.get(ean);
  if (vb && vb.asin !== va.asin) incoerenti.push({ ean, a: va.asin, b: vb.asin });
}
console.log(`\n╔═══ STESSO EAN, ASIN DIVERSI: ${incoerenti.length} ═══╗`);
console.log("  Amazon rifiuta queste righe con l'errore 101077: una SKU, un ASIN su tutti i marketplace.");
incoerenti.slice(0, 15).forEach(x => console.log(`     ${x.ean}  ${x.a} ≠ ${x.b}`));
if (incoerenti.length > 15) console.log(`     … e altri ${incoerenti.length - 15}`);

fs.mkdirSync("report", { recursive: true });
const esc = v => `"${String(v ?? "").replace(/"/g, '""')}"`;
fs.writeFileSync("report/mappe_asin_in_conflitto.csv",
  "asin,mappa,ean,marca,misura,titolo\n" +
  conflitti.flatMap(c => [
    ...c.A.map(e => [c.asin, nome(A_PATH), e, A.get(e).marca, A.get(e).misura, A.get(e).titolo]),
    ...c.B.map(e => [c.asin, nome(B_PATH), e, B.get(e).marca, B.get(e).misura, B.get(e).titolo]),
  ]).map(r => r.map(esc).join(",")).join("\n") + "\n");
fs.writeFileSync("report/mappe_ean_asin_incoerenti.csv",
  "ean,asin_" + nome(A_PATH) + ",asin_" + nome(B_PATH) + "\n" +
  incoerenti.map(x => [x.ean, x.a, x.b].map(esc).join(",")).join("\n") + "\n");
console.log(`\nscritti in report/:`);
console.log(`  mappe_asin_in_conflitto.csv     ${conflitti.length} ASIN`);
console.log(`  mappe_ean_asin_incoerenti.csv   ${incoerenti.length} EAN`);
process.exit(conflitti.length ? 1 : 0);
