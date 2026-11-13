/**
 * Controllo rapido del filtro "categoria di veicolo" su un listino fornitore.
 *
 *   node check_camion.mjs                      # cerca il CSV da solo
 *   node check_camion.mjs /percorso/file.csv   # oppure glielo dici tu
 *
 * Stampa quante righe il convertitore scarterebbe e quante terrebbe, divise
 * per categoria. Serve soprattutto a verificare che "Light Truck" (i furgoni,
 * meta' dei quali gia' in vendita su Amazon) NON finisca nel filtro dei camion.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { parseCSV, excludeRuleFor, fCol } from "./netlify/functions/_lib/converter.mjs";

const home = os.homedir();
const repo = path.dirname(new URL(import.meta.url).pathname);
const dirs = [
  process.argv[2],
  path.join(home, "Downloads"),
  path.join(home, "Desktop"),
  path.join(home, "Documents"),
  repo,
  "/tmp",
].filter(Boolean);

let csv = null;
for (const d of dirs) {
  try {
    if (fs.statSync(d).isFile()) { csv = d; break; }
    const hit = fs.readdirSync(d)
      .filter(f => /GLOBAL.*\.csv$/i.test(f))
      .map(f => path.join(d, f))
      .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs)[0];
    if (hit) { csv = hit; break; }
  } catch { /* cartella non leggibile: si prova la prossima */ }
}

if (!csv) {
  console.log("CSV non trovato in Downloads, Scrivania, Documenti, nel progetto o in /tmp.");
  console.log("Rilancia passando il percorso:  node check_camion.mjs /percorso/del/file.csv");
  process.exit(1);
}
console.log("CSV:", csv, "\n");

const { headers, rows } = parseCSV(fs.readFileSync(csv, "utf8"), ";");
const regola = excludeRuleFor({});
if (!regola) { console.log("Nessun filtro configurato."); process.exit(0); }
const col = fCol(headers, regola.col);
if (!col) { console.log(`La colonna "${regola.col}" non c'e' in questo CSV: il filtro non si applica.`); process.exit(0); }

const escluso = r => regola.values.has(String(r[col] ?? "").trim().toLowerCase());
const fuori = rows.filter(escluso);
const dentro = rows.filter(r => !escluso(r));
const conta = lista => {
  const o = {};
  for (const r of lista) { const k = r[col] || "(vuoto)"; o[k] = (o[k] || 0) + 1; }
  return Object.entries(o).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}: ${v}`).join(", ") || "-";
};

console.log("filtro attivo :", regola.col, "->", [...regola.values].join(", "));
console.log("righe totali  :", rows.length);
console.log("ESCLUSE       :", fuori.length, "  (" + conta(fuori) + ")");
console.log("TENUTE        :", dentro.length, "  (" + conta(dentro) + ")");

const furgoni = dentro.filter(r => String(r[col]).trim().toLowerCase() === "light truck").length;
console.log("");
console.log(furgoni > 0
  ? `OK: ${furgoni} furgoni (Light Truck) restano nel catalogo.`
  : "ATTENZIONE: nessun Light Truck e' sopravvissuto al filtro.");
