/**
 * /api/ftp-convert
 *
 * GET  → legge l'ultimo file già pronto dai Blobs (generato dallo scheduled job o da un POST)
 * POST → scarica i CSV dall'FTP adesso, li converte con la config salvata, salva il risultato
 *
 * Struttura FTP attesa:
 *   fornitori/
 *     nome_fornitore/
 *       file.csv   (uno qualsiasi, prende il primo .csv trovato)
 */

import * as ftp from "basic-ftp";
import { getStore } from "@netlify/blobs";

const RESULT_STORE = "ftp-results";
const RESULT_KEY   = "latest";

// ─── Utility ──────────────────────────────────────────────────────────────────

function getResultStore() {
  return getStore({ name: RESULT_STORE, consistency: "strong" });
}

function parseCSV(text, delimiter) {
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = lines[0].split(delimiter).map(x => x.trim());
  const rows = lines.slice(1).map(line => {
    const cols = line.split(delimiter);
    const obj = {};
    headers.forEach((k, i) => obj[k] = (cols[i] || "").trim());
    return obj;
  });
  return { headers, rows };
}

function fCol(headers, name) {
  return headers.find(h => h.toLowerCase() === name.toLowerCase());
}

function applyTiers(price, tiers) {
  if (!tiers || tiers.length === 0) return price;
  const tier = tiers.find(t => t.upTo === null || price <= t.upTo);
  if (!tier) return price;
  return price * (1 + tier.markupPct / 100) + tier.flatFee;
}

function resolveDuplicate(existing, candidate, mode, supplierPriority) {
  if (mode === "priority") {
    const pa = supplierPriority[existing.supplier] ?? 999;
    const pb = supplierPriority[candidate.supplier] ?? 999;
    if (pb < pa) return candidate;
    if (pb === pa && candidate.price < existing.price) return candidate;
    return existing;
  }
  return candidate.price < existing.price ? candidate : existing;
}

// ─── FTP: scarica tutti i CSV dei fornitori ───────────────────────────────────

async function fetchAllCSVsFromFTP(supplierNames, suppliers = []) {
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;

  const results = {}; // { supplierName: csvText }

  try {
    await client.access({
      host:     Netlify.env.get("FTP_HOST"),
      user:     Netlify.env.get("FTP_USER"),
      password: Netlify.env.get("FTP_PASS"),
      port:     parseInt(Netlify.env.get("FTP_PORT") || "21"),
      secure:   true, // FTPS con TLS (come FileZilla)
    });

    // Lista le cartelle sotto /fornitori
    let ftpDirs = [];
    try {
      // La root FTP contiene una cartella col nome del dominio
      // quindi il path reale è michelee14.sg-host.com/fornitori
      const rootList = await client.list();
      const domainDir = rootList.find(f => f.type === ftp.FileType.Directory && f.name.includes("."));
      if (domainDir) await client.cd(domainDir.name);
      await client.cd("fornitori");
      const list = await client.list();
      ftpDirs = list.filter(f => f.type === ftp.FileType.Directory).map(f => f.name);
    } catch (e) {
      throw new Error(`Cartella fornitori non trovata sull'FTP: ${e.message}`);
    }

    // Per ogni fornitore configurato, cerca la cartella FTP corrispondente
    // Usa ftpFolder se specificato, altrimenti il nome del fornitore (case-insensitive)
    for (const supplierName of supplierNames) {
      // Recupera il config del fornitore per leggere ftpFolder
      const supplierConfig = suppliers ? suppliers.find(s => s.name === supplierName) : null;
      const ftpFolderName  = supplierConfig?.ftpFolder?.trim() || supplierName;

      const matchedDir = ftpDirs.find(
        dir => dir.toLowerCase() === ftpFolderName.toLowerCase()
      );
      if (!matchedDir) continue; // cartella non trovata → skip

      try {
        await client.cd(matchedDir);
        const files = await client.list();
        const csvFile = files.find(f => f.name.toLowerCase().endsWith(".csv"));
        if (!csvFile) { await client.cd(".."); continue; }

        const chunks = [];
        const stream = new (await import("stream")).PassThrough();
        stream.on("data", chunk => chunks.push(chunk));
        await client.downloadTo(stream, csvFile.name);
        results[supplierName] = Buffer.concat(chunks).toString("utf-8");

        await client.cd("..");
      } catch (e) {
        console.warn(`Errore scaricando ${matchedDir}: ${e.message}`);
        await client.cd("..").catch(() => {});
      }
    }
  } finally {
    client.close();
  }

  return results;
}

// ─── Conversione ──────────────────────────────────────────────────────────────

function runConversion(config, csvMap, marketplace, dupMode) {
  const mp = config.marketplaces.find(m => m.code === marketplace);
  if (!mp) throw new Error(`Marketplace ${marketplace} non trovato nella config`);

  const bl = new Set(config.blacklist || []);
  const supplierPriority = Object.fromEntries(config.suppliers.map((s, i) => [s.name, i]));
  const em = {};
  let tR = 0, tS = 0, dup = 0, blk = 0;
  const errors = [];
  const tierCounts = {}, tierPriceSum = {};

  for (const sup of config.suppliers) {
    const csvText = csvMap[sup.name];
    if (!csvText) {
      errors.push(`${sup.name}: nessun CSV trovato sull'FTP`);
      continue;
    }

    const { headers, rows } = parseCSV(csvText, sup.delimiter || ";");
    const skC = fCol(headers, sup.skuCol);
    const enC = fCol(headers, sup.eanCol);
    const prC = fCol(headers, sup.priceCol);
    const stC = sup.stockCol ? fCol(headers, sup.stockCol) : null;

    if (!skC || !enC || !prC) {
      errors.push(`${sup.name}: colonne non trovate (${[!skC&&sup.skuCol,!enC&&sup.eanCol,!prC&&sup.priceCol].filter(Boolean).join(", ")})`);
      continue;
    }

    for (const row of rows) {
      tR++;
      const ean = (row[enC] || "").trim();
      const sku = (row[skC] || "").trim();
      const rp  = parseFloat((row[prC] || "0").replace(",", "."));

      if (!ean || ean.length < 8 || !sku || isNaN(rp) || rp <= 0) { tS++; continue; }
      if (bl.has(ean)) { blk++; continue; }

      const tiers = sup.tiers || [];
      const tierIdx = tiers.findIndex(t => t.upTo === null || rp <= t.upTo);
      const tierKey = tiers[tierIdx]?.upTo === null
        ? `>${tiers[tierIdx - 1]?.upTo ?? 0}€`
        : `≤${tiers[tierIdx]?.upTo}€`;
      const fp = applyTiers(rp, tiers);
      const stock = stC ? parseInt(row[stC]) || 0 : null;

      const candidate = { sku, ean, price: fp, supplier: sup.name, stock, tierKey, rp };

      if (em[ean]) {
        dup++;
        em[ean] = resolveDuplicate(em[ean], candidate, dupMode, supplierPriority);
      } else {
        em[ean] = candidate;
      }
    }
  }

  // Costruisce file di output
  const header = "sku\tproduct-id\tproduct-id-type\tprice\titem-condition\tquantity\tadd-delete\tleadtime-to-ship";
  const lines = [header];
  const sc = {};
  const previewData = [];
  let priceSum = 0;

  const sorted = Object.values(em).sort((a, b) => a.ean.localeCompare(b.ean));

  for (const it of sorted) {
    let ps = it.price.toFixed(2);
    if (marketplace !== "UK") ps = ps.replace(".", ",");
    const qty = it.stock !== null ? String(it.stock) : String(mp.quantity);
    lines.push(`${it.sku}\t${it.ean}\t4\t${ps}\t11\t${qty}\ta\t${mp.leadtime}`);
    sc[it.supplier] = (sc[it.supplier] || 0) + 1;
    tierCounts[it.tierKey]  = (tierCounts[it.tierKey]  || 0) + 1;
    tierPriceSum[it.tierKey] = (tierPriceSum[it.tierKey] || 0) + it.rp;
    priceSum += it.rp;
    if (previewData.length < 50) {
      previewData.push({ sku: it.sku, ean: it.ean, costPrice: it.rp, salePrice: it.price, supplier: it.supplier, qty, tierKey: it.tierKey });
    }
  }

  const avg_price_by_tier = {};
  Object.keys(tierCounts).forEach(k => { avg_price_by_tier[k] = tierPriceSum[k] / tierCounts[k]; });

  return {
    fileContent: lines.join("\r\n") + "\r\n",
    stats: {
      marketplace,
      total_products: sorted.length,
      total_read: tR,
      total_skipped: tS,
      duplicates_resolved: dup,
      blacklisted: blk,
      by_supplier: sc,
      by_tier: tierCounts,
      avg_price_by_tier,
      avg_price_total: sorted.length > 0 ? priceSum / sorted.length : 0,
      errors,
      previewData,
      generated_at: new Date().toISOString(),
    },
  };
}

// ─── Handler ─────────────────────────────────────────────────────────────────

export default async (req) => {
  const store = getResultStore();

  // GET → restituisce l'ultimo risultato pronto
  if (req.method === "GET") {
    try {
      const result = await store.get(RESULT_KEY, { type: "json" });
      return new Response(JSON.stringify(result || null), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500 });
    }
  }

  // POST → scarica da FTP, converte, salva
  if (req.method === "POST") {
    try {
      const body = await req.json().catch(() => ({}));
      const marketplace = body.marketplace || "IT";
      const dupMode     = body.dupMode     || "price";

      // Carica config da Blobs
      const configStore = getStore({ name: "amz-config", consistency: "strong" });
      const config = await configStore.get("app-config", { type: "json" });
      if (!config || !config.suppliers?.length) {
        return new Response(JSON.stringify({ error: "Nessuna configurazione trovata. Configura i fornitori prima." }), { status: 400 });
      }

      // Scarica CSV dall'FTP
      const supplierNames = config.suppliers.map(s => s.name);
      const csvMap = await fetchAllCSVsFromFTP(supplierNames, config.suppliers);

      if (Object.keys(csvMap).length === 0) {
        return new Response(JSON.stringify({ error: "Nessun CSV scaricato dall'FTP. Verifica che le cartelle in /fornitori corrispondano ai nomi dei fornitori configurati." }), { status: 400 });
      }

      // Conversione
      const { fileContent, stats } = runConversion(config, csvMap, marketplace, dupMode);

      // Salva risultato in Blobs
      await store.setJSON(RESULT_KEY, {
        fileContent,
        stats,
        filename: `InventoryLoader_${marketplace}_${new Date().toISOString().slice(0, 10)}.txt`,
      });

      return new Response(JSON.stringify({ ok: true, stats }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500 });
    }
  }

  return new Response("Method not allowed", { status: 405 });
};

export const config = {
  path: "/api/ftp-convert",
};
