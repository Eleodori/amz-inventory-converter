/**
 * Scheduled Function: gira ogni mattina alle 9:15 (ora italiana = 8:15 UTC)
 * 1. Scarica i CSV dall'FTP
 * 2. Esegue la conversione
 * 3. Salva il risultato in Blobs
 * 4. Confronta con la conversione precedente
 * 5. Se ci sono variazioni significative, salva un alert in Blobs
 * 6. Salva nello storico
 */

import * as ftp from "basic-ftp";
import { getStore } from "@netlify/blobs";

// ─── Soglie alert ─────────────────────────────────────────────────────────────
const ALERT_THRESHOLDS = {
  productsDeltaPct:  10,  // ±10% prodotti totali
  supplierDeltaPct:  20,  // ±20% prodotti di un singolo fornitore
};

// ─── Funzioni condivise (duplicate da ftp-convert per autonomia dello scheduled job) ──

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

async function fetchAllCSVsFromFTP(supplierNames, suppliers = []) {
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;
  const results = {};
  try {
    await client.access({
      host:     Netlify.env.get("FTP_HOST"),
      user:     Netlify.env.get("FTP_USER"),
      password: Netlify.env.get("FTP_PASS"),
      port:     parseInt(Netlify.env.get("FTP_PORT") || "21"),
      secure:   true, // FTPS con TLS
    });
    const rootList = await client.list();
    const domainDir = rootList.find(f => f.type === ftp.FileType.Directory && f.name.includes("."));
    if (domainDir) await client.cd(domainDir.name);
    await client.cd("fornitori");
    const list = await client.list();
    const ftpDirs = list.filter(f => f.type === ftp.FileType.Directory).map(f => f.name);
    for (const supplierName of supplierNames) {
      const supplierConfig = suppliers.find(s => s.name === supplierName);
      const ftpFolderName  = supplierConfig?.ftpFolder?.trim() || supplierName;
      const matchedDir = ftpDirs.find(dir => dir.toLowerCase() === ftpFolderName.toLowerCase());
      if (!matchedDir) continue;
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

function runConversion(config, csvMap, marketplace, dupMode) {
  const mp = config.marketplaces.find(m => m.code === marketplace);
  if (!mp) throw new Error(`Marketplace ${marketplace} non trovato`);
  const bl = new Set(config.blacklist || []);
  const supplierPriority = Object.fromEntries(config.suppliers.map((s, i) => [s.name, i]));
  const em = {};
  let tR = 0, tS = 0, dup = 0, blk = 0;
  const errors = [];
  const tierCounts = {}, tierPriceSum = {};

  for (const sup of config.suppliers) {
    const csvText = csvMap[sup.name];
    if (!csvText) { errors.push(`${sup.name}: nessun CSV su FTP`); continue; }
    const { headers, rows } = parseCSV(csvText, sup.delimiter || ";");
    const skC = fCol(headers, sup.skuCol);
    const enC = fCol(headers, sup.eanCol);
    const prC = fCol(headers, sup.priceCol);
    const stC = sup.stockCol ? fCol(headers, sup.stockCol) : null;
    if (!skC || !enC || !prC) { errors.push(`${sup.name}: colonne mancanti`); continue; }
    for (const row of rows) {
      tR++;
      const ean = (row[enC] || "").trim();
      const sku = (row[skC] || "").trim();
      const rp  = parseFloat((row[prC] || "0").replace(",", "."));
      if (!ean || ean.length < 8 || !sku || isNaN(rp) || rp <= 0) { tS++; continue; }
      if (bl.has(ean)) { blk++; continue; }
      const tiers = sup.tiers || [];
      const tierIdx = tiers.findIndex(t => t.upTo === null || rp <= t.upTo);
      const tierKey = tiers[tierIdx]?.upTo === null ? `>${tiers[tierIdx-1]?.upTo??0}€` : `≤${tiers[tierIdx]?.upTo}€`;
      const fp = applyTiers(rp, tiers);
      const stock = stC ? parseInt(row[stC]) || 0 : null;
      const candidate = { sku, ean, price: fp, supplier: sup.name, stock, tierKey, rp };
      if (em[ean]) { dup++; em[ean] = resolveDuplicate(em[ean], candidate, dupMode, supplierPriority); }
      else em[ean] = candidate;
    }
  }

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
    if (previewData.length < 50) previewData.push({ sku: it.sku, ean: it.ean, costPrice: it.rp, salePrice: it.price, supplier: it.supplier, qty, tierKey: it.tierKey });
  }
  const avg_price_by_tier = {};
  Object.keys(tierCounts).forEach(k => { avg_price_by_tier[k] = tierPriceSum[k] / tierCounts[k]; });
  return {
    fileContent: lines.join("\r\n") + "\r\n",
    stats: {
      marketplace, total_products: sorted.length, total_read: tR, total_skipped: tS,
      duplicates_resolved: dup, blacklisted: blk, by_supplier: sc, by_tier: tierCounts,
      avg_price_by_tier, avg_price_total: sorted.length > 0 ? priceSum / sorted.length : 0,
      errors, previewData, generated_at: new Date().toISOString(),
    },
  };
}

// ─── Analisi variazioni e generazione alert ───────────────────────────────────

function buildAlerts(current, previous) {
  if (!previous) return [];
  const alerts = [];

  // Variazione totale prodotti
  const totalDelta = current.total_products - previous.total_products;
  const totalDeltaPct = previous.total_products > 0
    ? Math.abs(totalDelta / previous.total_products * 100)
    : 0;

  if (totalDeltaPct >= ALERT_THRESHOLDS.productsDeltaPct) {
    alerts.push({
      type: totalDelta < 0 ? "warning" : "info",
      title: totalDelta < 0 ? "⚠️ Calo prodotti significativo" : "📈 Aumento prodotti significativo",
      message: `Totale prodotti: ${previous.total_products.toLocaleString()} → ${current.total_products.toLocaleString()} (${totalDelta > 0 ? "+" : ""}${totalDelta.toLocaleString()}, ${totalDeltaPct.toFixed(1)}%)`,
    });
  }

  // Variazione per fornitore
  const allSuppliers = new Set([
    ...Object.keys(current.by_supplier || {}),
    ...Object.keys(previous.by_supplier || {}),
  ]);
  for (const sup of allSuppliers) {
    const curr = current.by_supplier?.[sup]  || 0;
    const prev = previous.by_supplier?.[sup] || 0;
    if (prev === 0 && curr > 0) {
      alerts.push({ type: "info", title: `🆕 Nuovo fornitore attivo: ${sup}`, message: `${curr.toLocaleString()} prodotti caricati` });
      continue;
    }
    if (curr === 0 && prev > 0) {
      alerts.push({ type: "error", title: `❌ Fornitore scomparso: ${sup}`, message: `${prev.toLocaleString()} prodotti non più disponibili` });
      continue;
    }
    if (prev > 0) {
      const delta = curr - prev;
      const deltaPct = Math.abs(delta / prev * 100);
      if (deltaPct >= ALERT_THRESHOLDS.supplierDeltaPct) {
        alerts.push({
          type: delta < 0 ? "warning" : "info",
          title: `${delta < 0 ? "⚠️" : "📦"} ${sup}: ${delta < 0 ? "calo" : "aumento"} prodotti`,
          message: `${prev.toLocaleString()} → ${curr.toLocaleString()} (${delta > 0 ? "+" : ""}${delta.toLocaleString()}, ${deltaPct.toFixed(1)}%)`,
        });
      }
    }
  }

  // Errori nei CSV
  if (current.errors?.length > 0) {
    alerts.push({
      type: "error",
      title: "❌ Errori nella lettura CSV",
      message: current.errors.join(" | "),
    });
  }

  return alerts;
}

// ─── Scheduled handler ────────────────────────────────────────────────────────

export default async (req) => {
  console.log("🕐 Scheduled job avviato:", new Date().toISOString());

  try {
    const configStore = getStore({ name: "amz-config",   consistency: "strong" });
    const resultStore = getStore({ name: "ftp-results",  consistency: "strong" });
    const alertStore  = getStore({ name: "amz-alerts",   consistency: "strong" });
    const historyStore = getStore({ name: "conversion-history", consistency: "strong" });

    // Carica config
    const config = await configStore.get("app-config", { type: "json" });
    if (!config?.suppliers?.length) {
      console.warn("Nessuna configurazione trovata, skip.");
      return;
    }

    // Marketplace principale (primo configurato)
    const marketplace = config.marketplaces?.[0]?.code || "IT";
    const dupMode = "price";

    // Prende il risultato precedente per confronto
    const previous = await resultStore.get("latest", { type: "json" });
    const previousStats = previous?.stats || null;

    // Scarica CSV dall'FTP
    console.log("📥 Scaricando CSV dall'FTP...");
    const supplierNames = config.suppliers.map(s => s.name);
    const csvMap = await fetchAllCSVsFromFTP(supplierNames, config.suppliers);
    console.log(`✓ CSV scaricati: ${Object.keys(csvMap).join(", ")}`);

    // Conversione
    console.log("⚙️ Conversione in corso...");
    const { fileContent, stats } = runConversion(config, csvMap, marketplace, dupMode);
    console.log(`✓ ${stats.total_products} prodotti generati`);

    // Salva risultato
    const filename = `InventoryLoader_${marketplace}_${new Date().toISOString().slice(0, 10)}.txt`;
    await resultStore.setJSON("latest", { fileContent, stats, filename });

    // Salva nello storico
    const historyRecord = {
      id: crypto.randomUUID(),
      created_at: new Date().toISOString(),
      marketplace: stats.marketplace,
      total_products: stats.total_products,
      total_read: stats.total_read,
      total_skipped: stats.total_skipped,
      duplicates_resolved: stats.duplicates_resolved,
      blacklisted: stats.blacklisted,
      by_supplier: stats.by_supplier,
      by_tier: stats.by_tier,
      avg_price_by_tier: stats.avg_price_by_tier,
      avg_price_total: stats.avg_price_total,
      source: "scheduled",
    };
    await historyStore.setJSON(`conv-${historyRecord.created_at}-${historyRecord.id}`, historyRecord);

    // Analisi variazioni e alert
    const alerts = buildAlerts(stats, previousStats);
    if (alerts.length > 0) {
      console.log(`⚠️ ${alerts.length} alert generati`);
      const existingAlerts = await alertStore.get("active", { type: "json" }) || [];
      const newAlerts = alerts.map(a => ({
        ...a,
        id: crypto.randomUUID(),
        created_at: new Date().toISOString(),
        read: false,
      }));
      // Mantieni solo gli alert degli ultimi 7 giorni + i nuovi
      const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const kept = existingAlerts.filter(a => a.created_at > cutoff && a.read);
      await alertStore.setJSON("active", [...kept, ...newAlerts]);
    } else {
      console.log("✓ Nessuna variazione significativa");
    }

    console.log("✅ Job completato con successo");
  } catch (e) {
    console.error("❌ Errore nello scheduled job:", e.message);

    // Salva un alert di errore critico
    try {
      const alertStore = getStore({ name: "amz-alerts", consistency: "strong" });
      const existing = await alertStore.get("active", { type: "json" }) || [];
      existing.push({
        id: crypto.randomUUID(),
        type: "error",
        title: "❌ Errore nel job automatico di questa mattina",
        message: e.message,
        created_at: new Date().toISOString(),
        read: false,
      });
      await alertStore.setJSON("active", existing);
    } catch {}
  }
};

export const config = {
  schedule: "15 8 * * 1-6", // 8:15 UTC = 9:15 IT (ora solare) / 10:15 IT (ora legale)
                              // Lun–Sab, domenica esclusa
};
