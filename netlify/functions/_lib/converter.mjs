/**
 * Logica di conversione condivisa.
 * Unica copia: la usano sia /api/ftp-convert sia lo scheduled job.
 *
 * La conversione produce record COMPATTI ({sku, ean, qty, price, ...}), non
 * righe larghe 140 colonne: sarebbero ~3 MB di stringhe vuote da salvare su
 * Blobs e da spedire al browser a ogni caricamento. L'espansione al formato
 * template avviene solo quando si genera il file (expandRows).
 */

import { resolveColumns, vocabFor } from "./template.mjs";

// ─── CSV ──────────────────────────────────────────────────────────────────────

/**
 * Parser CSV che gestisce i campi quotati.
 * Il vecchio parser faceva line.split(delimiter): bastava un campo tipo
 * "Pirelli; P Zero" per far slittare tutte le colonne successive e pubblicare
 * su Amazon un prezzo letto dalla colonna sbagliata, senza alcun errore.
 */
export function parseCSV(text, delimiter = ";") {
  if (!text) return { headers: [], rows: [] };
  const src = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text; // strip BOM
  const d = delimiter || ";";

  const records = [];
  let field = "";
  let record = [];
  let inQuotes = false;

  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (inQuotes) {
      if (ch === '"') {
        if (src[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += ch;
      continue;
    }
    if (ch === '"' && field === "") { inQuotes = true; continue; }
    if (ch === d) { record.push(field); field = ""; continue; }
    if (ch === "\r") continue;
    if (ch === "\n") { record.push(field); records.push(record); record = []; field = ""; continue; }
    field += ch;
  }
  if (field !== "" || record.length) { record.push(field); records.push(record); }

  const nonEmpty = records.filter(r => r.some(c => c.trim() !== ""));
  if (nonEmpty.length < 2) return { headers: [], rows: [] };

  const headers = nonEmpty[0].map(h => h.trim());
  const rows = nonEmpty.slice(1).map(cols => {
    const o = {};
    headers.forEach((k, i) => { o[k] = (cols[i] ?? "").trim(); });
    return o;
  });
  return { headers, rows };
}

export function fCol(headers, name) {
  if (!name) return null;
  return headers.find(h => h.toLowerCase() === String(name).toLowerCase()) || null;
}

// ─── Validazione EAN ──────────────────────────────────────────────────────────

/**
 * Il vecchio controllo era ean.length < 8: passavano stringhe non numeriche,
 * codici a 9-12 cifre e a 14 cifre. Amazon li rifiuta (errori 5501 / 8541) e le
 * righe finivano scartate in silenzio nel report di elaborazione.
 */
export function normalizeEAN(raw) {
  const s = String(raw ?? "").trim().replace(/\s+/g, "");
  if (!/^\d+$/.test(s)) return null;
  if (![8, 12, 13, 14].includes(s.length)) return null;
  if (!checkDigitOk(s)) return null;
  return s;
}

function checkDigitOk(s) {
  // Cifra di controllo GTIN-8/12/13/14: pesi 3 e 1 alternati partendo da destra.
  let sum = 0;
  for (let i = s.length - 2, w = 3; i >= 0; i--, w = w === 3 ? 1 : 3) sum += Number(s[i]) * w;
  return ((10 - (sum % 10)) % 10) === Number(s[s.length - 1]);
}

// ─── Prezzi ───────────────────────────────────────────────────────────────────

function sortTiers(tiers) {
  return [...tiers].sort((a, b) => {
    if (a.upTo === null || a.upTo === undefined) return 1;
    if (b.upTo === null || b.upTo === undefined) return -1;
    return a.upTo - b.upTo;
  });
}

export function applyTiers(price, tiers) {
  if (!tiers || tiers.length === 0) return price;
  // Ordinamento difensivo: il find() assume scaglioni crescenti. Riordinandoli
  // dall'interfaccia, senza sort si applicherebbe il markup dello scaglione sbagliato.
  const tier = sortTiers(tiers).find(t => t.upTo === null || t.upTo === undefined || price <= t.upTo);
  return tier ? price * (1 + (tier.markupPct || 0) / 100) + (tier.flatFee || 0) : price;
}

export function tierKeyFor(price, tiers) {
  if (!tiers || tiers.length === 0) return "—";
  const sorted = sortTiers(tiers);
  const idx = sorted.findIndex(t => t.upTo === null || t.upTo === undefined || price <= t.upTo);
  const t = sorted[idx];
  if (!t) return "—";
  return (t.upTo === null || t.upTo === undefined) ? `>${sorted[idx - 1]?.upTo ?? 0}€` : `≤${t.upTo}€`;
}

/**
 * Formattazione del prezzo per il file di TESTO tab-delimited: punto decimale su
 * UK, virgola sugli altri marketplace.
 *
 * ATTENZIONE: non usarla per l'.xlsx. Amazon rifiuta il prezzo scritto come testo
 * con l'errore 13006 ("Formato cella errato. Imposta il formato in Excel su
 * Numero"). Nell'.xlsx il prezzo deve essere una cella numerica: un numero non ha
 * separatore decimale, quindi la questione virgola/punto non esiste.
 */
export function formatPrice(value, marketplace) {
  const s = Number(value).toFixed(2);
  return marketplace === "UK" ? s : s.replace(".", ",");
}

/** Indici delle colonne che contengono un importo (da formattare solo nel .txt). */
export function priceColumns(template) {
  const { cols } = resolveColumns(template);
  return ["price", "minPrice", "maxPrice"].map(f => cols[f]).filter(i => i !== undefined);
}

export function resolveDuplicate(existing, candidate, mode, supplierPriority) {
  if (mode === "priority") {
    const pa = supplierPriority[existing.supplier] ?? 999;
    const pb = supplierPriority[candidate.supplier] ?? 999;
    if (pb < pa) return candidate;
    if (pb === pa && candidate.price < existing.price) return candidate;
    return existing;
  }
  return candidate.price < existing.price ? candidate : existing;
}

// ─── Espansione al formato template ───────────────────────────────────────────

/**
 * Trasforma i record compatti in righe larghe nCols, pronte per il file.
 * qty 0 = riga di disattivazione: l'offerta resta nel catalogo Amazon ma non e'
 * acquistabile, e si riattiva da sola appena torna disponibile dal fornitore.
 * Sostituisce il "Modifica e sostituisci", che cancellava e ricreava tutto.
 */
export function expandRows(records, template, opts = {}) {
  const { cols, missing } = resolveColumns(template);
  if (missing.length) throw new Error("Template incompleto, colonne mancanti: " + missing.join(", "));
  const vocab = vocabFor(template);
  const marketplace = opts.marketplace || "IT";
  const leadtime = opts.leadtime;
  const n = template.nCols;

  return records.map(rec => {
    const row = new Array(n).fill("");
    const put = (f, v) => { if (cols[f] !== undefined) row[cols[f]] = v; };
    put("sku", rec.sku);
    put("action", vocab.create);
    // Se abbiamo un ASIN verificato lo scriviamo e NON mandiamo l'EAN: cosi'
    // l'offerta e' inchiodata al prodotto giusto invece di essere abbinata da
    // Amazon. Le istruzioni del template dicono "l'ASIN OPPURE l'ID esterno",
    // quindi non li mandiamo insieme.
    if (rec.asin && cols.asin !== undefined) {
      put("asin", rec.asin);
    } else {
      put("extIdType", vocab.ean);
      put("extId", rec.ean);
    }
    put("condition", vocab.conditionNew);
    put("channel", vocab.channelMerchant);
    put("quantity", Number(rec.qty) || 0);
    if (leadtime !== null && leadtime !== undefined && leadtime !== "") put("leadtime", Number(leadtime));
    // Prezzi come NUMERI, non come stringhe: nell'.xlsx devono essere celle
    // numeriche (errore 13006). buildTxt li converte in testo con la virgola.
    put("price", round2(rec.price));
    if (rec.minPrice != null) put("minPrice", round2(rec.minPrice));
    if (rec.maxPrice != null) put("maxPrice", round2(rec.maxPrice));
    return row;
  });
}

/**
 * Serializza header + dati come .txt tab-delimited (equivalente al "Salva come
 * testo (delimitato da tabulazioni)" di Excel). Qui i prezzi diventano testo con
 * il separatore decimale del marketplace, perche' un file di testo non ha tipi.
 */
export function buildTxt(template, dataRows, opts = {}) {
  const n = template.nCols;
  const marketplace = opts.marketplace || "IT";
  const decCols = new Set(opts.decimalCols || priceColumns(template));
  const pad = r => { const a = (r || []).slice(0, n); while (a.length < n) a.push(""); return a; };
  const esc = v => String(v ?? "").replace(/\t/g, " ").replace(/\r?\n/g, " ");
  const fmt = (v, i) => (decCols.has(i) && typeof v === "number") ? formatPrice(v, marketplace) : esc(v);
  const lines = [];
  for (const hr of template.headerRows) lines.push(pad(hr).map(esc).join("\t"));
  for (const dr of dataRows) lines.push(pad(dr).map((v, i) => fmt(v, i)).join("\t"));
  return lines.join("\r\n") + "\r\n";
}

// ─── Conversione ──────────────────────────────────────────────────────────────

/**
 * @param config    configurazione app (suppliers, marketplaces, blacklist, ...)
 * @param csvMap    { nomeFornitore: testoCSV }
 * @param template  template Amazon in uso (serve per validare le colonne)
 * @param opts      { marketplace, dupMode, qtyMode, publishedSkus }
 */
export function runConversion(config, csvMap, template, opts = {}) {
  const marketplace = opts.marketplace || config.marketplaces?.[0]?.code || "IT";
  const dupMode = opts.dupMode || "price";
  const qtyMode = opts.qtyMode || "catalog";
  const published = opts.publishedSkus || {};
  const asinMap = opts.asinMap || {};
  const onlyMapped = !!config.onlyMapped;

  const mp = (config.marketplaces || []).find(m => m.code === marketplace);
  if (!mp) throw new Error(`Marketplace ${marketplace} non trovato nella configurazione`);

  const { missing } = resolveColumns(template);
  if (missing.length) throw new Error("Template incompleto, colonne mancanti: " + missing.join(", "));

  const bl = new Set(config.blacklist || []);
  const supplierPriority = Object.fromEntries((config.suppliers || []).map((s, i) => [s.name, i]));
  const minStock = Number(config.minStock ?? 0);
  const floorMarginPct = Number(config.floorMarginPct ?? 0);
  const maxQty = Number(config.maxQty ?? 0);

  const em = {};
  let totalRead = 0, skipped = 0, badEan = 0, dup = 0, blocked = 0, belowMinStock = 0, unmappedSkipped = 0;
  const errors = [];
  const tierCounts = {}, tierPriceSum = {};

  for (const sup of config.suppliers || []) {
    const csvText = csvMap[sup.name];
    if (!csvText) { errors.push(`${sup.name}: nessun CSV trovato sull'FTP`); continue; }

    const { headers, rows } = parseCSV(csvText, sup.delimiter || ";");
    if (!headers.length) { errors.push(`${sup.name}: CSV vuoto o illeggibile`); continue; }

    const skC = fCol(headers, sup.skuCol);
    const enC = fCol(headers, sup.eanCol);
    const prC = fCol(headers, sup.priceCol);
    const stC = sup.stockCol ? fCol(headers, sup.stockCol) : null;
    if (!skC || !enC || !prC) {
      errors.push(`${sup.name}: colonne non trovate (${[!skC && sup.skuCol, !enC && sup.eanCol, !prC && sup.priceCol].filter(Boolean).join(", ")})`);
      continue;
    }
    if (sup.stockCol && !stC) errors.push(`${sup.name}: colonna stock "${sup.stockCol}" non trovata, uso la quantita' di fallback`);

    for (const row of rows) {
      totalRead++;
      const ean = normalizeEAN(row[enC]);
      const sku = String(row[skC] ?? "").trim();
      const rp = parseFloat(String(row[prC] ?? "0").replace(",", "."));

      if (!ean) { badEan++; skipped++; continue; }
      if (!sku || isNaN(rp) || rp <= 0) { skipped++; continue; }
      if (bl.has(ean)) { blocked++; continue; }

      let stock = null;
      if (stC) {
        const raw = String(row[stC] ?? "").trim();
        const parsed = parseInt(raw.replace(/[^\d-]/g, ""), 10);
        stock = Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
      }

      // Buffer di stock: sotto soglia non pubblichiamo. E' nella coda lunga di
      // articoli quasi esauriti che nasce la maggior parte degli annullamenti.
      if (stock !== null && minStock > 0 && stock < minStock) { belowMinStock++; continue; }

      const finalPrice = applyTiers(rp, sup.tiers || []);
      const candidate = {
        sku, ean, price: finalPrice, supplier: sup.name, stock,
        tierKey: tierKeyFor(rp, sup.tiers || []), costPrice: rp,
      };

      if (em[ean]) { dup++; em[ean] = resolveDuplicate(em[ean], candidate, dupMode, supplierPriority); }
      else em[ean] = candidate;
    }
  }

  const sorted = Object.values(em).sort((a, b) => a.ean.localeCompare(b.ean));
  const records = [];
  const previewData = [];
  const bySupplier = {};
  let priceSum = 0;

  for (const it of sorted) {
    const raw = qtyMode === "fixed" ? Number(mp.quantity) : (it.stock !== null ? it.stock : Number(mp.quantity));
    const qty = maxQty > 0 ? Math.min(raw, maxQty) : raw;

    const rec = { sku: it.sku, ean: it.ean, qty, price: round2(it.price) };
    const mapped = asinMap[it.ean];
    if (mapped?.asin) rec.asin = mapped.asin;
    else if (onlyMapped) { unmappedSkipped++; continue; } // non pubblichiamo cio' che non e' verificato
    if (floorMarginPct > 0) rec.minPrice = round2(it.costPrice * (1 + floorMarginPct / 100));
    records.push(rec);

    bySupplier[it.supplier] = (bySupplier[it.supplier] || 0) + 1;
    tierCounts[it.tierKey] = (tierCounts[it.tierKey] || 0) + 1;
    tierPriceSum[it.tierKey] = (tierPriceSum[it.tierKey] || 0) + it.costPrice;
    priceSum += it.costPrice;
    if (previewData.length < 50) {
      previewData.push({ sku: it.sku, ean: it.ean, costPrice: it.costPrice, salePrice: it.price, supplier: it.supplier, qty: String(qty), tierKey: it.tierKey, status: "attivo" });
    }
  }

  // ─── Disattivazione delle SKU non piu' fornite ──────────────────────────────
  const todayIso = new Date().toISOString();
  const today = todayIso.slice(0, 10);
  const zeroKeepDays = Number(config.zeroKeepDays ?? 90);
  const nextPublished = {};
  let zeroed = 0, dropped = 0;

  for (const rec of records) {
    const prev = published[rec.ean] || {};
    nextPublished[rec.ean] = {
      sku: rec.sku,
      price: rec.price,
      firstSeen: prev.firstSeen || today,
      lastSeen: today,
      zeroSince: null,
    };
  }

  for (const [ean, rec] of Object.entries(published)) {
    if (nextPublished[ean]) continue;
    const zeroSince = rec.zeroSince || today;
    const ageDays = Math.floor((Date.parse(today) - Date.parse(zeroSince)) / 86400000);
    if (ageDays > zeroKeepDays) { dropped++; continue; } // smettiamo di ripeterla ogni giorno
    const zeroRow = { sku: rec.sku || ean, ean, qty: 0, price: round2(Number(rec.price) || 0.01) };
    if (asinMap[ean]?.asin) zeroRow.asin = asinMap[ean].asin;
    records.push(zeroRow);
    nextPublished[ean] = { ...rec, zeroSince, lastZeroed: today };
    zeroed++;
    if (previewData.length < 50) {
      previewData.push({ sku: rec.sku || ean, ean, costPrice: null, salePrice: Number(rec.price) || 0, supplier: "—", qty: "0", tierKey: "—", status: "disattivata" });
    }
  }

  const avg_price_by_tier = {};
  Object.keys(tierCounts).forEach(k => { avg_price_by_tier[k] = tierPriceSum[k] / tierCounts[k]; });

  return {
    records,
    publishedSkus: nextPublished,
    stats: {
      marketplace,
      leadtime: mp.leadtime,
      total_products: sorted.length,
      total_rows: records.length,
      zeroed,
      dropped_from_tracking: dropped,
      total_read: totalRead,
      total_skipped: skipped,
      bad_ean: badEan,
      below_min_stock: belowMinStock,
      duplicates_resolved: dup,
      blacklisted: blocked,
      with_asin: records.filter(r => r.asin).length,
      without_asin: records.filter(r => !r.asin).length,
      unmapped_skipped: unmappedSkipped,
      asin_map_size: Object.keys(asinMap).length,
      by_supplier: bySupplier,
      by_tier: tierCounts,
      avg_price_by_tier,
      avg_price_total: sorted.length > 0 ? priceSum / sorted.length : 0,
      errors,
      previewData,
      template: { nCols: template.nCols, marketplaceId: template.marketplaceId || null, source: template.source || null, isSeed: !!template.isSeed },
      generated_at: todayIso,
    },
  };
}

function round2(n) { return Math.round(Number(n) * 100) / 100; }

// ─── Guard-rail ───────────────────────────────────────────────────────────────

/**
 * Decide se il risultato e' pubblicabile.
 * Prima il risultato veniva salvato come "ultimo pronto" e solo dopo si
 * calcolavano gli alert: un CSV fornitore troncato produceva un file mutilato
 * gia' scaricabile, con l'avviso in ritardo.
 */
export function safetyCheck(stats, previousStats, config = {}) {
  const maxDeltaPct = Number(config.maxDeltaPct ?? 20);
  const minProducts = Number(config.minProducts ?? 100);
  const blockers = [];

  if (stats.errors?.length) blockers.push("Errori nella lettura dei CSV: " + stats.errors.join(" | "));
  if (stats.total_products < minProducts) blockers.push(`Solo ${stats.total_products} prodotti disponibili (soglia minima ${minProducts})`);

  if (previousStats?.total_products > 0) {
    const delta = stats.total_products - previousStats.total_products;
    const pct = Math.abs(delta / previousStats.total_products * 100);
    if (pct >= maxDeltaPct) {
      blockers.push(`Variazione prodotti ${delta > 0 ? "+" : ""}${delta} (${pct.toFixed(1)}%) oltre la soglia del ${maxDeltaPct}%`);
    }
  }
  return { ok: blockers.length === 0, blockers };
}

export function buildAlerts(current, previous, thresholds = {}) {
  const productsDeltaPct = Number(thresholds.productsDeltaPct ?? 10);
  const supplierDeltaPct = Number(thresholds.supplierDeltaPct ?? 20);
  const alerts = [];
  if (!previous) return alerts;

  const totalDelta = current.total_products - previous.total_products;
  const totalPct = previous.total_products > 0 ? Math.abs(totalDelta / previous.total_products * 100) : 0;
  if (totalPct >= productsDeltaPct) {
    alerts.push({
      type: totalDelta < 0 ? "warning" : "info",
      title: totalDelta < 0 ? "⚠️ Calo prodotti significativo" : "📈 Aumento prodotti significativo",
      message: `Totale prodotti: ${previous.total_products} → ${current.total_products} (${totalDelta > 0 ? "+" : ""}${totalDelta}, ${totalPct.toFixed(1)}%)`,
    });
  }

  const all = new Set([...Object.keys(current.by_supplier || {}), ...Object.keys(previous.by_supplier || {})]);
  for (const sup of all) {
    const curr = current.by_supplier?.[sup] || 0;
    const prev = previous.by_supplier?.[sup] || 0;
    if (prev === 0 && curr > 0) { alerts.push({ type: "info", title: `🆕 Nuovo fornitore attivo: ${sup}`, message: `${curr} prodotti caricati` }); continue; }
    if (curr === 0 && prev > 0) { alerts.push({ type: "error", title: `❌ Fornitore scomparso: ${sup}`, message: `${prev} prodotti non piu' disponibili` }); continue; }
    if (prev > 0) {
      const delta = curr - prev;
      const pct = Math.abs(delta / prev * 100);
      if (pct >= supplierDeltaPct) {
        alerts.push({
          type: delta < 0 ? "warning" : "info",
          title: `${delta < 0 ? "⚠️" : "📦"} ${sup}: ${delta < 0 ? "calo" : "aumento"} prodotti`,
          message: `${prev} → ${curr} (${delta > 0 ? "+" : ""}${delta}, ${pct.toFixed(1)}%)`,
        });
      }
    }
  }

  if (current.errors?.length) {
    alerts.push({ type: "error", title: "❌ Errori nella lettura CSV", message: current.errors.join(" | ") });
  }
  return alerts;
}
