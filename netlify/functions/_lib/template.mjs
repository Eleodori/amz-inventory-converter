/**
 * Gestione del template Amazon "Modello di caricamento delle offerte"
 * (nuovo formato, quello che ha sostituito l'Inventory Loader classico).
 *
 * Struttura del foglio "Modello":
 *   riga 1  → settings=... / settings2=... / settings3=... (la stringa e' spezzata
 *             su piu' celle perche' Excel non accetta piu' di 32.767 caratteri per cella)
 *   riga 2  → avviso "Compila questo modello in ITALIANO..."
 *   riga 3  → nome del gruppo di attributi
 *   riga 4  → etichetta locale (labelRow)
 *   riga 5  → nome tecnico del campo (attributeRow)  ← quella che conta
 *   riga 6  → riga di esempio (ignorata da Amazon in fase di caricamento)
 *   riga 7+ → dati (dataRow)
 *
 * Non hardcodiamo i nomi delle colonne: le risolviamo per pattern sulla riga
 * degli attributi. Cosi' il tool continua a funzionare se Amazon aggiunge,
 * sposta o rinomina colonne, e funziona su qualsiasi marketplace (l'attributo
 * del prezzo contiene il marketplace id, es. APJ6JRA9NG5V4 per Amazon.it).
 */

import { TEMPLATE_SEED } from "./templateSeed.mjs";
import { templateStore, TEMPLATE_KEY } from "./stores.mjs";

/** Colonne che il converter deve saper trovare. */
export const FIELD_PATTERNS = {
  sku:       /^contribution_sku#1\.value$/,
  action:    /^::record_action$/,
  extIdType: /^externally_assigned_product_identifier#1\.type$/,
  extId:     /^externally_assigned_product_identifier#1\.value$/,
  asin:      /^merchant_suggested_asin#1\.value$/,
  condition: /^condition_type#1\.value$/,
  channel:   /^fulfillment_availability#1\.fulfillment_channel_code$/,
  quantity:  /^fulfillment_availability#1\.quantity$/,
  leadtime:  /^fulfillment_availability#1\.lead_time_to_ship_max_days$/,
  price:     /our_price#1\.schedule#1\.value_with_tax$/,
  minPrice:  /minimum_seller_allowed_price#1\.schedule#1\.value_with_tax$/,
  maxPrice:  /maximum_seller_allowed_price#1\.schedule#1\.value_with_tax$/,
};

/** Campi senza i quali non si puo' generare un file valido. */
export const REQUIRED_FIELDS = ["sku", "action", "extIdType", "extId", "condition", "channel", "quantity", "price"];

/** Valori delle tendine, per lingua del template. */
const VOCAB = {
  it_IT: { create: "Crea o modifica", delete: "Elimina", ean: "EAN", conditionNew: "Nuovo", channelMerchant: "Gestito dal venditore (default)" },
  en_GB: { create: "Create or update", delete: "Delete", ean: "EAN", conditionNew: "New", channelMerchant: "Merchant Fulfilled (default)" },
  de_DE: { create: "Erstellen oder aktualisieren", delete: "Löschen", ean: "EAN", conditionNew: "Neu", channelMerchant: "Vom Verkäufer versandt (Standard)" },
  fr_FR: { create: "Créer ou mettre à jour", delete: "Supprimer", ean: "EAN", conditionNew: "Neuf", channelMerchant: "Expédié par le vendeur (par défaut)" },
  es_ES: { create: "Crear o actualizar", delete: "Eliminar", ean: "EAN", conditionNew: "Nuevo", channelMerchant: "Gestionado por el vendedor (predeterminado)" },
};

/**
 * Attributi di cui ci serve il valore localizzato, e come si chiama la lista
 * nel foglio "Dropdown Lists" del template.
 */
export const VOCAB_ATTRS = {
  action:    /^::record_action$/,
  extIdType: /^externally_assigned_product_identifier#1\.type$/,
  condition: /^condition_type#1\.value$/,
  channel:   /^fulfillment_availability#1\.fulfillment_channel_code$/,
};

/**
 * Ricava il vocabolario dalle liste del template.
 *
 * La tabella VOCAB qui sotto e' hardcodata per lingua, e le stringhe non
 * italiane non sono verificate contro un template reale: se una sola non
 * combacia, Amazon rifiuta tutte le righe. Il template invece porta i valori
 * validi nel foglio "Dropdown Lists" (riga 3 = nome tecnico, righe 4+ = valori
 * nella lingua del file), quindi la fonte giusta e' quella. La tabella resta
 * solo come ultima spiaggia per i template salvati prima di questa modifica.
 *
 * Nelle liste di Amazon il primo valore e' quello "normale": per ::record_action
 * e' la creazione/aggiornamento, per condition_type e' il nuovo, per il canale
 * e' la gestione da parte del venditore. L'EAN si riconosce dal codice, che non
 * e' tradotto.
 */
export function vocabFromLists(lists) {
  if (!lists) return null;
  const a = lists.action, c = lists.condition, ch = lists.channel, e = lists.extIdType;
  if (!a?.length || !c?.length || !ch?.length || !e?.length) return null;
  const ean = e.find(v => /^ean$/i.test(String(v).trim()));
  if (!ean) return null;                       // senza EAN non possiamo identificare il prodotto
  if (a.length < 2) return null;               // servono creazione e cancellazione
  return {
    create: a[0], delete: a[a.length - 1],
    ean, conditionNew: c[0], channelMerchant: ch[0],
    origine: "template",
  };
}

export function vocabFor(template) {
  const dal = vocabFromLists(template?.vocabLists);
  if (dal) return dal;
  const lang = template?.contentLanguageTag || "it_IT";
  const fallback = VOCAB[lang] || VOCAB.it_IT;
  return { ...fallback, origine: VOCAB[lang] ? "tabella:" + lang : "tabella:it_IT (lingua non riconosciuta)" };
}

/**
 * Estrae le liste dal foglio "Dropdown Lists" di un workbook SheetJS.
 * Vive qui perche' la usano sia il browser sia lo script che rigenera il seed.
 */
export function extractVocabLists(workbook, XLSX) {
  const name = workbook.SheetNames.find(n => n === "Dropdown Lists")
            || workbook.SheetNames.find(n => /dropdown|tendina|listas|listes/i.test(n));
  if (!name) return null;
  const sh = workbook.Sheets[name];
  if (!sh || !sh["!ref"]) return null;
  const rng = XLSX.utils.decode_range(sh["!ref"]);
  const at = (r, c) => { const k = XLSX.utils.encode_cell({ r, c }); return sh[k] ? String(sh[k].v).trim() : ""; };
  const out = {};
  for (let c = 0; c <= rng.e.c; c++) {
    const attr = at(2, c);                     // riga 3: nome tecnico dell'attributo
    if (!attr) continue;
    for (const [field, re] of Object.entries(VOCAB_ATTRS)) {
      if (out[field] || !re.test(attr)) continue;
      const vals = [];
      for (let r = 3; r <= rng.e.r; r++) { const v = at(r, c); if (v === "") break; vals.push(v); }
      if (vals.length) out[field] = vals;
    }
  }
  return Object.keys(out).length ? out : null;
}

/**
 * Costruisce l'indice colonna per ogni campo logico.
 * Ritorna { cols: {campo: indice0based}, missing: [campi obbligatori non trovati] }
 */
export function resolveColumns(template) {
  const attrRowIdx = (template.attributeRow || 5) - 1;
  const attrs = (template.headerRows?.[attrRowIdx] || []).map(v => String(v ?? "").trim());
  const cols = {};
  for (const [field, re] of Object.entries(FIELD_PATTERNS)) {
    const i = attrs.findIndex(a => re.test(a));
    if (i >= 0) cols[field] = i;
  }
  const missing = REQUIRED_FIELDS.filter(f => cols[f] === undefined);
  return { cols, missing, attrs };
}

/** Validazione minima di un template caricato dall'utente. */
export function validateTemplate(t) {
  const errors = [];
  if (!t || typeof t !== "object") return ["Template non valido"];
  if (!Array.isArray(t.headerRows) || t.headerRows.length < 6) errors.push("Righe di intestazione mancanti (servono le prime 6 righe del foglio Modello)");
  if (!t.nCols || t.nCols < 10) errors.push("Numero di colonne non plausibile");
  const first = String(t.headerRows?.[0]?.[0] || "");
  if (!first.startsWith("settings=")) errors.push('La cella A1 non inizia con "settings=": il foglio non sembra un template Amazon');
  if (errors.length === 0) {
    const { missing } = resolveColumns(t);
    if (missing.length) errors.push("Colonne obbligatorie non trovate: " + missing.join(", "));
  }
  return errors;
}

export function templateInfo(t) {
  const { cols, missing } = resolveColumns(t);
  return {
    source: t.source || null,
    sheetName: t.sheetName || "Modello",
    nCols: t.nCols,
    dataRow: t.dataRow,
    labelRow: t.labelRow,
    attributeRow: t.attributeRow,
    marketplaceId: t.marketplaceId || null,
    contentLanguageTag: t.contentLanguageTag || null,
    uploadedAt: t.uploadedAt || null,
    isSeed: !!t.isSeed,
    resolved: Object.keys(cols),
    missing,
    vocab: vocabFor(t),
  };
}

export function seedTemplate() {
  return { ...TEMPLATE_SEED, isSeed: true };
}

/** Template in uso: quello caricato dall'utente, altrimenti il default del repo. */
export async function loadTemplate() {
  const stored = await templateStore().get(TEMPLATE_KEY, { type: "json" });
  return stored || seedTemplate();
}
