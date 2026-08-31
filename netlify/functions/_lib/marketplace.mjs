/**
 * Impostazioni per marketplace.
 *
 * Fino alla v6.1 esisteva un solo marketplace: una configurazione, un template,
 * una blacklist, un registro, una mappa ASIN. Aggiungere la Germania senza
 * separarli significherebbe che caricare il template DE sostituisce quello
 * italiano, e — molto peggio — che il registro delle SKU pubblicate si incrocia
 * fra i due cataloghi: un EAN presente su DE e non su IT si spegnerebbe a
 * vicenda.
 *
 * Qui c'e' UN solo posto che decide, per un dato marketplace, quali impostazioni
 * valgono e su quale chiave dei blob si legge e si scrive. Se questa logica
 * finisse sparsa fra converter, handler e interfaccia, prima o poi due pezzi
 * userebbero chiavi diverse e il registro si corromperebbe in silenzio.
 */

/** Valori di default, usati quando ne' il marketplace ne' la config li fissano. */
export const MK_DEFAULTS = {
  leadtime: 3,
  quantity: 10,
  minStock: 0,
  maxQty: 0,
  floorMarginPct: 0,
  zeroKeepDays: 90,
  onlyMapped: false,
  dupMode: "price",
  qtyMode: "catalog",
  maxDeltaPct: 20,
  minProducts: 100,
};

/** Campi che un marketplace puo' sovrascrivere rispetto alla config globale. */
const OVERRIDABLE = [
  "leadtime", "quantity", "minStock", "maxQty", "floorMarginPct",
  "zeroKeepDays", "onlyMapped", "dupMode", "qtyMode", "maxDeltaPct", "minProducts",
];

/** Chiave di blob per marketplace. `keyFor("skus", "DE")` → "skus-DE". */
export function keyFor(base, code) {
  if (!code) throw new Error("keyFor: codice marketplace mancante");
  return `${base}-${String(code).toUpperCase()}`;
}

/** Il primo marketplace configurato: quello che eredita i dati pre-esistenti. */
export function primaryCode(config) {
  return config?.marketplaces?.[0]?.code || "IT";
}

/**
 * Marketplace su cui generare. `enabled: false` lo esclude senza cancellarlo,
 * cosi' si puo' sospendere la Germania senza perderne la configurazione.
 */
export function activeMarketplaces(config) {
  return (config?.marketplaces || []).filter(m => m?.code && m.enabled !== false);
}

export function findMarketplace(config, code) {
  const want = String(code || "").toUpperCase();
  return (config?.marketplaces || []).find(m => String(m?.code || "").toUpperCase() === want) || null;
}

/**
 * Le impostazioni effettive per un marketplace.
 *
 * Precedenza: valore sul marketplace → valore globale nella config → default.
 * Cosi' una configurazione scritta prima di questa modifica continua a produrre
 * esattamente gli stessi numeri: nessun marketplace ha override, quindi vince
 * sempre il valore globale, che e' quello che veniva usato prima.
 */
export function settingsFor(config, code) {
  const mk = findMarketplace(config, code);
  if (!mk) throw new Error(`Marketplace ${code} non trovato nella configurazione`);
  const out = { code: String(mk.code).toUpperCase() };
  for (const f of OVERRIDABLE) {
    out[f] = mk[f] ?? config?.[f] ?? MK_DEFAULTS[f];
  }
  // Blacklist. La distinzione fra "assente" e "vuota" e' sostanziale:
  //   assente  → configurazione non ancora migrata: il primario eredita la
  //              lista globale storica (i 223 EAN gia' caricati)
  //   vuota [] → scelta esplicita di non bloccare niente, e va rispettata
  // Confondere le due cose farebbe resuscitare una blacklist svuotata a mano.
  const isPrimary = out.code === String(primaryCode(config)).toUpperCase();
  out.blacklist = Array.isArray(mk.blacklist)
    ? mk.blacklist
    : (isPrimary && Array.isArray(config?.blacklist) ? config.blacklist : []);
  out.alertThresholds = { ...(config?.alertThresholds || {}), ...(mk.alertThresholds || {}) };
  return out;
}

/**
 * Fasce di rincaro del fornitore per questo marketplace.
 * `tiersByMarket` ha la precedenza; in mancanza si usano le fasce del fornitore,
 * che sono quelle storiche.
 */
export function tiersFor(supplier, code) {
  const byMarket = supplier?.tiersByMarket;
  const want = String(code || "").toUpperCase();
  if (byMarket && Array.isArray(byMarket[want]) && byMarket[want].length) return byMarket[want];
  return supplier?.tiers || [];
}

/**
 * Migrazione della configurazione al formato per marketplace.
 *
 * Sposta la blacklist globale sul primo marketplace. Va fatta in un solo punto,
 * attraversata sia dalla lettura sia dalla scrittura, altrimenti resterebbero
 * due fonti di verita' per la stessa lista e vincerebbe l'ultimo che scrive.
 */
export function migrateConfig(raw) {
  if (!raw || typeof raw !== "object") return raw;
  const cfg = { ...raw };
  const mks = (cfg.marketplaces || []).map(m => ({ ...m }));
  if (mks.length === 0) return cfg;

  const legacy = Array.isArray(cfg.blacklist) ? cfg.blacklist : [];
  if (!Array.isArray(mks[0].blacklist)) {
    // Prima migrazione: la lista globale diventa quella del marketplace primario.
    mks[0].blacklist = legacy;
  }
  for (const m of mks) if (!Array.isArray(m.blacklist)) m.blacklist = [];

  cfg.marketplaces = mks;
  // Il campo globale resta per compatibilita' ma non e' piu' la fonte: lo
  // teniamo allineato al primario cosi' un client vecchio vede numeri sensati.
  cfg.blacklist = mks[0].blacklist;
  return cfg;
}

/** Riepilogo per marketplace, per l'interfaccia e per la diagnostica. */
export function marketplaceSummary(config) {
  return (config?.marketplaces || []).map(m => {
    const s = settingsFor(config, m.code);
    return {
      code: s.code,
      enabled: m.enabled !== false,
      leadtime: s.leadtime,
      minStock: s.minStock,
      maxQty: s.maxQty,
      onlyMapped: s.onlyMapped,
      blacklist: s.blacklist.length,
      tiersPerFornitore: Object.fromEntries(
        (config.suppliers || []).map(sup => [sup.name, tiersFor(sup, s.code).length])
      ),
    };
  });
}
