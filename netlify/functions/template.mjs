/**
 * /api/template
 *
 * Un template per marketplace: dieci nomi di colonna contengono l'id del
 * marketplace, quindi il file italiano e quello tedesco sono due file diversi e
 * non condividono niente. Il marketplace si passa con ?mk=IT / ?mk=DE; senza
 * parametro si intende il primario.
 *
 * GET     → template in uso per quel marketplace (+ diagnostica delle colonne)
 * GET ?full=1 → include anche le righe di intestazione
 * POST    → salva un template per quel marketplace
 * DELETE  → rimuove il template di quel marketplace
 */

import { templateStore, TEMPLATE_KEY, json, setForMarket, delForMarket, resolveMarketplaceFromRequest } from "./_lib/stores.mjs";
import { validateTemplate, templateInfo, seedTemplate, loadTemplate } from "./_lib/template.mjs";
import { findMarketplace } from "./_lib/marketplace.mjs";

export default async (req) => {
  const r = await resolveMarketplaceFromRequest(req);
  if (r.error) return json({ error: r.error }, 400);
  const { config: cfg, code, primary } = r;

  if (req.method === "GET") {
    try {
      const t = await loadTemplate(cfg, code);
      if (!t) {
        return json({
          marketplace: code, info: null,
          error: `Nessun template caricato per ${code}. Scaricalo da Seller Central con il marketplace impostato su ${code} e caricalo qui.`,
        });
      }
      const url = new URL(req.url);
      const info = { ...templateInfo(t), marketplace: code };
      if (url.searchParams.get("full") === "1") return json({ marketplace: code, info, template: t });
      return json({ marketplace: code, info });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "POST") {
    try {
      const body = await req.json();
      const t = {
        source: body.source || null,
        sheetName: body.sheetName || "Modello",
        nCols: body.nCols,
        labelRow: body.labelRow || 4,
        attributeRow: body.attributeRow || 5,
        dataRow: body.dataRow || 7,
        marketplaceId: body.marketplaceId || null,
        contentLanguageTag: body.contentLanguageTag || null,
        headerRows: body.headerRows,
        // Le liste dei valori validi ("Dropdown Lists") vanno persistite: senza
        // di esse il vocabolario torna alla tabella hardcodata, dove le stringhe
        // non italiane non sono verificate. Questo handler le scartava.
        vocabLists: body.vocabLists || null,
        uploadedAt: new Date().toISOString(),
      };
      const errors = validateTemplate(t);
      if (errors.length) return json({ error: errors.join(" · "), errors }, 400);

      // Il template deve essere quello del marketplace richiesto: caricare per
      // sbaglio il file italiano su DE produrrebbe un file che Amazon.de rifiuta
      // in blocco, e la diagnosi non sarebbe immediata.
      const mk = findMarketplace(cfg, code);
      if (mk?.marketplaceId && t.marketplaceId && mk.marketplaceId !== t.marketplaceId) {
        return json({
          error: `Questo template e' del marketplace ${t.marketplaceId}, ma lo stai caricando su ${code} ` +
            `(${mk.marketplaceId}). Scaricalo da Seller Central con il marketplace impostato su ${code}.`,
        }, 400);
      }

      await setForMarket(templateStore(), TEMPLATE_KEY, code, t);
      return json({ ok: true, marketplace: code, info: { ...templateInfo(t), marketplace: code } });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "DELETE") {
    try {
      await delForMarket(templateStore(), TEMPLATE_KEY, code);
      // Sul primario si torna al seed del repo, che e' italiano. Su un altro
      // marketplace non c'e' nessun default plausibile: resta senza template.
      if (primary) {
        await templateStore().delete(TEMPLATE_KEY).catch(() => {});
        return json({ ok: true, marketplace: code, info: { ...templateInfo(seedTemplate()), marketplace: code } });
      }
      return json({ ok: true, marketplace: code, info: null });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/template" };
