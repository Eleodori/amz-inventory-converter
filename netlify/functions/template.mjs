/**
 * /api/template
 *
 * GET  → template attualmente in uso (+ diagnostica delle colonne risolte)
 * POST → salva un nuovo template
 *        body: { headerRows, nCols, labelRow, attributeRow, dataRow,
 *                marketplaceId, contentLanguageTag, sheetName, source }
 *        Le righe di intestazione le estrae il browser dal .xlsm scaricato da
 *        Seller Central (parsing con SheetJS), qui validiamo e persistiamo.
 * DELETE → torna al template di default incluso nel repo
 */

import { templateStore, TEMPLATE_KEY, json } from "./_lib/stores.mjs";
import { validateTemplate, templateInfo, seedTemplate, loadTemplate } from "./_lib/template.mjs";

export default async (req) => {
  const store = templateStore();

  if (req.method === "GET") {
    try {
      const t = await loadTemplate();
      const url = new URL(req.url);
      if (url.searchParams.get("full") === "1") return json({ info: templateInfo(t), template: t });
      return json({ info: templateInfo(t) });
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
        uploadedAt: new Date().toISOString(),
      };
      const errors = validateTemplate(t);
      if (errors.length) return json({ error: errors.join(" · "), errors }, 400);

      await store.setJSON(TEMPLATE_KEY, t);
      return json({ ok: true, info: templateInfo(t) });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "DELETE") {
    try {
      await store.delete(TEMPLATE_KEY);
      return json({ ok: true, info: templateInfo(seedTemplate()) });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/template" };
