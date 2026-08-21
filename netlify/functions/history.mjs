/**
 * /api/history
 * GET    → elenco conversioni (decrescente)
 * POST   → salva un record (usato dalla conversione manuale nel browser)
 * DELETE ?id=... → elimina un record
 */

import { historyStore, saveHistoryRecord, json } from "./_lib/stores.mjs";

export default async (req) => {
  const store = historyStore();

  if (req.method === "GET") {
    try {
      const { blobs } = await store.list();
      const records = await Promise.all(blobs.map(({ key }) => store.get(key, { type: "json" })));
      const sorted = records.filter(Boolean).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      return json(sorted);
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "POST") {
    try {
      const body = await req.json();
      const record = {
        id: crypto.randomUUID(),
        created_at: new Date().toISOString(),
        marketplace: body.marketplace,
        total_products: body.total_products,
        total_rows: body.total_rows,
        zeroed: body.zeroed,
        total_read: body.total_read,
        total_skipped: body.total_skipped,
        bad_ean: body.bad_ean,
        below_min_stock: body.below_min_stock,
        duplicates_resolved: body.duplicates_resolved,
        blacklisted: body.blacklisted,
        by_supplier: body.by_supplier,
        by_tier: body.by_tier,
        avg_price_by_tier: body.avg_price_by_tier,
        avg_price_total: body.avg_price_total,
        source: body.source || "browser",
      };
      await saveHistoryRecord(record); // include la potatura, che prima girava solo qui
      return json({ ok: true, id: record.id }, 201);
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "DELETE") {
    try {
      const id = new URL(req.url).searchParams.get("id");
      if (!id) return json({ error: "id mancante" }, 400);
      const { blobs } = await store.list();
      const match = blobs.find(({ key }) => key.includes(id));
      if (match) await store.delete(match.key);
      return json({ ok: true });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/history" };
