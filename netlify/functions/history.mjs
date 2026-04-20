import { getStore } from "@netlify/blobs";

const STORE_NAME = "conversion-history";
const MAX_RECORDS = 90; // ~3 mesi di storico assumendo 1 conversione/giorno

function getHistoryStore() {
  // Usa global store in produzione, deploy store altrove
  if (Netlify.context?.deploy?.context === "production") {
    return getStore({ name: STORE_NAME, consistency: "strong" });
  }
  return getStore({ name: STORE_NAME, consistency: "strong" });
}

export default async (req) => {
  const store = getHistoryStore();

  // GET /api/history → lista tutte le conversioni
  if (req.method === "GET") {
    try {
      const { blobs } = await store.list();

      const records = await Promise.all(
        blobs.map(async ({ key }) => {
          const data = await store.get(key, { type: "json" });
          return data;
        })
      );

      // Ordina per data decrescente, rimuovi null
      const sorted = records
        .filter(Boolean)
        .sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

      return new Response(JSON.stringify(sorted), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  // POST /api/history → salva una nuova conversione
  if (req.method === "POST") {
    try {
      const body = await req.json();

      const record = {
        id: crypto.randomUUID(),
        created_at: new Date().toISOString(),
        marketplace: body.marketplace,
        total_products: body.total_products,
        total_read: body.total_read,
        total_skipped: body.total_skipped,
        duplicates_resolved: body.duplicates_resolved,
        blacklisted: body.blacklisted,
        by_supplier: body.by_supplier,        // { "Deldo": 1200 }
        by_tier: body.by_tier,                // { "≤50": 300, "≤100": 800, ... }
        avg_price_by_tier: body.avg_price_by_tier, // { "≤50": 45.2, ... }
        avg_price_total: body.avg_price_total,
      };

      const key = `conv-${record.created_at}-${record.id}`;
      await store.setJSON(key, record);

      // Pulizia: mantieni solo gli ultimi MAX_RECORDS record
      const { blobs } = await store.list();
      if (blobs.length > MAX_RECORDS) {
        const sorted = blobs.sort((a, b) => a.key.localeCompare(b.key));
        const toDelete = sorted.slice(0, blobs.length - MAX_RECORDS);
        await Promise.all(toDelete.map(({ key }) => store.delete(key)));
      }

      return new Response(JSON.stringify({ ok: true, id: record.id }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  // DELETE /api/history?id=... → elimina un record
  if (req.method === "DELETE") {
    try {
      const url = new URL(req.url);
      const id = url.searchParams.get("id");
      if (!id) {
        return new Response(JSON.stringify({ error: "id mancante" }), { status: 400 });
      }

      const { blobs } = await store.list();
      const match = blobs.find(({ key }) => key.includes(id));
      if (match) await store.delete(match.key);

      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  return new Response("Method not allowed", { status: 405 });
};

export const config = {
  path: "/api/history",
};
