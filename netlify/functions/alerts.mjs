/**
 * /api/alerts
 * GET  → restituisce gli alert attivi
 * POST { action: "read", id } → segna un alert come letto
 * POST { action: "read-all" } → segna tutti come letti
 * POST { action: "clear"   } → elimina tutti i letti
 */

import { getStore } from "@netlify/blobs";

function getAlertStore() {
  return getStore({ name: "amz-alerts", consistency: "strong" });
}

export default async (req) => {
  const store = getAlertStore();

  if (req.method === "GET") {
    try {
      const alerts = await store.get("active", { type: "json" }) || [];
      // Ordina per data decrescente, non letti prima
      alerts.sort((a, b) => {
        if (a.read !== b.read) return a.read ? 1 : -1;
        return new Date(b.created_at) - new Date(a.created_at);
      });
      return new Response(JSON.stringify(alerts), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500 });
    }
  }

  if (req.method === "POST") {
    try {
      const body = await req.json();
      let alerts = await store.get("active", { type: "json" }) || [];

      if (body.action === "read" && body.id) {
        alerts = alerts.map(a => a.id === body.id ? { ...a, read: true } : a);
      } else if (body.action === "read-all") {
        alerts = alerts.map(a => ({ ...a, read: true }));
      } else if (body.action === "clear") {
        alerts = alerts.filter(a => !a.read);
      }

      await store.setJSON("active", alerts);
      return new Response(JSON.stringify({ ok: true }), {
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
  path: "/api/alerts",
};
