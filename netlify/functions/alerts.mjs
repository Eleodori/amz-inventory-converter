/**
 * /api/alerts
 * GET  → alert attivi (non letti prima, poi per data decrescente)
 * POST { action: "read", id } | { action: "read-all" } | { action: "clear" }
 */

import { alertStore, json } from "./_lib/stores.mjs";

export default async (req) => {
  const store = alertStore();

  if (req.method === "GET") {
    try {
      const alerts = (await store.get("active", { type: "json" })) || [];
      alerts.sort((a, b) => {
        if (a.read !== b.read) return a.read ? 1 : -1;
        return new Date(b.created_at) - new Date(a.created_at);
      });
      return json(alerts);
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  if (req.method === "POST") {
    try {
      const body = await req.json();
      let alerts = (await store.get("active", { type: "json" })) || [];

      if (body.action === "read" && body.id) alerts = alerts.map(a => (a.id === body.id ? { ...a, read: true } : a));
      else if (body.action === "read-all") alerts = alerts.map(a => ({ ...a, read: true }));
      else if (body.action === "clear") alerts = alerts.filter(a => !a.read); // elimina i letti

      await store.setJSON("active", alerts);
      return json({ ok: true, count: alerts.length });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }

  return json({ error: "Method not allowed" }, 405);
};

export const config = { path: "/api/alerts" };
