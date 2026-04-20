import { getStore } from "@netlify/blobs";

const CONFIG_KEY = "app-config";

function getConfigStore() {
  return getStore({ name: "amz-config", consistency: "strong" });
}

export default async (req) => {
  const store = getConfigStore();

  // GET /api/config → carica la configurazione
  if (req.method === "GET") {
    try {
      const config = await store.get(CONFIG_KEY, { type: "json" });
      if (!config) {
        return new Response(JSON.stringify(null), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response(JSON.stringify(config), {
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

  // POST /api/config → salva la configurazione
  if (req.method === "POST") {
    try {
      const body = await req.json();
      await store.setJSON(CONFIG_KEY, {
        ...body,
        updated_at: new Date().toISOString(),
      });
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
  path: "/api/config",
};
