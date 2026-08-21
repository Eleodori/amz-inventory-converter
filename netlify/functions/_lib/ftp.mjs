import * as ftp from "basic-ftp";
import { PassThrough } from "stream";

/**
 * Scarica il primo .csv trovato nella cartella di ogni fornitore.
 * Struttura attesa sull'FTP:  <dominio>/fornitori/<cartella>/file.csv
 */
export async function fetchAllCSVsFromFTP(suppliers = []) {
  const client = new ftp.Client(60000);
  client.ftp.verbose = false;
  const results = {};
  const problems = [];

  try {
    await client.access({
      host: Netlify.env.get("FTP_HOST"),
      user: Netlify.env.get("FTP_USER"),
      password: Netlify.env.get("FTP_PASS"),
      port: parseInt(Netlify.env.get("FTP_PORT") || "21", 10),
      secure: true,
      secureOptions: { rejectUnauthorized: false }, // certificato intestato a sgvps.net
    });

    let ftpDirs = [];
    try {
      const rootList = await client.list();
      const domainDir = rootList.find(f => f.type === ftp.FileType.Directory && f.name.includes("."));
      if (domainDir) await client.cd(domainDir.name);
      await client.cd("fornitori");
      const list = await client.list();
      ftpDirs = list.filter(f => f.type === ftp.FileType.Directory).map(f => f.name);
    } catch (e) {
      throw new Error(`Cartella "fornitori" non trovata sull'FTP: ${e.message}`);
    }

    for (const sup of suppliers) {
      const wanted = (sup.ftpFolder || sup.name || "").trim();
      const dir = ftpDirs.find(d => d.toLowerCase() === wanted.toLowerCase());
      if (!dir) { problems.push(`${sup.name}: cartella FTP "${wanted}" non trovata`); continue; }
      try {
        await client.cd(dir);
        const files = await client.list();
        const csvFile = files.find(f => f.name.toLowerCase().endsWith(".csv"));
        if (!csvFile) { problems.push(`${sup.name}: nessun .csv in "${dir}"`); await client.cd(".."); continue; }

        const chunks = [];
        const stream = new PassThrough();
        stream.on("data", c => chunks.push(c));
        await client.downloadTo(stream, csvFile.name);
        const buf = Buffer.concat(chunks);
        results[sup.name] = decodeCsv(buf);
        await client.cd("..");
      } catch (e) {
        problems.push(`${sup.name}: errore scaricando "${dir}" (${e.message})`);
        await client.cd("..").catch(() => {});
      }
    }
  } finally {
    client.close();
  }

  return { csvMap: results, problems };
}

/**
 * I CSV dei grossisti non sono sempre UTF-8. Se la decodifica UTF-8 produce
 * caratteri di sostituzione, riproviamo in latin1.
 */
function decodeCsv(buf) {
  const utf8 = buf.toString("utf-8");
  if (!utf8.includes("�")) return utf8;
  try { return new TextDecoder("latin1").decode(buf); }
  catch { return utf8; }
}
