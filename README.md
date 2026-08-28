# amz-inventory-converter

Converte i listini CSV dei fornitori nel **Modello di caricamento delle offerte** di
Amazon Seller Central (il formato che ha sostituito l'Inventory Loader classico).

Deploy: <https://amz-inventory-converter.netlify.app>

---

## Come si usa, ogni giorno

1. Alle **9:15** (10:15 con l'ora legale), da lunedì a sabato, un job automatico scarica i
   CSV dei fornitori dall'FTP e prepara il file.
2. Apri il sito, tab **🤖 Auto**, e scarica il file: prima prova **.xlsx**, il **.txt**
   tab-delimited è l'alternativa se il caricamento lo rifiuta.
3. Su Seller Central: *Catalogo → Aggiungi prodotti tramite caricamento* e carica con
   **"Aggiungi prodotti e modifica il tuo inventario"**.

> ⚠️ **Non usare "Modifica e sostituisci il tuo inventario".**
> Gli articoli non più disponibili dal fornitore sono già nel file con **quantità 0**:
> l'offerta resta a catalogo, non è acquistabile, e si riattiva da sola quando torna
> disponibile. Cancellare e ricreare l'intero catalogo ogni giorno consuma il limite
> settimanale di creazione offerte (errore 8571), lascia le offerte cancellate durante
> l'elaborazione del feed, e collide con la finestra di 24 ore per il riuso delle SKU.

---

## Il formato del template

Il foglio `Modello` del template Amazon è strutturato così:

| Riga | Contenuto |
|---|---|
| 1 | `settings=...`, `settings2=...`, `settings3=...` — la stringa di configurazione, spezzata su più celle perché Excel non accetta più di 32.767 caratteri per cella |
| 2 | avviso "Compila questo modello in ITALIANO…" |
| 3 | nome del gruppo di attributi |
| 4 | etichetta locale (`labelRow`) |
| 5 | **nome tecnico del campo** (`attributeRow`) ← la riga che conta |
| 6 | riga di esempio (Amazon la ignora in fase di caricamento) |
| 7+ | dati (`dataRow`) |

Le colonne **non sono hardcodate per posizione**: vengono risolte per pattern sulla riga 5.
Così il tool continua a funzionare se Amazon aggiunge, sposta o rinomina colonne, e
funziona su qualsiasi marketplace (l'attributo del prezzo contiene il marketplace id,
`APJ6JRA9NG5V4` per Amazon.it).

Campi compilati:

| Campo | Valore |
|---|---|
| `contribution_sku#1.value` | SKU (nella configurazione attuale coincide con l'EAN) |
| `::record_action` | `Crea o modifica` |
| `externally_assigned_product_identifier#1.type` / `.value` | `EAN` / il codice, **solo se non c'è un ASIN verificato** |
| `condition_type#1.value` | `Nuovo` |
| `fulfillment_availability#1.fulfillment_channel_code` | `Gestito dal venditore (default)` |
| `fulfillment_availability#1.quantity` | quantità (0 = disattivazione) |
| `fulfillment_availability#1.lead_time_to_ship_max_days` | tempo di gestione |
| `merchant_suggested_asin#1.value` | ASIN, **quando esiste un abbinamento verificato** |
| `purchasable_offer[...]#1.our_price#1.schedule#1.value_with_tax` | prezzo — **cella numerica**, non testo (vedi sotto) |
| `...minimum_seller_allowed_price...` | opzionale, se imposti un margine minimo nelle Regole |

### Quando Amazon cambia il template

Scaricalo da *Catalogo → Aggiungi prodotti tramite caricamento → Scarica un foglio di
calcolo* e caricalo nel tab **📄 Template**. Le colonne vengono rilevate automaticamente e
non serve toccare il codice. Il tab mostra quali campi ha riconosciuto e quali mancano.

Nel repo c'è un template di default (`_lib/templateSeed.mjs`, estratto da
`InventoryLoader_IT_20260820New_Version.xlsm`) usato come fallback finché non ne carichi uno.

---

## Tab

| Tab | A cosa serve |
|---|---|
| 🤖 **Auto** | file pronto, generazione manuale da FTP, quarantena, download .xlsx/.txt |
| 📄 **Template** | carica il template Amazon, verifica i campi riconosciuti |
| 🔗 **ASIN** | mappa verificata EAN→ASIN e registro delle SKU pubblicate |
| ⚙️ **Regole** | stock minimo, tetto quantità, giorni di quantità 0, soglie del guard-rail |
| ⚡ **Manuale** | percorso di riserva: carichi i CSV a mano se l'FTP non risponde |
| 🏭 **Fornitori** | colonne del CSV, delimitatore, cartella FTP, fasce di rincaro |
| 🌍 **Marketplace** | quantità di fallback e tempo di gestione |
| 🚫 **Blacklist** | EAN da non pubblicare mai |
| 📊 **Storico** | trend delle conversioni |

---

## Controllo di sicurezza (guard-rail)

Prima il risultato veniva salvato come "ultimo pronto" e solo dopo si calcolavano gli
alert: un CSV fornitore troncato produceva un file mutilato già scaricabile, con l'avviso
in ritardo.

Ora, prima di pubblicare, il risultato viene bloccato se:

- ci sono errori nella lettura dei CSV;
- i prodotti sono meno di `minProducts` (default 100);
- la variazione rispetto al giorno prima supera `maxDeltaPct` (default 20%).

In quel caso finisce in **quarantena**: l'ultimo file buono non viene toccato e nel tab Auto
compaiono i motivi del blocco con i pulsanti *Pubblica comunque* / *Scarta* / *Rigenera e forza*.

---

## Architettura

```
Fornitori → FTPS  (netlify/functions/_lib/ftp.mjs)
   ↓
scheduled-convert.mjs   (cron "15 8 * * 1-6")
   ↓ parseCSV → validazione EAN → dedup → fasce di rincaro → quantità 0
   ↓ safetyCheck
Blobs:  ftp-results/latest | ftp-results/pending
        amz-published/skus         ← storico delle SKU pubblicate
        amz-template/current       ← template caricato
        conversion-history/*       ← storico (potato a 180 record)
        amz-alerts/active
   ↓
index.html  → expandRecords → buildXlsx / buildTxt  (SheetJS nel browser)
```

I record salvati su Blobs sono **compatti** (`{sku, ean, qty, price}`): righe larghe 140
colonne significherebbero megabyte di stringhe vuote da salvare e da spedire al browser.
L'espansione al formato template avviene solo al momento del download.

### API

| Endpoint | Metodi |
|---|---|
| `/api/config` | GET, POST |
| `/api/template` | GET (`?full=1` per le intestazioni), POST, DELETE |
| `/api/asinmap` | GET (`?full=1`), POST (CSV, `?replace=1`), DELETE |
| `/api/published` | GET, POST (report offerte attive), DELETE |
| `/api/ftp-convert` | GET (`?rows=0` per la sola diagnostica), POST (`{force}`) |
| `/api/publish` | POST `{action:"promote"\|"discard"}` |
| `/api/history` | GET, POST, DELETE `?id=` |
| `/api/alerts` | GET, POST `{action}` |

### Variabili d'ambiente Netlify

```
FTP_HOST, FTP_PORT, FTP_USER, FTP_PASS
```

---

## Test

```bash
npm install
npm test
```

`test.mjs` copre la conversione lato server (validazione EAN, parser CSV con campi
quotati, fasce, disattivazione a quantità 0, guard-rail, struttura del .txt e del .xlsx,
e un confronto con il template compilato a mano). `test_client.mjs` verifica che il
parsing del template nel browser ricostruisca esattamente lo stesso template e che
client e server producano righe identiche.

---

## Nota sull'accesso

Il sito e le API sono **pubblici**: chiunque conosca l'URL può leggere `/api/config`
(fornitori, fasce di rincaro) e `/api/ftp-convert` (prezzi, e i prezzi di costo nel campo
`previewData`). È una scelta consapevole. Se un domani vuoi chiuderlo senza dover digitare
una password, la strada più comoda è un token nell'URL salvato nel browser al primo
accesso.

---

## Due lezioni pagate care

### Il prezzo nell'`.xlsx` deve essere una cella numerica

Scrivendolo come testo con la virgola (`149,10`) Amazon rifiuta **tutte** le righe con
l'errore **13006** — *"Formato cella errato. Imposta il formato in Excel su Numero"*.
La regola della virgola decimale per il marketplace italiano vale per il **file di testo
tab-delimited**, dove non esistono tipi di cella; in un `.xlsx` un numero è un numero e
non ha separatore. È la regola giusta applicata al formato sbagliato: 7.917 righe
rifiutate in un colpo.

I test controllano il **tipo** della cella rileggendo il file generato, non solo il valore.

### L'ASIN va scritto, non fatto indovinare

Il template identifica il prodotto con l'ASIN **oppure** con un identificativo esterno.
Mandando solo l'EAN, l'abbinamento alla pagina prodotto lo decide Amazon — e su 5.571
offerte attive **94 erano finite sulla scheda di un pneumatico di misura o marca diversa**.
Due sono state vendute: un Hankook 195/65 R15 estivo venduto come Michelin Alpin 7
245/40 R18 invernale.

Gli errori si concentravano nelle famiglie a variazioni (Turanza 6, AllSeasonContact 2):
l'EAN si attacca al "figlio" sbagliato e la misura risulta vicina ma diversa. Ogni
"Elimina e sostituisci" cancella e ricrea tutte le offerte, cioè **rifà l'abbinamento da
zero per l'intero catalogo**: farlo ogni giorno moltiplicava le occasioni di sbagliare.

Il rimedio è la mappa verificata: `audit_asin.mjs` confronta marca e misura del listino
fornitore con il titolo della pagina Amazon, tiene solo le coppie coerenti, e il tool
scrive quell'ASIN nel file. Le offerte così pinnate non possono più spostarsi.

```bash
node audit_asin.mjs report_offerte_attive.txt listino_deldo.csv
# produce report/mappa_ean_asin_verificata.csv → si carica nel tab 🔗 ASIN
```

### Mappare gli EAN che non hanno ancora un ASIN, senza farlo a mano

Seller Central ha una **ricerca prodotti in blocco**: incolli un elenco di EAN e
scarichi un *Modello di caricamento delle offerte precompilato*. Quel file contiene
tre colonne di riferimento in più rispetto al template normale:

| colonna | contenuto |
|---|---|
| `::your_search_term` | l'EAN che hai cercato |
| `::recommended_action` | es. "Pronto per l'offerta" |
| `::amazon_title` | il titolo della pagina Amazon |

più `merchant_suggested_asin#1.value` già compilato: è la mappa EAN→ASIN scritta da
Amazon. Le istruzioni dentro quel file dicono *"Ti consigliamo di verificare i
dettagli precompilati per assicurarti che la tua offerta appaia sul prodotto giusto"* —
ed è esattamente ciò che fa l'audit, confrontando marca e misura del titolo con
quelle del listino fornitore:

```bash
node audit_asin.mjs ListingLoader_precompilato.xlsm listino_deldo.csv report/mappa_ean_asin_verificata.csv
```

Il terzo argomento è la mappa esistente: le coppie nuove ci vengono **unite**, non
la sostituiscono. Gli EAN che cambiano ASIN rispetto a prima finiscono in
`report/asin_cambiati.csv` da controllare a mano. Le righe cercate per ASIN invece
che per EAN, e quelle per cui Amazon non restituisce un ASIN, vengono contate a
parte e non entrano in mappa.

Nel tab ASIN c'è anche l'opzione **"pubblica solo gli EAN con ASIN verificato"**: il modo
più sicuro, quello che non è verificato non va in vendita.

### Il registro va inizializzato dalle offerte attive, non dal listino

Il registro `amz-published` decide chi esce con quantità 0. Costruendolo dal listino del
fornitore, le offerte attive su Amazon il cui EAN il fornitore non manda più non entravano
mai nel registro: restavano acquistabili a tempo indeterminato su merce non ordinabile
(92 casi). Si carica il report offerte attive dal tab 🔗 ASIN e quelle SKU entrano nel
registro, uscendo a quantità 0 al primo file utile.
