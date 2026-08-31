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
| 🤖 **Auto** | un blocco per marketplace: file pronto, generazione da FTP, quarantena, download .xlsx/.txt |
| 📄 **Template** | carica il template Amazon, verifica i campi riconosciuti, **uno per marketplace** |
| 🔗 **ASIN** | mappa verificata EAN→ASIN e registro delle SKU pubblicate, **per marketplace** |
| ⚙️ **Regole** | stock minimo, tetto quantità, giorni di quantità 0, soglie del guard-rail |
| ⚡ **Manuale** | percorso di riserva: carichi i CSV a mano se l'FTP non risponde |
| 🏭 **Fornitori** | colonne del CSV, delimitatore, cartella FTP, fasce di rincaro (dedicate o ereditate, per marketplace) |
| 🌍 **Marketplace** | quali cataloghi, id marketplace, quantità di fallback, tempo di gestione, stock minimo |
| 🚫 **Blacklist** | EAN da non pubblicare mai, **per marketplace** |
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

## Più di un marketplace

Un'esecuzione produce **un file per marketplace attivo**. I CSV dei fornitori si scaricano
una volta sola: sono gli stessi articoli, cambia solo come vengono scritti e prezzati.

Cosa è **separato** per marketplace, e perché:

| Dato | Perché non si può condividere |
|---|---|
| **Template** | dieci nomi di colonna contengono l'id del marketplace (`purchasable_offer[marketplace_id=…]`, `merchant_shipping_group[marketplace_id=…]`). Un template italiano caricato su Amazon.de viene rifiutato in blocco. |
| **Registro SKU pubblicate** | un registro condiviso spegnerebbe a vicenda gli EAN presenti su un solo catalogo: un articolo che esiste solo su IT arriverebbe a DE con quantità 0. |
| **Mappa EAN→ASIN** | l'ASIN è per marketplace. Lo stesso EAN può avere pagine diverse su .it e .de, e un ASIN sbagliato pubblica l'offerta sulla pagina di un altro pneumatico. |
| **Blacklist** | le autorizzazioni marca e i blocchi valgono su un catalogo alla volta: una marca bloccata su IT può essere vendibile su DE. |
| **Fasce di rincaro** | opzionali (`tiersByMarket`). Concorrenza e spedizione non sono le stesse. Se un marketplace non ne ha di dedicate usa quelle del fornitore. |
| **Ultimo risultato / quarantena** | il guard-rail confronta ogni marketplace **con se stesso**: confrontare il primo file tedesco con l'ultimo italiano darebbe una variazione del 100% e bloccherebbe sempre. |

Il **primo marketplace della lista è il primario**: eredita i dati salvati quando ne
esisteva uno solo (chiavi Blobs senza suffisso). Gli altri partono vuoti — e devono, perché
"chiave mancante" su DE non può voler dire "usa i dati italiani".

```
Blobs:  ftp-results/latest        ← primario (compatibilità)
        ftp-results/latest-DE     ← ogni altro marketplace
        amz-published/skus-DE
        amz-asinmap/current-DE
        amz-template/current-DE
```

Con **un solo marketplace configurato l'interfaccia resta identica a prima**: il selettore
non compare e i tab non cambiano. Verificato con un rendering headless dedicato.

Un marketplace che fallisce (template mancante, template sbagliato) **non ferma gli altri**:
`convertForMarket` restituisce l'errore invece di sollevarlo, il file italiano esce comunque
e l'errore diventa un alert `[DE] …`.

---

## Architettura

```
Fornitori → FTPS  (netlify/functions/_lib/ftp.mjs)   ← un solo scarico
   ↓
scheduled-convert.mjs   (cron "15 8 * * 1-6")
   ↓ per ogni marketplace attivo:
   ↓   _lib/convertRun.mjs → convertForMarket()
   ↓     parseCSV → validazione EAN → dedup → fasce del marketplace → quantità 0
   ↓     safetyCheck (contro il file precedente DELLO STESSO marketplace)
Blobs:  ftp-results/latest[-CODE] | ftp-results/pending[-CODE]
        amz-published/skus[-CODE]      ← registro delle SKU pubblicate
        amz-asinmap/current[-CODE]     ← mappa verificata EAN→ASIN
        amz-template/current[-CODE]    ← template caricato
        conversion-history/*           ← storico (potato a 180 record)
        amz-alerts/active
   ↓
index.html  → expandRecords → buildXlsx / buildTxt  (SheetJS nel browser)
```

`_lib/marketplace.mjs` è l'unico posto che risolve impostazioni e chiavi:
`settingsFor(config, code)` (override del marketplace → globale → default),
`tiersFor(supplier, code)`, `keyFor(base, code)`, `migrateConfig(raw)`.
`_lib/convertRun.mjs` è l'unica conversione: il job notturno e il pulsante "Genera adesso"
chiamano la stessa funzione, così non possono divergere come facevano quando erano due copie.

I record salvati su Blobs sono **compatti** (`{sku, ean, qty, price}`): righe larghe 140
colonne significherebbero megabyte di stringhe vuote da salvare e da spedire al browser.
L'espansione al formato template avviene solo al momento del download.

### API

| Endpoint | Metodi |
|---|---|
| `/api/config` | GET, POST (con `rev` per il controllo di concorrenza) |
| `/api/template` | GET (`?full=1` per le intestazioni), POST, DELETE — tutti con `?mk=CODICE` |
| `/api/asinmap` | GET (`?full=1`), POST (CSV, `?replace=1`), DELETE — tutti con `?mk=CODICE` |
| `/api/published` | GET, POST (report offerte attive), DELETE — tutti con `?mk=CODICE` |
| `/api/ftp-convert` | GET (`?rows=0` diagnostica, `?mk=` un solo marketplace), POST (`{force, marketplace}`) |
| `/api/publish` | POST `{action:"promote"\|"discard", marketplace}` |
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

---

## Un catalogo nuovo, da zero

1. Tab **🌍 Marketplace** → *+ Aggiungi* → scegli il paese. L'id marketplace si compila da
   solo per i paesi verificati; per gli altri lo si legge dal template al primo caricamento.
2. Tab **📄 Template** → scegli il marketplace nel selettore → scarica il template **da
   Seller Central con quel marketplace selezionato** e caricalo. Un template di un altro
   marketplace viene rifiutato con l'id sbagliato in chiaro, non salvato in silenzio.
3. Tab **🏭 Fornitori** → *Modifica* → *Fasce per* → il nuovo marketplace. Di default
   eredita le fasce del primario; *Fasce dedicate* ne crea una copia modificabile.
4. Tab **🚫 Blacklist** e **🔗 ASIN**: partono vuoti. La mappa ASIN di un marketplace non
   vale per un altro e va rifatta con una ricerca prodotti su quel marketplace.
5. Tab **🤖 Auto** → *Genera adesso tutti*. Ogni file va caricato **sul suo marketplace**:
   cambia marketplace in Seller Central prima di caricare.

Un marketplace con la spunta *attivo* togliata non genera file ma **non perde niente**:
template, mappa, blacklist e registro restano dove sono.
