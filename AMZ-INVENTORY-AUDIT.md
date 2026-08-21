# amz-inventory-converter — Audit tecnico e conformità Amazon

**Per**: Michele · MARIT SRL
**Data**: 20 agosto 2026
**Oggetto**: revisione della repo `Eleodori/amz-inventory-converter` + ricerca sulle regole Amazon Seller Central per il caricamento inventario in dropshipping

---

## 0. Sintesi esecutiva

Il codice è pulito e ben strutturato. Il problema non è come è scritto: è **cosa produce e come lo carichi**.

Cinque cose richiedono attenzione, in ordine di urgenza:

| # | Problema | Gravità | Scadenza |
|---|---|---|---|
| 1 | **Il sito è pubblico senza autenticazione**: chiunque legge i tuoi listini, i markup e i prezzi di costo | 🔴 Critico | subito |
| 2 | **OTDR ≥ 90% su Amazon.it**: enforcement dal **1 settembre 2026** → disattivazione offerte | 🔴 Critico | **12 giorni** |
| 3 | **"Modifica e sostituisci" ogni giorno** è esplicitamente sconsigliato da Amazon ("*too risky*") | 🔴 Critico | subito |
| 4 | **Nessun fail-safe**: un CSV Deldo troncato ti cancella il catalogo, e l'alert arriva *dopo* | 🟠 Alto | subito |
| 5 | **Compliance pneumatici** (GPSR, etichetta UE, EPR/PFU) non gestita dal tool | 🟠 Alto | già scaduta |

---

## 1. Cosa fa oggi il tuo sistema

Ho letto tutta la repo. Ricostruzione del flusso:

```
Deldo → FTPS ($FTP_HOST)
          <dominio>/fornitori/DLD/*.csv
   ↓
scheduled-convert.mjs   (cron "15 8 * * 1-6" = 10:15 IT in ora legale, Lun-Sab)
   ↓ parseCSV(";")  →  filtri  →  dedup per EAN  →  applyTiers()
   ↓
Blobs "ftp-results"/latest  +  "conversion-history"  +  "amz-alerts"
   ↓
index.html (tab ⚡Auto)  →  download InventoryLoader_IT_YYYY-MM-DD.txt
   ↓
TU → Seller Central → "Modifica e sostituisci il tuo inventario"
```

**File generato** (`ftp-convert.mjs:200`, `scheduled-convert.mjs:148`, `index.html:516`):

```
sku	product-id	product-id-type	price	item-condition	quantity	add-delete	leadtime-to-ship
```
con `product-id-type=4` (EAN), `item-condition=11` (Nuovo), `add-delete=a`, prezzo con **virgola** decimale per tutti i marketplace tranne UK. Il formato è corretto: è un **Inventory Loader**, tab-delimited, e la virgola decimale è giusta per IT/DE/FR/ES.

**Configurazione live** (letta da `/api/config` — vedi §2):

| | |
|---|---|
| Fornitori | 1 — Deldo, cartella FTP `DLD`, delimitatore `;` |
| Colonne | `skuCol=EAN`, `eanCol=EAN`, `priceCol=Price`, `stockCol=Stock` |
| Markup | **1 solo scaglione**: +10% + 12 € su tutto il catalogo |
| Marketplace | IT — `quantity` fallback 8, `leadtime` **4 giorni** |
| Blacklist | 8 EAN |

Nota: `skuCol` e `eanCol` puntano entrambi su `EAN`, quindi **SKU = EAN**. È una buona notizia per la migrazione (§4): le tue SKU sono stabili e deterministiche.

Il `fatture.html` è un tool separato di fatturazione B2B (Global Way) e non entra in questo flusso.

---

## 2. 🔴 Il sito è completamente pubblico

Questo l'ho verificato, non dedotto. Il progetto Netlify ha `requiresPassword: false` e nel codice **non esiste nessuna forma di autenticazione** — né Basic Auth, né edge function di gating, né controllo nelle functions.

Ho chiamato l'endpoint da fuori, senza credenziali:

```
GET https://amz-inventory-converter.netlify.app/api/config
→ 200 OK
{"suppliers":[{"name":"Deldo",...,"tiers":[{"upTo":null,"markupPct":10,"flatFee":12}]}],
 "marketplaces":[{"code":"IT","quantity":8,"leadtime":4}],
 "blacklist":[...],"updated_at":"2026-08-20T14:47:58.779Z"}
```

Cosa è esposto a chiunque conosca l'URL:

- **il tuo margine esatto** (+10% + 12 €) — un concorrente ricava il tuo prezzo di costo da qualsiasi tuo prezzo pubblico su Amazon;
- il nome dei fornitori e la struttura delle cartelle FTP;
- `GET /api/ftp-convert` restituisce l'intero file pronto **più `previewData`, che include `costPrice`** — cioè i prezzi di acquisto Deldo in chiaro;
- `POST /api/config` permette a un anonimo di **riscrivere la configurazione** (markup, fornitori, blacklist);
- `POST /api/ftp-convert` permette di far girare conversioni a comando;
- `POST /api/history` e `DELETE /api/history?id=` permettono di inquinare o cancellare lo storico.

Le credenziali FTP non sono esposte (stanno nelle env var), quindi il danno diretto è contenuto, ma **il listino di acquisto e il margine sono l'informazione commercialmente più sensibile che hai**.

**Fix, in ordine di rapidità:**

1. **Immediato** — attiva la password del sito da Netlify (*Site configuration → Access & security → Password protection*), oppure aggiungi una Edge Function con Basic Auth come hai già fatto su `globalway-b2b`. Riusa lo stesso pattern.
2. In ogni caso aggiungi il controllo **dentro le functions**, non solo davanti al sito: la protezione del sito non copre necessariamente i path `/api/*`.
3. Togli `costPrice` da `previewData` (`ftp-convert.mjs:207`, `scheduled-convert.mjs:155`, `index.html:529`) o servilo solo dietro auth.

---

## 3. 🔴 "Modifica e sostituisci il tuo inventario", ogni giorno

### Cosa fa davvero

L'opzione che usi è il **Purge and Replace**. Non è un merge: azzera l'inventario e lo rimpiazza con il contenuto del file. La conferma più autorevole che ho trovato è di una dipendente Amazon sui forum ufficiali:

> *"The purge and replace option allows you to completely replace your current inventory with a new inventory file."*
> *"If you upload an empty file with purge and replace selected, all listings will be deleted."*
> — Angie_Amazon, [Seller Central UK](https://sellercentral.amazon.co.uk/seller-forums/discussions/t/667f5c34-9ade-4759-a54e-213b74e39bc1)

Quindi sì: **il tuo flusso funziona come previsto**. Le SKU assenti dal file del giorno vengono cancellate, e il tuo `add-delete='a'` su tutte le righe non le protegge — la cancellazione la decide *l'opzione di upload*, non il contenuto del campo.

### Il problema

Nello stesso post, la stessa dipendente Amazon elenca gli usi **da evitare**. Testualmente:

> *"Managing out-of-stock items (**use Price and Quantity file instead**)"*
> *"**Regular inventory synchronization (too risky)**"*
> *"FBA products"*

E gli usi appropriati: cancellare tutto per ripartire da zero, cambiare la convenzione di naming delle SKU. Cioè: **operazioni una tantum**.

Il tuo caso — sincronizzazione giornaliera dell'intero catalogo da CSV fornitore — è letteralmente la riga *"Regular inventory synchronization (too risky)"*.

*(Verifica: ho fatto controllare questa citazione da un secondo passaggio indipendente. Confermata. Va però detto che è un post su forum ufficiale, non una pagina di policy vincolante.)*

### I quattro rischi concreti, in ordine di gravità

**1. Limite settimanale di creazione offerte → errore 8571.** È il rischio che può bloccarti del tutto. La ASIN Creation Policy ufficiale:

> *"we limit the number of **listings (offers and ASINs)** that you can create in a given week until you establish a sales history with Amazon"*
> *"If you are an established seller and have created a high number of new listings, we reserve the right to **temporarily remove your ability to create new listings**."*
> — [ASIN Creation Policy (PDF ufficiale)](https://m.media-amazon.com/images/G/02/rainier/help/legal/ASIN_Creation_Policy_English_080920.pdf)

Il limite riguarda le **offers**, non solo gli ASIN nuovi. Un purge giornaliero di N SKU equivale a cancellare e ricreare N offerte al giorno, cioè **7×N creazioni a settimana**. Con 7.000 SKU sono ~49.000/settimana. L'errore corrispondente, dal PDF ufficiale sugli errori feed, è **8571 — "ASIN creation limit reached; account creating unusually high listings"**. Nessun numero di soglia è pubblicato: cresce con lo storico vendite.

**2. Delisting quotidiano durante l'elaborazione.** Amazon dichiara che i feed possono richiedere **fino a 8 ore** sotto carico. Nella finestra tra purge e replace le offerte sono cancellate ma non ancora ricreate: ogni giorno perdi visibilità e vendite per una frazione della giornata.

**3. Finestra 24h di riuso SKU, in collisione diretta con un ciclo a 24 ore.** Amazon indica **fino a 24 ore** perché una SKU cancellata torni riutilizzabile. È il meccanismo che genera l'errore *"Merchant SKU already exists. Merchant SKUs cannot be reused"*. Nel tuo caso è particolarmente rilevante perché **SKU = EAN**: le stesse SKU tornano identiche ogni giorno.

**4. Non è automatizzabile, per sempre.** Dal **31 luglio 2025** la Feeds API SP-API non supporta più i feed legacy flat file/XML per listings, pricing e inventory:

> *"Starting July 31, 2025, the Feeds API no longer supports legacy XML and flat file listing feeds, including pricing, inventory, relationships, and images."*
> — [Feeds API Best Practices](https://developer-docs.amazon/sp-api/docs/feeds-api-best-practices)

Non esiste modo di automatizzare il Purge and Replace via API. Se resti su questo flusso, il caricamento resta **manuale a mano, ogni giorno, per sempre**.

### Cosa NON perdi (per correttezza)

Va detto, perché è la buona notizia: **recensioni, BSR e storico vendite dell'ASIN non si resettano**. Appartengono all'ASIN, non alla tua offerta.

> *"Deleting and relisting the product with the exact SKU will not affect your existing FBA inventory (if any), ASIN sales history or reviews."*
> — [Seller Central EU](https://sellercentral-europe.amazon.com/seller-forums/discussions/t/0cffdf3ad116f62be780342c5e71c6b9)

Quello che perdi è la storia a livello di *tua offerta* (anzianità, metriche accumulate sulla SKU). E, durante la finestra di purge, l'eleggibilità alla Buy Box — perché un'offerta cancellata non è eleggibile per definizione. Che ci sia una penalizzazione *persistente* dopo la ricreazione è un'inferenza, non un dato documentato.

---

## 4. Il metodo corretto: architettura target

Amazon indica la strada nello stesso post: per gestire i non disponibili, *"use Price and Quantity file instead"*. Il principio è separare tre operazioni che oggi fai con un solo strumento:

| Operazione | Strumento | Frequenza |
|---|---|---|
| Prezzo + quantità su SKU esistenti | **File Prezzo e quantità** (upload "Aggiungi prodotti e modifica") | giornaliera, o più spesso |
| Non disponibili | stesso file, **`quantity=0`** | giornaliera |
| Prodotti nuovi | flat file categoria "Pneumatici e ruote", **solo il delta** | quando serve |
| Cancellazione definitiva | Inventory Loader `add-delete='x'` in **Aggiungi/Modifica** | mensile, mai in Purge |
| **Purge and Replace** | — | **mai come operazione ricorrente** |

### Perché `quantity=0` invece di cancellare

È la strada che Amazon raccomanda. La SKU passa in **"Inattivo (Non disponibile)"**: resta nel tuo catalogo, non è visibile ai clienti, e **si riattiva istantaneamente** rimettendo quantità > 0.

**Vantaggi**: nessuna attesa 24h, non consuma il budget settimanale di creazione offerte (elimina il rischio 8571), mantiene anzianità e mapping SKU↔ASIN, nessuna finestra di delisting, file più piccolo, reversibile.

**Svantaggio**: il catalogo accumula SKU inattive. Si gestisce con una pulizia mensile con `add-delete='x'` sulle SKU ferme da >60-90 giorni, in modalità Aggiungi/Modifica.

Non ho trovato documentazione ufficiale che indichi una durata massima per le offerte FBM a quantità zero. Circola nel settore la voce di rimozioni massive di listing dormienti da parte di Amazon: trattala come segnale, non come fatto.

### Modifiche concrete al tuo converter

Il cambiamento è più piccolo di quanto sembri. Oggi generi il file dei soli prodotti disponibili. Ti serve generare il file **di tutte le SKU che hai mai pubblicato**, con `quantity=0` per quelle assenti dal CSV di oggi.

1. **Persisti l'universo delle SKU pubblicate.** Nuovo blob, es. `published-skus`: `{ ean: { firstSeen, lastSeen, lastQty } }`. Lo aggiorni a ogni conversione.
2. **Genera il file come**: prodotti del CSV di oggi con la loro quantità, **più** ogni SKU in `published-skus` non presente oggi, con `quantity=0` e prezzo ultimo noto.
3. **Carica con "Aggiungi prodotti e modifica il tuo inventario"** — non più con "Modifica e sostituisci".
4. Aggiungi due colonne al file: `minimum-seller-allowed-price` e `maximum-seller-allowed-price` (vedi §7).
5. Opzionale ma consigliato: emetti anche un **file delta**, con solo le righe cambiate rispetto a ieri. Amazon lo raccomanda esplicitamente: *"Include only the products you are updating, not your entire inventory"*.

Una nota tecnica utile se in futuro passi a SP-API: aggiornando `fulfillment_availability` con operazione `replace` senza includere la `quantity`, **azzeri lo stock**. Per i soli aggiornamenti di quantità va usato `merge`. È un errore classico dei feed automatici.

### ⚠️ Due verifiche da fare tu, in account

Non le ho potute risolvere da fonti pubbliche — le pagine di help di Seller Central richiedono login.

- Un post ufficiale Amazon del 2025 sui tool per cataloghi grandi afferma che *"The Inventory Loader allows you to create and edit offers in bulk, handling **up to 600 SKUs at a time**"* ([fonte](https://sellercentral.amazon.com/seller-forums/discussions/t/6063783e-c87a-4a97-b9ae-68a9dd2e7bea)). Contrasta con la prassi consolidata di file da decine di migliaia di righe. **Se il limite fosse reale, i tuoi file potrebbero già essere elaborati solo in parte.** Controlla il report di elaborazione: *Catalogo → Aggiungi prodotti tramite upload → Verifica stato dell'upload* e confronta le righe accettate con quelle inviate.
- I valori esatti di `add-delete` (`a`/`d`/`x`): le fonti pubbliche si contraddicono su cosa faccia `d`. La fonte autoritativa è il foglio **"Data Definitions"** del template Inventory Loader scaricato dal tuo account.

---

## 5. 🔴 Il calendario 2026: cosa scade tra 12 giorni

Questa è la parte che ho verificato con più attenzione, perché le date sono imminenti e cambiano le priorità.

| Data | Cosa | Marketplace |
|---|---|---|
| 29 giu 2026 ✅ già in vigore | Handling time devono essere **accurati** per le SKU seller-fulfilled | US (rollout EU da verificare) |
| **15 lug 2026** ✅ già in vigore | **OTDR ≥ 90%** diventa un obbligo | **DE, FR, IT, ES** |
| **1 set 2026** ⏳ **12 giorni** | **Inizio enforcement OTDR**: le offerte non conformi possono essere **disattivate** | **DE, FR, IT, ES** |
| 1 set 2026 | **AHT si attiva d'ufficio** sulle SKU con handling time più lungo della performance reale di ≥1 giorno per oltre 30 giorni | EU |
| 30 set 2026 | **BHDR ≥ 90%** — consegne a clienti Amazon Business dentro il loro orario di apertura | UK + EU |
| 30 ott 2026 | Enforcement BHDR: offerte **nascoste ai clienti business** | UK + EU |

Fonti dirette:

> *"Starting July 15, 2026, sellers must maintain an on-time delivery rate of 90% or higher."*
> *"Starting September 1, 2026, if performance remains below 90%, non-compliant listings may be deactivated"*
> — [SP-API changelog: EU FBM Requirements](https://developer-docs.amazon/sp-api/changelog/sp-api-updates-eu-fbm-requirements-product-type-definitions-api-title-structure-external-fulfillment-api-shipment-status)

> *"Starting September 30, 2026, sellers must maintain a business hour delivery rate of 90% or higher."*
> — [stessa fonte](https://developer-docs.amazon/sp-api/changelog/sp-api-updates-eu-fbm-requirements-product-type-definitions-api-title-structure-external-fulfillment-api-shipment-status) + [annuncio forum ufficiale](https://sellercentral.amazon.com/seller-forums/discussions/t/57222b70-40df-4574-aa0f-9f815c197987)

### Il problema strutturale del dropshipping

La **protezione OTDR** (esenzione dalle penalità) si ottiene solo usando **tutti e tre** insieme: Shipping Settings Automation, Automated Handling Time e **Amazon Buy Shipping** con etichette "OTDR Protected". Stessa cosa per il BHDR.

**Se Deldo spedisce con il proprio corriere e il proprio contratto, tu non generi l'etichetta, quindi non hai accesso a Amazon Buy Shipping, quindi non ottieni la protezione.** Questo è il rischio strutturale principale del modello, e vale più di tutto il resto messo insieme.

Le opzioni realistiche:

- **(a) La migliore**: negoziare con Deldo che tu generi le etichette con **Amazon Buy Shipping** e loro le stampino e applichino. Risolve in un colpo BHDR, protezione OTDR, VTR *e* il requisito della Drop Shipping Policy sull'identificazione del mittente.
- (b) Attivare AHT + SSA e accettare di perdere il controllo sull'handling time.
- (c) Rinunciare al canale B2B (per gli pneumatici è un sacrificio grosso: gommisti, officine, flotte).
- (d) Portare le misure top-seller in FBA.

### E il tuo `leadtime: 4`

Il tuo `leadtime-to-ship` è **4 giorni** per tutto il catalogo. Questo ti espone da due lati opposti:

- **Verso l'alto**: 4 giorni di handling time + transito rende la promessa di consegna non competitiva. Nella categoria pneumatici il prodotto è commodity e l'ASIN è condiviso: la Buy Box è quasi tutto il fatturato, e il peso della velocità di consegna nell'algoritmo è cresciuto molto.
- **Verso il basso**: se Deldo in realtà spedisce in 1-2 giorni, dal 1 settembre 2026 Amazon rileva lo scostamento e ti attiva l'**AHT d'ufficio**, riducendo l'handling time a un valore che poi non controlli più. Il rischio è che in un periodo fortunato Amazon abbassi strutturalmente il valore, e poi ai primi rallentamenti di Deldo tu accumuli ritardi su LSR e OTDR.

> *"Your handling time is considered accurate when the actual handling time consistently matches your configured handling time for each SKU."*
> — [annuncio ufficiale handling time](https://sellercentral.amazon.com/seller-forums/discussions/t/da197a8b-e781-4530-99db-eb0ac8a5876d)

**Regola operativa**: non usare l'handling time come ammortizzatore del rischio fornitore. Dal 2026 Amazon punisce sia il valore gonfiato sia il ritardo reale. L'unico ammortizzatore legittimo è il **buffer di stock**.

Misura il tempo reale medio Deldo→cliente sugli ordini degli ultimi 30 giorni e allinea il `leadtime` a quello, non a un margine di sicurezza. Poi decidi **consapevolmente** se attivare l'AHT (che porta con sé la protezione LSR) o disattivare il toggle *prima* dell'auto-enrollment.

*(Onestà sulle fonti: l'irreversibilità dell'AHT dopo l'attivazione d'ufficio è riportata da un venditore che cita una notifica Amazon, non da documentazione ufficiale. Trattala come rischio probabile, non certo.)*

---

## 6. 🟠 Overselling: la matematica del Cancellation Rate

Le soglie ufficiali che ti riguardano:

| Metrica | Soglia | Finestra | Conseguenza |
|---|---|---|---|
| **Cancellation Rate** (pre-spedizione) | **< 2,5%** | **7 giorni** | disattivazione account |
| Order Defect Rate | < 1% | 60 giorni | disattivazione account |
| Late Shipment Rate | < 4% | 10/30 giorni | disattivazione account |
| Valid Tracking Rate | > 95% | 30 giorni | restrizioni vendita non-FBA |
| **OTDR** | **≥ 90%** (racc. 97%) | 14 giorni | **disattivazione offerte** |
| Invoice Defect Rate | < 5% | mensile | disattivazione account |

Fonti: [Order Performance Programme Policy EU (PDF)](https://m.media-amazon.com/images/G/39/MP/EU/redline_Order_Performance_programme_policy_tracked_GGJVNFDXQT8C3RA8.pdf), [Controllare lo stato dell'account IT (PDF)](https://m.media-amazon.com/images/G/29/rainier/help/legal/Monitor_your_account_health_Italian_120720.pdf)

Il punto critico: **un ordine che non puoi evadere perché Deldo ha esaurito lo stock è, per definizione, un annullamento del venditore** e colpisce la CR. Gli annullamenti richiesti dal cliente no.

Con soglia 2,5% su **7 giorni**:

| Ordini/settimana | Annullamenti che ti mettono fuori soglia |
|---|---|
| 40 | **1** |
| 100 | 2 |
| 200 | 5 |

La catena del danno con un feed a 24 ore:

```
Deldo esaurisce una misura alle 11:00
 → il tuo feed è di stamattina: quantity ancora 8
 → cliente ordina alle 15:00
 → ordine non evadibile → annullamento venditore → Cancellation Rate
```

E l'errore da non fare mai: confermare la spedizione per "salvare" la CR e poi non spedire. Trasforma un problema di CR (2,5% su 7 giorni) in LSR + VTR + ODR + reclami A-Z, cioè finestre più lunghe e conseguenze peggiori.

### Contromisure, in ordine di rapporto impatto/costo

1. **Alza la frequenza del feed.** La best practice Amazon è *"Upload one feed of the same type no more than once every 20 minutes"*. Aggiornare **una volta al giorno è una tua scelta, non un vincolo Amazon**. Passare a ogni 30-60 minuti è la contromisura a più alto impatto e costo quasi nullo — e diventa possibile solo dopo essere uscito dal Purge and Replace (§4), perché il purge non si può fare 24 volte al giorno.
2. **Quantity fisse basse invece dello stock reale.** Oggi pubblichi `stock` reale se la colonna c'è, altrimenti 8. Pubblicare una quantità **piccola e fissa** (2-4) limita l'esposizione massima a un singolo ciclo di aggiornamento: un ASIN pneumatico raramente vende più di 4 pezzi in un'ora.
3. **Buffer sullo stock fornitore.** Pubblica solo se `stock ≥ soglia` (es. 8), altrimenti `quantity=0`. La coda lunga di SKU quasi esaurite è dove nasce la maggior parte degli annullamenti.
4. **Verifica pre-conferma.** Prima di confermare la spedizione, controlla la disponibilità reale presso Deldo. Se manca: annulla subito (CR) invece di confermare e ritardare (LSR+ODR).
5. **Dashboard di rischio.** Monitora il **conteggio assoluto** di annullamenti negli ultimi 7 giorni contro il budget (ordini × 2,5%), non la percentuale. Al 60% del budget, sospendi le pubblicazioni sulle SKU a rischio. Questo lo puoi aggiungere al tab Storico che hai già.
6. **Secondo fornitore sulle misure top-seller**, così un out-of-stock Deldo non diventa un annullamento.
7. **Valuta se ti serve tutto il catalogo.** Un catalogo di 20.000 SKU con feed giornaliero genera più annullamenti di 3.000 SKU curate. Sugli pneumatici il ROI della coda lunga è negativo se costa la salute dell'account.

---

## 7. 🟠 Fair Pricing: il tuo markup è un rischio

La Fair Pricing Policy vieta, tra l'altro:

> *"setting a price on a product or service that is significantly higher than recent prices offered on or off Amazon"*
> *"selling multiple units of a product for more per unit than that of a single unit of the same product"*
> — [Amazon Marketplace Fair Pricing Policy (PDF)](https://m.media-amazon.com/images/G/02/rainier/help/legal/Amazon_Marketplace_Fair_Pricing_Policy_EN_161220.pdf)

Conseguenze: *"removing Buy Box, removing the offer, or in serious or repeated cases, suspending or terminating selling privileges"*.

Il tuo motore applica **+10% + 12 €** uniformemente su tutto il catalogo, senza nessun confronto con il mercato. Su un catalogo di migliaia di misure questo produrrà con certezza prezzi fuori mercato su una frazione delle SKU, tipicamente:

- misure a bassa rotazione dove il listino Deldo non è aggiornato;
- ASIN dove esistono venditori con accordi migliori;
- **ASIN mappati male** (misura/brand diversi) → il confronto Amazon avviene su un ASIN sbagliato. È la causa più comune di "high pricing error" apparentemente inspiegabile;
- **ASIN che sono set da 2 o 4 pneumatici** con il markup applicato per unità → viola direttamente il punto sui multipack.

Il flat fee di 12 € è particolarmente esposto sulle misure economiche: su un pneumatico da 35 € di costo diventa +34% effettivo.

Gli esiti possibili: perdita del Featured Offer; **soppressione dell'offerta** (su un ASIN dove sei l'unico venditore, il prodotto diventa non acquistabile); problemi di visualizzazione. Nessuna soglia percentuale è pubblicata da Amazon — chiunque citi un numero preciso lo sta inferendo.

**Contromisure implementabili nel tuo converter:**

| Contromisura | Come |
|---|---|
| **Cap superiore per SKU** | confronta il prezzo calcolato con il prezzo competitivo dell'ASIN (SP-API Product Pricing) e almeno un prezzo esterno; se supera la soglia (es. mediana ASIN × 1,10) → **non pubblicare** (`quantity=0`) invece di pubblicare fuori mercato |
| **Floor price** | costo Deldo + spedizione + commissione Amazon (15% su Auto e Moto) + IVA + margine minimo; sotto → `quantity=0`. Oggi il tuo tool non ha nessun floor: se Deldo sbaglia un prezzo in listino, tu vendi in perdita |
| **Scaglioni reali** | il `DEFAULT_TIERS` nel codice (18%+7 / 13%+10 / 12%+18 / 10%+30) è più sensato dell'unico scaglione 10%+12 attivo in produzione. Vale la pena riattivare una struttura a scaglioni |
| **min/max in Seller Central** | popola `minimum-seller-allowed-price` e `maximum-seller-allowed-price` nel file: Amazon li usa come riferimento e riducono i falsi positivi |
| **Automate Pricing** | delega la competitività intraday a Automate Pricing con i tuoi min/max. È gratuito col piano Professionale, aggiorna in <15 minuti, e — dato utile — *"Price changes made by Automate Pricing don't count toward the daily price update allotment"* ([sell.amazon.com](https://sell.amazon.com/tools/automate-pricing)). Il tuo file giornaliero si limiterebbe a costo e disponibilità |
| **Coerenza multipack** | se l'ASIN è un set, calcola il prezzo con prezzo unitario ≤ prezzo del singolo pezzo |
| **Monitoraggio** | leggi il dashboard *Gestisci avvisi sui prezzi* e usa i flag come input del motore |

---

## 8. 🟠 Conformità pneumatici — quello che il tool non copre

Questa parte non riguarda il codice ma vale più del codice, perché blocca le offerte a monte.

### GPSR (Reg. UE 2023/988, in vigore dal 13 dicembre 2024)

È il motivo più frequente di delisting su Amazon EU. L'art. 19 richiede che l'offerta online mostri: nome/marchio e indirizzo postale ed **elettronico** del **fabbricante**; se il fabbricante non è UE, gli stessi dati della **persona responsabile** UE; identificazione del prodotto con immagine; **avvertenze e informazioni di sicurezza in italiano**.

Amazon lo traccia in *Rendimento → Stato dell'account → **Gestisci la tua conformità***, con tre tipi di violazione: `GPSR: Safety Warnings and Information`, `GPSR: Responsible Person Contact Details`, `GPSR: Manufacturer Contact Details`.

Punti pratici emersi dal thread [RIVENDITORE PNEUMATICI – CONFORMITÀ NORMATIVE](https://sellercentral-europe.amazon.com/seller-forums/discussions/t/f623fd09-576e-463b-926b-6aa44c023f14):

- i grossisti spesso si rifiutano di fornire i dati della persona responsabile invocando la privacy: **obiezione non valida**, è un obbligo di legge;
- conviene contattare **direttamente i brand** (Michelin, Pirelli, Continental, Hankook…), non i distributori;
- come informazione di sicurezza si può caricare la **foto del fianco/etichetta** del pneumatico tramite il campo immagine `PS01`;
- Amazon ha chiarito che i dati della persona responsabile sono quelli **stampati sul prodotto**, quindi identici per tutti i rivenditori dello stesso ASIN, e che **la marcatura CE non è richiesta** per gli pneumatici.

Il caricamento in bulk si fa dalla sezione **"Sicurezza e conformità"** (evidenziata in viola) del template di categoria, espandendo le colonne nascoste col "+". Revisione fino a **14 giorni lavorativi**, e va **ripetuto per ogni store UE**. Guida ufficiale: [Bulk Upload Feature for GPSR information](https://sellercentral.amazon.co.uk/seller-forums/discussions/t/91cabfcb-3466-4f61-8a92-41804a1a05a3).

### Etichetta UE pneumatici (Reg. UE 2020/740)

L'**art. 6 riguarda direttamente te come distributore**: nella vendita via internet l'etichetta deve essere **vicino al prezzo**, visibile prima dell'acquisto, con scheda informativa accessibile.

Amazon **non ha campi flat-file dedicati** per le classi dell'etichetta. La Guida di stile Auto e Moto (il PDF che hai già nel progetto) prescrive di metterle **nel titolo**:

```
[marca] + [modello] + [larghezza/proporzioni/R cerchione] + [indice di carico]
 + [classificazione velocità] + [aderenza bagnato / efficienza / rumore] + [-tipo-]
```
Esempio ufficiale Amazon: `Pirelli Pzero Nero 195/45/R14 82 V C/B/68 -Pneumatico quattro stagioni-`

Massimo 80 caratteri, in italiano, senza simboli, e **mai marca/modello/anno del veicolo nel titolo** (confligge col Part Finder ed è vietato).

Da aggiungere: immagine dell'etichetta UE come `other-image-url1`; bullet con le classi + 3PMSF; e — non obbligatorio ma difensivo — il link alla scheda informativa EPREL, che ha un formato pubblico comodo: `https://eprel.ec.europa.eu/informationsheet/Fiche_{ID}_IT.pdf`.

**Non devi registrarti in EPREL**: l'obbligo è del fornitore/importatore. Devi solo ricevere da lui il numero di registrazione.

### EPR / PFU — il punto più sottovalutato

Amazon.it richiede un **ERN (EPR Registration Number) anche per la categoria pneumatici**, con scadenza comunicata al **30-31 marzo 2026 — già passata** ([annuncio ufficiale Seller Forums IT](https://sellercentral.amazon.it/seller-forums/discussions/t/f475cdb8-6ff7-4aba-a5de-b939fb1175a8)). Chi non lo fornisce viene iscritto d'ufficio al servizio **"EPR Pagamento a tuo nome"**, con addebito dei contributi *più* commissioni, oppure vede le offerte disattivate.

**Controlla subito** *Stato dell'account → Gestisci la tua conformità* e verifica se ci sono addebiti già in corso.

C'è poi una questione più grossa, sul piano nazionale. La definizione di immissione sul mercato ai fini PFU (DM 182/2019) comprende gli pneumatici *"introdotti sul territorio nazionale per la vendita **con qualsiasi modalità, incluse le tecniche di comunicazione a distanza**"*. In dropshipping da fornitori UE che spediscono direttamente al consumatore italiano, **MARIT SRL rischia di qualificarsi come importatore ai fini PFU**, con obbligo di:

1. iscrizione al **Registro MASE** produttori/importatori pneumatici (DM 147/2024);
2. adesione a un consorzio (Ecopneus, EcoTyre, Greentire…);
3. garantire il recupero del **95% in peso** degli pneumatici immessi l'anno precedente;
4. **contributo ambientale esposto separatamente in fattura**;
5. dichiarazione annuale entro il 31 ottobre.

Lo scenario alternativo è che il fornitore abbia già assolto il contributo e tu sia semplice distributore, ricevendolo in fattura e ribaltandolo.

**Questa è la questione su cui ti serve un parere del commercialista/legale, prioritario rispetto a tutto il resto di questa sezione.** Non è una cosa che posso risolvere io con una ricerca: dipende da come sono strutturati i tuoi acquisti da Deldo.

### Drop Shipping Policy — la parte fisica

Il modello "grossista europeo che spedisce al cliente finale" è la forma **consentita** di dropshipping. Il rischio è nella documentazione dentro il pacco:

> *"Identify yourself as the seller of your products on all packing slips, invoices, external packaging and other information"*
> *"Have an agreement with your supplier that they will identify you (and no one else) as a seller"*
> — [Drop Shipping Policy (PDF)](https://m.media-amazon.com/images/G/41/rainier/help/legal/Drop_Shipping_Policy_Update.pdf)

Serve un **accordo scritto con Deldo** (è il documento che Amazon chiede in caso di appello) e una verifica a campione dei pacchi: niente DDT, fatture, logo, nastro brandizzato o materiale promozionale Deldo, e lettera di vettura che non identifichi un altro venditore.

### Fatturazione

**Tu** emetti la fattura al cliente finale, non Deldo. E oltre all'obbligo fiscale italiano c'è quello contrattuale Amazon, più stringente: fattura valida per **ogni ordine Amazon Business entro 1 giorno lavorativo** dalla conferma di spedizione, con **Invoice Defect Rate < 5%** in vigore su IT dal 5 aprile 2021. Sugli pneumatici la quota di acquirenti business è alta, quindi l'IDR non è una metrica secondaria. Attivare il **VAT Calculation Service** (gratuito) automatizza le fatture e azzera il rischio IDR — ma **verifica col commercialista**: se Deldo spedisce da Belgio/Olanda/Germania, il paese di spedizione non è l'Italia e questo ha implicazioni IVA/OSS.

### E il ListingLoader nel progetto

Il file `ListingLoader_prefilled_with_10_search_results.xlsx.xlsm` che hai nel progetto è un template **solo-offerta** (`product_type = PRODUCT`, 158 colonne). **Non contiene nessun attributo pneumatico né nessun campo GPSR.** Per creare listing di pneumatici conformi ti serve il template categoria **"Pneumatici e ruote"** da *Catalogo → Aggiungi prodotti tramite caricamento → Scarica un foglio di calcolo*, e la fonte autoritativa per sapere cosa è obbligatorio è il suo foglio **"Definizioni dati"**.

---

## 9. Bug e criticità nel codice

Ordinati per gravità. I riferimenti sono a `netlify/functions/`.

### 🔴 Nessun fail-safe sull'anomalia del CSV

`scheduled-convert.mjs:270` salva `latest` **prima** di calcolare gli alert (riga 291). Se il CSV Deldo arriva troncato — 500 righe invece di 7.000 — il file viene generato con 500 prodotti, salvato come "ultimo pronto", e l'alert ti avvisa *dopo*. Se tu lo scarichi e lo carichi in Purge and Replace, **cancelli 6.500 offerte**.

E se il CSV manca del tutto, `runConversion` non solleva: aggiunge una stringa a `errors` e produce un file con **zero righe**. In Purge and Replace, un file vuoto cancella tutto l'inventario.

**Fix**: invertire l'ordine e mettere un guard-rail bloccante prima di scrivere `latest`. Qualcosa come: se `total_products` devia da `previous.total_products` oltre il ±15%, o se `errors.length > 0`, **non sovrascrivere `latest`** — salva il risultato in un blob `pending-review` e alza un alert. Meglio saltare un giorno che pubblicare un catalogo mutilato.

### 🔴 Il parser CSV non gestisce i campi quotati

`ftp-convert.mjs:30` e `scheduled-convert.mjs:27`: `line.split(delimiter)`.

Se una riga Deldo contiene un campo quotato con dentro il delimitatore — `"Pirelli; P Zero"` — **tutte le colonne successive slittano di uno**. Il risultato non è un errore: è un prezzo letto dalla colonna sbagliata, silenziosamente. Su un catalogo di migliaia di righe, anche poche righe corrotte diventano prezzi assurdi pubblicati su Amazon (§7).

Nessun BOM handling, per giunta: se il CSV arriva con BOM UTF-8, la prima intestazione diventa `﻿EAN` e `fCol` non la trova → "colonne mancanti" e zero prodotti da quel fornitore.

**Fix**: usa un parser vero (`csv-parse` è già compatibile con l'ambiente Netlify Functions), e strippa il BOM: `text.replace(/^﻿/, '')`.

### 🟠 Validazione EAN troppo permissiva

`ftp-convert.mjs:167`, `scheduled-convert.mjs:127`, `index.html:564`: `ean.length < 8`.

Passano stringhe non numeriche, EAN a 9-12 cifre, codici a 14 cifre. Con `product-id-type=4` Amazon si aspetta un EAN valido: il risultato sono errori feed **5501** ("Product ID non disponibile su Amazon") e **8541** ("dati in conflitto con un ASIN esistente"), cioè righe scartate silenziosamente.

Nota interessante: il tuo altro progetto, `globalway-b2b`, fa la cosa giusta — `skippedBadEan` per gli EAN che non sono 13 cifre numeriche. Qui la regola è più lasca. Vale la pena allinearle, e aggiungere anche la **verifica del check digit** EAN-13: è tre righe di codice e ti fa scartare a monte righe che Amazon rifiuterebbe.

### 🟠 Gli alert non letti vengono cancellati

`scheduled-convert.mjs:303`:

```js
const kept = existingAlerts.filter(a => a.created_at > cutoff && a.read);
```

Tiene solo gli alert **già letti** degli ultimi 7 giorni. Quindi: se ieri il job ha generato un alert "❌ Fornitore scomparso: Deldo" e tu non l'hai aperto, **domani quell'alert sparisce** quando il job ne genera di nuovi. È esattamente al contrario di come dovrebbe funzionare.

**Fix**: `filter(a => a.created_at > cutoff)`, oppure `filter(a => !a.read || a.created_at > cutoff)` se vuoi tenere gli unread a tempo indeterminato.

C'è anche un'asimmetria: se `alerts.length === 0` (riga 292) il blob non viene toccato affatto, quindi la pulizia dei vecchi non avviene mai nei giorni tranquilli.

### 🟠 Lo storico dal cron non viene mai potato

`history.mjs` limita a `MAX_RECORDS = 90`, ma la potatura gira **solo** nell'handler POST. Lo scheduled job scrive direttamente su `historyStore` (`scheduled-convert.mjs:288`) **senza** cleanup. Con una conversione al giorno per sei giorni a settimana, i blob crescono senza limite.

**Fix**: estrarre la logica di cleanup e chiamarla anche dallo scheduled job.

### 🟡 `quantity` fallback a 8 su tutto

Se `stockCol` non è configurata o la colonna manca, ogni SKU esce con `quantity = 8`. Con la matematica del §6, otto pezzi dichiarati su una misura che Deldo non ha è esattamente il profilo che genera annullamenti. Vedi le contromisure (quantity fissa bassa + buffer).

Collegato: `parseInt(row[stC]) || 0` (riga 176/133). Se la colonna Stock contiene `">10"` o `"disponibile"`, `parseInt` dà `NaN` → `0` → SKU disattivata per errore. Se contiene `"20+"` → 20. Vale la pena loggare i valori non numerici invece di assorbirli in silenzio.

### 🟡 `applyTiers` dipende dall'ordinamento

`tiers.find(t => t.upTo === null || price <= t.upTo)` assume che gli scaglioni siano in ordine crescente di `upTo`. Non c'è nulla che lo garantisca: se dall'UI si riordinano, i markup si applicano sullo scaglione sbagliato. Un `sort` difensivo prima del `find` costa nulla.

### 🟡 Encoding hardcoded UTF-8

`Buffer.concat(chunks).toString("utf-8")`. Se Deldo esporta in Latin-1 (frequente nei CSV di grossisti) i caratteri accentati si corrompono. Impatto basso perché usi solo EAN/prezzo/stock, ma se in futuro leggi brand o descrizioni diventa un problema.

### 🟡 Codice duplicato tra `ftp-convert.mjs` e `scheduled-convert.mjs`

`parseCSV`, `fCol`, `applyTiers`, `resolveDuplicate`, `fetchAllCSVsFromFTP` e `runConversion` sono duplicati, con un commento che lo dichiara intenzionale ("per autonomia dello scheduled job"). Il costo è che ogni fix va applicato due volte — tre con `index.html`, che ha una terza copia della stessa logica. Con le modifiche di §4 e §7 in arrivo, questo diventa una fonte concreta di divergenze. Vale la pena estrarre un `netlify/functions/_lib/converter.mjs` condiviso, come hai già fatto in `globalway-b2b`.

### 🟡 Il cron salta il cambio ora

`schedule: "15 8 * * 1-6"` è UTC, quindi 9:15 in ora solare e **10:15 in ora legale**. Il commento nel codice lo dice, quindi lo sai — ma se l'obiettivo è "prima dell'apertura", d'estate parti un'ora più tardi. Non c'è modo di gestire il DST in cron: se conta, la soluzione è schedulare alle 6:15 UTC e accettare 7:15/8:15.

---

## 10. Roadmap proposta

**Oggi**

- [ ] Metti la password sul sito Netlify e/o Basic Auth sulle functions. Togli `costPrice` da `previewData`.
- [ ] Controlla *Stato dell'account → Gestisci la tua conformità*: violazioni GPSR aperte, stato EPR pneumatici, eventuali addebiti "Pagamento a tuo nome" già in corso.
- [ ] Controlla il report di elaborazione dell'ultimo upload: quante righe accettate su quante inviate (il possibile limite di 600 SKU, §4).

**Questa settimana — prima del 1 settembre**

- [ ] Guarda il tuo OTDR attuale in *Rendimento → Stato dell'account*. Se è sotto il 90%, hai 12 giorni prima che le offerte comincino a essere disattivate.
- [ ] Misura il tempo reale Deldo→cliente sugli ordini degli ultimi 30 giorni e allinea il `leadtime` a quel valore.
- [ ] Apri la conversazione con Deldo su **Amazon Buy Shipping**: tu generi l'etichetta, loro la applicano. È l'unica strada che risolve OTDR, BHDR, VTR e Drop Shipping Policy insieme.
- [ ] Aggiungi il guard-rail bloccante sull'anomalia del CSV (§9). È mezz'ora di lavoro e ti protegge dallo scenario peggiore.

**Prossime 2-3 settimane**

- [ ] Migra da Purge and Replace al modello `quantity=0` + "Aggiungi prodotti e modifica": blob `published-skus`, generazione del file completo con gli zeri, opzionalmente file delta.
- [ ] Sostituisci il parser CSV, irrigidisci la validazione EAN, sistema il bug degli alert non letti.
- [ ] Aggiungi floor price e cap superiore al motore di pricing; popola `minimum-seller-allowed-price` / `maximum-seller-allowed-price` nel file.
- [ ] Alza la frequenza del feed (30-60 minuti) — possibile solo dopo la migrazione.

**Poi**

- [ ] Valuta Automate Pricing con i tuoi min/max, così il file giornaliero gestisce solo costo e disponibilità.
- [ ] Parere legale/fiscale sulla qualifica PFU di MARIT SRL (importatore vs distributore) e sull'impostazione VCS con spedizione da altro Stato UE.
- [ ] Raccolta dati GPSR dai brand + caricamento in bulk; template categoria "Pneumatici e ruote"; titoli con la formula ufficiale; immagine dell'etichetta UE.
- [ ] Estrai la libreria condivisa per eliminare la tripla duplicazione della logica di conversione.
- [ ] Dashboard di rischio CR nel tab Storico: annullamenti assoluti negli ultimi 7 giorni vs budget.

---

## 11. Note sull'affidabilità di questa ricerca

Da tenere presente quando agisci su queste informazioni:

- **Le pagine di help di Seller Central richiedono login** e non sono leggibili da fuori. Tutto quello che riporto viene da: PDF legali ufficiali su `m.media-amazon.com`, changelog e docs `developer-docs.amazon`, annunci ufficiali e post di dipendenti Amazon sui Seller Forums, e in subordine fonti terze — sempre segnalate come tali.
- Le cinque affermazioni più consequenziali (Purge & Replace "too risky", OTDR 90% su IT, BHDR, handling time, deprecazione feed legacy) le ho fatte **verificare da un secondo passaggio indipendente**. Tre confermate, due parzialmente: l'attivazione d'ufficio dell'AHT decorre dal **1 settembre 2026**, non dal 29 giugno, e la sua **irreversibilità non è documentata ufficialmente** (viene da un venditore che cita una notifica). Il nome `POST_FLAT_FILE_INVLOADER_DATA` non compare nominalmente nelle docs: che sia deprecato è un'inferenza dalla categoria.
- **Non ho trovato l'annuncio OTDR in italiano** su Seller Central IT indicizzato pubblicamente. La fonte citabile è il changelog SP-API. Vale la pena cercare la notifica nelle news del tuo account.
- Le due cose che **solo tu puoi verificare** perché richiedono l'account: il limite di 600 SKU per Inventory Loader, e i valori esatti di `add-delete` dal foglio "Data Definitions" del template.
- Sulla qualifica PFU: quello che riporto è il quadro normativo e la lettura che ne dà il settore. **Non è un parere legale** e la risposta dipende dalla struttura contrattuale dei tuoi acquisti.

---

## Fonti principali

**Amazon — PDF ufficiali (accessibili senza login)**
- [ASIN Creation Policy](https://m.media-amazon.com/images/G/02/rainier/help/legal/ASIN_Creation_Policy_English_080920.pdf)
- [How to resolve common feed errors](https://m.media-amazon.com/images/G/01/AmazonServices/How_to_Solve_Common_Feed_Errors_3_30._CB1522402454_.pdf)
- [Drop Shipping Policy](https://m.media-amazon.com/images/G/41/rainier/help/legal/Drop_Shipping_Policy_Update.pdf)
- [Amazon Marketplace Fair Pricing Policy](https://m.media-amazon.com/images/G/02/rainier/help/legal/Amazon_Marketplace_Fair_Pricing_Policy_EN_161220.pdf)
- [Order Performance Programme Policy (EU)](https://m.media-amazon.com/images/G/39/MP/EU/redline_Order_Performance_programme_policy_tracked_GGJVNFDXQT8C3RA8.pdf)
- [Controllare lo stato dell'account (IT)](https://m.media-amazon.com/images/G/29/rainier/help/legal/Monitor_your_account_health_Italian_120720.pdf)
- [Account Health Rating program policy and FAQ](https://m.media-amazon.com/images/G/09/rainier/help/Account_Health_Rating_program_policy_and_FAQ_EN.pdf)
- [Amazon Business Invoicing Policy](https://m.media-amazon.com/images/G/02/rainier/help/legal/Amazon_Business_Invoicing_Policy_EN_161220.pdf)
- [Guida di stile Auto e Moto (IT)](https://images-na.ssl-images-amazon.com/images/G/29/rainier/help/style/AutomotiveStyleGuideIT._V508671174_.pdf)
- [Restrizioni su categorie, prodotti e contenuti (IT)](https://m.media-amazon.com/images/G/29/rainier/help/legal/Category_Product_and_Content_Restrictions_Italian_120720.pdf)

**Amazon — documentazione tecnica**
- [Feeds API Best Practices](https://developer-docs.amazon/sp-api/docs/feeds-api-best-practices) · [Rate Limits](https://developer-docs.amazon.com/sp-api/docs/feeds-api-rate-limits) · [FAQ](https://developer-docs.amazon.com/sp-api/docs/feeds-api-faq)
- [Listings Feed Type Values](https://developer-docs.amazon/sp-api/docs/listings-feed-type-values) · [Listings APIs FAQ](https://developer-docs.amazon.com/sp-api/docs/listings-apis-faq)
- [Building Listings Management Workflows](https://developer-docs.amazon/sp-api/docs/building-listings-management-workflows-guide)
- [Changelog: EU FBM Requirements (OTDR/BHDR)](https://developer-docs.amazon/sp-api/changelog/sp-api-updates-eu-fbm-requirements-product-type-definitions-api-title-structure-external-fulfillment-api-shipment-status)
- [Managing Handling Time (SP-API samples #124)](https://github.com/amzn/selling-partner-api-samples/discussions/124)
- [Automate Pricing](https://sell.amazon.com/tools/automate-pricing)

**Amazon — Seller Forums (annunci ufficiali e post di dipendenti)**
- [Purge & Replace — Angie_Amazon](https://sellercentral.amazon.co.uk/seller-forums/discussions/t/667f5c34-9ade-4759-a54e-213b74e39bc1) ⭐
- [New handling time requirements for seller-fulfilled products](https://sellercentral.amazon.com/seller-forums/discussions/t/da197a8b-e781-4530-99db-eb0ac8a5876d)
- [New business hour delivery requirement (BHDR)](https://sellercentral.amazon.com/seller-forums/discussions/t/57222b70-40df-4574-aa0f-9f815c197987)
- [Bulk Listing Updates: Efficiency Tools for Large Catalogs](https://sellercentral.amazon.com/seller-forums/discussions/t/6063783e-c87a-4a97-b9ae-68a9dd2e7bea)
- [Inventory file templates (part 3): Inventory Loader](https://sellercentral-europe.amazon.com/seller-forums/discussions/t/8b5be355-c630-4276-96c3-463769d6934b)
- [Inventory file templates (part 4)](https://sellercentral.amazon.com/seller-forums/discussions/t/cb846aeb-93f0-440d-9b0b-a33af69444db)
- [Amazon drop shipping policies: the dos and don'ts](https://sellercentral.amazon.com/seller-forums/discussions/t/4442563f-f6e8-4522-b990-afaa60a1600d)
- [ASIN Creation Limit, Error 8571 — Glenn_Amazon](https://sellercentral.amazon.com/seller-forums/discussions/t/619a52f5-8301-475f-b17c-54b36515dd5e)
- [Come gestire il file inventario su Amazon (IT)](https://sellercentral.amazon.it/seller-forums/discussions/t/d89bb981-ad6c-4658-915c-f8b9dc774e56)
- [Mantieni la conformità ai nuovi requisiti EPR in Italia (IT)](https://sellercentral.amazon.it/seller-forums/discussions/t/f475cdb8-6ff7-4aba-a5de-b939fb1175a8)
- [RIVENDITORE PNEUMATICI — CONFORMITÀ NORMATIVE](https://sellercentral-europe.amazon.com/seller-forums/discussions/t/f623fd09-576e-463b-926b-6aa44c023f14)
- [Bulk Upload Feature for GPSR information](https://sellercentral.amazon.co.uk/seller-forums/discussions/t/91cabfcb-3466-4f61-8a92-41804a1a05a3)
- [Deleted SKUs unable to reinstate — Manny_Amazon](https://sellercentral.amazon.fr/seller-forums/discussions/t/69c9c27c-4534-42be-9928-978db058cefd)

**Normativa**
- [Reg. UE 2023/988 (GPSR)](https://eur-lex.europa.eu/legal-content/IT/TXT/?uri=CELEX%3A32023R0988)
- [Reg. UE 2020/740 (etichettatura pneumatici)](https://eur-lex.europa.eu/legal-content/IT/TXT/PDF/?uri=CELEX%3A32020R0740)
- [Commissione UE — Tyres / EPREL](https://energy-efficient-products.ec.europa.eu/product-list/tyres_en) · [EPREL per fornitori](https://energy-efficient-products.ec.europa.eu/suppliers_en)
- [RENAP — Registro Pneumatici](https://www.renap.gov.it/it/registro-pneumatici) · [Ecopneus — Normativa](https://www.ecopneus.it/normativa/)
- [Art. 49 Codice del Consumo](https://www.brocardi.it/codice-del-consumo/parte-iii/titolo-iii/capo-i/sezione-ii/art49.html)
- [sell.amazon.it — Conformità EPR](https://sell.amazon.it/conformita-responsabilita-estesa-del-produttore) · [Vendere ricambi auto](https://sell.amazon.it/imparare/vendita-di-componenti-automobile)
