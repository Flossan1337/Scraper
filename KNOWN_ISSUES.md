# Known Issues

Senast uppdaterad: 2026-09-04

## 13. `fetch_nelly_trends_v3.py` loopade oändligt i chunk-slingan — LÖST 2026-09-04, men värdena har skiftat

`fetch_country_monthly()` avancerade fönstret med
`chunk_start = chunk_end - relativedelta(months=OVERLAP_MONTHS)`. Så fort `chunk_end` klampades
till `end_date` (dagens datum) hamnade `chunk_start` på `end_date - 2 månader`, nästa varv
klampade `chunk_end` till `end_date` igen, och samma fönster hämtades **för alltid**. Slingan kunde
bara avbrytas av att `_fetch_single_chunk()` gav upp efter `MAX_RETRIES` 429:or och kastade vidare
— vilket landade i `except`-grenen i `main()` och gav "CRITICAL FAILURE" utan data. Buggen fanns
alltså i originalkoden också; den maskerades av att körningen ändå dog på 429 långt innan, så det
såg ut som ett rate-limit-problem.

`build_chunks()` i `fetch_rugvista_trends_v2.py` har rätt vakt sedan tidigare
(`if cur <= chunks[-1][0]: break`). Samma vakt är nu tillagd i nelly. Körningen tar 10 s och gör
44 chunks.

**Konsekvens för datan:** nelly-serierna är hopsydda chunk för chunk, så när kedjan av
skalfaktorer ändras ändras nivån på hela serien. Nya utdatan mot den tidigare committade xlsx:en,
125 överlappande månader:

| serie | korrelation | max abs diff | medel abs diff |
|---|---|---|---|
| Nelly_SE | 0.9937 | 32.2 | 15.20 |
| Nelly_NO | 0.9886 | 23.1 | 12.49 |
| Nelly_DK | 0.9948 | 11.9 | 3.13 |
| Nelly_FI | 0.9948 | 24.6 | 11.39 |

Formen är alltså intakt (korrelation 0,99+) men **nivån har flyttat sig 10–15 punkter i snitt**.
Jämför man nelly-tal före och efter 2026-09-04 jämför man därför inte samma skala. Det är ett
argument till för att gå över till ett enda anrop per land (se punkt 11) — då kommer värdena från
Googles egen normalisering i stället för vår skalkedja, och blir stabila mellan körningar.
Utdatan börjar dessutom nu på 2015-12-31 i stället för 2016-01-31 (en extra månad).

## 12. Plejd-trends för små länder (ES, CH, IS, DK) är brus, inte en tidsserie

Upptäckt när utdatan från `fetch_plejd_trends.py` diffades mot den committade historiken
2026-09-04 (SE och NO rörde sig knappt — max 5 respektive 7 punkter — vilket bekräftar att själva
hämtningen är likvärdig med den gamla). De små länderna rörde sig i stället upp till 100 punkter:

| serie | andel månader = 0 | 100-toppen låg | flyttade toppen |
|---|---|---|---|
| Plejd_SE | 2 % | 2025-11 | nej |
| Plejd_NO | 41 % | 2025-11 | nej |
| Plejd_FI | 50 % | 2025-10 | nej |
| Plejd_NL | 64 % | 2026-03 → 2026-07 | ja |
| Plejd_DE | 80 % | 2025-11 → 2026-07 | ja |
| Plejd_IS | 91 % | 2024-11 → 2026-01 | ja |
| Plejd_DK | 91 % | 2024-11 → 2024-10 | ja |
| Plejd_ES | 95 % | 2025-10 → 2019-02 | ja |
| Plejd_CH | 98 % | 2019-03 → 2023-06 | ja |

Google normaliserar varje serie 0–100 mot sin egen toppmånad. När 90–98 % av månaderna är noll
avgörs toppen av var Googles sampling råkar hitta någon sökning alls, och den landar på olika
månad varje körning — varpå hela serien skalas om. Det är inte ett fel i hämtningen och går inte
att fixa där; det är vad Trends returnerar för för lite sökvolym.

**Praktiskt:** `Plejd_SE`, `Plejd_NO` och `Plejd_FI` går att följa över tid. `Plejd_ES`,
`Plejd_CH`, `Plejd_IS` och `Plejd_DK` ska inte användas för nivå- eller trendjämförelser mellan
körningar, och `Plejd_NL`/`Plejd_DE` bör tolkas försiktigt. Samma resonemang gäller alla
Trends-serier med hög nollandel, inte bara Plejd.

## 11. ~~Google Trends-scripten (8 st) troligen trasiga p.g.a. hårdare rate-limiting~~ — ROTORSAK HITTAD och FIXAD 2026-09-04

Alla 8 `fetch_*_trends.py`-script migrerades till `google_trends_monthly` (schema +
engångsbackfill från befintlig xlsx-historik, se `scripts/tools/load_google_trends_history.py`),
och DB-skrivning kopplades in i varje scripts kod (`core/trends.py`). **Ingen av de 8 kördes
live under migreringen** — beslutat medvetet, eftersom Google Trends/pytrends är extremt
känsligt för 429-blockering och en full körning kan ta flera minuter per script.

Misstanken stämde: alla 8 var trasiga. Men **inte** för att Google skärpt sig mot volym —
rotorsaken var pytrends självt, och backoff-logiken gjorde saken sämre, inte bättre.

**Mätt 2026-09-04, inte gissat:**

* Google svarar **429 på den FÖRSTA requesten i en ny HTTP-session** och sätter `NID`-cookien
  på just det 429-svaret. Varje efterföljande request i **samma** session går igenom.
* 40 explore-anrop i rad utan någon fördröjning alls: **39 st 200, 1 st 429** — och 429:an var
  request nr 0. Hela svängen tog 2,1 sekunder.
* Headers spelar ingen roll. En naken session utan ens `User-Agent` beter sig identiskt.
  Testat: bara UA, UA+Accept-Language, UA+Referer, full webbläsarupp­sättning — alla 0/6 när
  sessionen är ny per anrop, alla OK när sessionen återanvänds.

pytrends gör precis tvärtom: `mk_client()` byggde en ny `TrendReq` **per land och per retry**,
så varje request var "en sessions första request" och fick därför 429. Scripten tolkade det som
"vi är rate-limitade", sov 60 s+ med exponentiell backoff, och byggde sedan **ännu en** färsk
klient — vilket garanterade nästa 429. Backoffen orsakade felet, den överlevde det inte. Det är
därför körningarna tog evigheter och ändå slutade utan data.

**Ovanpå det finns en riktig per-IP-kvot — men den tar betalt för nya sessioner, inte för
requests.** Under felsökningen låste sig IP:n i ~12 minuter efter ~100 anrop, men de anropen var
utspridda över massor av nybyggda sessioner som var och en brände en handskaknings-429. Till
jämförelse: alla 8 script i rad, ~156 requests på en enda uppvärmd session, tog 27 sekunder och
slog inte i något tak alls. En normal full körning ligger alltså inte i närheten av gränsen. Det
som kostar är att bygga om sessioner i en loop — precis det gamla pytrends-koden gjorde.

**Fix:** `pytrends` är borta ur `requirements.txt`. `core/trends.py` har en egen
`TrendReq` (samma API-yta: `build_payload()` + `interest_over_time()`) ovanpå `requests`, med

* **en** uppvärmd session per process, delad av alla anrop,
* `NID`-cookien cachad på disk i `.cache/` så omkörningar slipper handskakningens 429,
* korta retrier (1 s, 3 s) i stället för minutlång backoff,
* en brytare: efter 3 helt misslyckade requests i rad kastas `TrendsQuotaError` direkt, så ett
  kvotstopp avbryter körningen på sekunder i stället för att mala i minuter,
* `fetch_series()` som cachar varje hämtad serie på disk, nycklad på exakt request — en körning
  som dör halvvägs återupptas vid omkörning i stället för att börja om.

Scripten skriver dessutom bara xlsx/databas när **alla** serier kom hem, annars tappas kolumner
tyst ur xlsx:en.

### Kvarstår: chunkningen i `fetch_nelly_trends_v3.py` och `fetch_rugvista_trends_v2.py`

Båda delar upp 2016→idag i småfönster med överlapp och syr ihop resultatet. Kommentarerna i
dem säger att en enda stor förfrågan "triggar rate-limiting nästan alltid" — **det är fel**,
Google returnerar hela 2016→idag i ett anrop (verifierat för Plejd: 129 månadspunkter).

Chunkningen kostar därför onödigt mycket kvot — nelly gör ~13 anrop per land, ~52 totalt,
alltså ~104 HTTP-requests per körning, och är det script som mest sannolikt slår i per-IP-kvoten
på egen hand. Den behålls ändå tills någon diffar ett enkelanrop mot hela xlsx-historiken:
byte från hopsydd serie till Googles egen normalisering **ändrar varenda siffra i utdatan**, och
enligt CLAUDE.md får en xlsx inte peka om utan att diffen är gjord och förstådd.

## 9. ~~`extract_state_history.py` dubbelkodade å/ä/ö på Windows — smittade backfillad historik~~ — LÖST 2026-08-05

`git()`-hjälparen i `scripts/tools/extract_state_history.py` körde
`subprocess.run(["git", "show", ...], capture_output=True, text=True)` utan att ange
`encoding="utf-8"`. Med `text=True` avkodades stdout med `locale.getpreferredencoding()`, som på
den här Windows-maskinen är `cp1252` — inte UTF-8. Varje historisk `data/*_state.json`-fil som
innehöll UTF-8-bytes för å/ä/ö (t.ex. "ä" = `0xC3 0xA4`) blev därför feltolkad tecken för
tecken (`0xC3`→"Ã", `0xA4`→"¤") innan den skrevs till `raw/<namn>/<datum>.json` med
`encoding="utf-8"` — vilket permanent dubbelkodade strängen i cache-filen.

**Ursprungligen upptäckt i `nelly_daily_summary.by_category`:** alla 131 datum som lästs in via
`load_nelly_inventory_history.py` (2026-03-17 till 2026-07-25, via `raw/nelly_inventory_state/`)
hade dubbelkodade kategorinycklar (t.ex. `"Kläder>Jeans"` blev `"KlÃ¤der>Jeans"`), medan alla
datum från 2026-07-27 och framåt — skrivna direkt av `track_nelly_inventory.py` live, som aldrig
gick via `raw/` — hade korrekt kodning. Samma logiska kategori pivoterades därför till två olika
kolumner i Power Query beroende på datum ("By Category"-fliken i `DATA DASHBOARD.xlsx`).

**Fullständig genomgång visade att bugg smittat fyra tabeller, inte bara Nelly** — inklusive
punkt 7 nedan, som tidigare (felaktigt) bedömdes vara äkta källdatakorruption:
- `nelly_daily_summary.by_category`/`by_brand` — 131 datum.
- `rugvista_variant_snapshot.variant_name`/`.parent_name` — **49 083 av 89 098 rader** (55 %),
  det klart största enskilda fyndet.
- `ahlsell_led_panel_article.product_name` — 48 rader. Detta var **samma bugg som punkt 7**,
  inte äkta källdatakorruption som ursprungligen antaget — den "bekräftelsen" gjordes mot samma
  (då ännu opatchade) `raw/`-cache som orsakade felet i första hand.
- `ahlsell_warehouse.name`/`.city`/`.address` — 67/48/75 av 110 rader.

Alla fyra tabellers text är nu korrekt (0 dubbelkodade rader kvar, verifierat med en genomgång
av samtliga 77 text-kolumner + 7 JSONB-kolumner i schemat).

**Fix:** lade till `encoding="utf-8"` på `subprocess.run`-anropet i `git()` (och läser bytes
direkt + avkodar explicit, istället för att lita på `text=True`). Körde om
`extract_state_history.py` för att skriva om hela `raw/` (11 mappar). Skrev tre engångsscript
för att ladda om de fyra påverkade tabellerna med `upsert_rows` (skriver över, till skillnad
från den ursprungliga `ON CONFLICT DO NOTHING`-insatsen som bara skulle hoppa över redan
existerande rader): `scripts/tools/fix_rugvista_encoding.py` (radvis `UPDATE` matchat på
`(snapshot_date, product_id)` snarare än `(captured_at, product_id)`, eftersom en historisk
gap-fill-rad kunde ha en `captured_at` några sekunder ifrån vad samma datums råfil anger idag)
och `scripts/tools/fix_ahlsell_encoding.py` (LED-panel + warehouse, en batch vardera). Nelly
fixades via en direkt omkörning av samma upsert-logik som `load_nelly_inventory_history.py`
redan hade, fast med `upsert_rows` istället för `insert_rows`.

## 10. `nelly_daily_summary.restocks`/`.returns` var exakt 0 för 2026-08-02 och 2026-08-03

Båda dagarna hade 0 restocks och 0 returer, till skillnad från grannedagarna (t.ex. 38–170
restocks, 806–2832 returer). Manuell omräkning av lagerdeltat direkt från de git-committade
`data/nelly_inventory_state.json`-snapshottarna (`last_snapshot`-fälten för 08-01→08-02 och
08-02→08-03) visar **exakt noll** produkter med lagerökning av ~33 000 gemensamma
produkt-nycklar, båda dagarna — datan i databasen är alltså en korrekt spegling av vad scriptet
beräknade, inte ett fel i migreringen eller skrivvägen. Grannedagen 07-31→08-01 hade 2 870
ökningar (matchar 38+2832 exakt), så mönstret bröts tvärt just dessa två dagar.

Inte fastställt om detta är en verklig (om ovanlig) lugn period i Nellys faktiska lagerrörelser
eller ett tecken på att något i deras Elevate-API tystnat/ändrats för just lagerökningar. **Bevaka
framåt:** om mönstret (0 restocks + 0 returer) fortsätter flera dagar till är det sannolikt ett
uppströms-problem värt att undersöka i `track_nelly_inventory.py`s datakälla, inte en engångshändelse.

## 7. ~~`ahlsell_led_panel_article.product_name` hade 48 rader med "äkta" teckenkodningsfel~~ — OMVÄRDERAT och LÖST 2026-08-05

**Denna bedömning var felaktig.** Ursprungligen (2026-07-29) bekräftades detta mot
`raw/ahlsell_led_panel_state/2026-07-07.json` t.o.m. `2026-07-16.json` och drogs slutsatsen att
källan själv var dubbelkodad — men den "bekräftelsen" kontrollerade samma `raw/`-cache som punkt
9 senare visade var korrupt av en bugg i `extract_state_history.py` (mis-avkodning som cp1252 på
Windows), inte den faktiska data som scriptet skrev. Med den bytt (se punkt 9) innehåller
`raw/ahlsell_led_panel_state/*.json` **noll** dubbelkodade tecken — samma mönster som Rugvista.
Alltså: inte en genuin, oreparerbar historisk skada, utan samma extraktions-bugg som allt annat i
punkt 9. Fixad genom omladdning via `scripts/tools/fix_ahlsell_encoding.py`.

## 8. ~~`ahlsell_article` hade 39 rader med teckenkodningsfel som ändrade kategorisering~~ — LÖST 2026-07-29

Till skillnad från punkt 7 ovan var råkällan här alltid korrekt kodad (verifierat i samtliga
`raw/ahlsell_plejd_state/*.json`, från första filen 2026-05-27 till senaste) — felet uppstod i
den ursprungliga engångsladdningen till databasen (`scripts/tools/load_ahlsell_history.py`,
körd 2026-07-28), inte i källdatan. En felfri nyskrivning med samma sträng lyckas (verifierat
med en direkt round-trip-test), så den exakta orsaken till den ursprungliga körningens fel är
inte fastställd — men eftersom ett färskt försök inte reproducerar felet bedöms det som en
engångsanomali snarare än ett kvarstående systemfel.

**Konsekvens:** 9 artiklar med "Väggarmatur"-namn (`7706311/312/313`, `7706614/615/616`,
`7706621/622/623`) kategoriserades som "Övrigt" istället för "Armaturer", eftersom
`categorize()`s nyckelordsmatchning på "väggarmatur" inte kunde matcha den dubbelkodade texten.
Detta upptäcktes när `ahlsell_plejd_sales_v` validerades mot `data/ahlsell_plejd_inventory.xlsx`
och gav ett konsekvent 26-enheters-fel mellan just de två kategorierna, varje dag.

**Fix:** laddade om `ahlsell_article` med `core.db.upsert_rows` (istället för den ursprungliga
`ON CONFLICT DO NOTHING`-insatsen) från en färsk sammanslagning av alla `raw/`-filer plus
aktuell `data/ahlsell_plejd_state.json`. Validerat efteråt: `ahlsell_plejd_sales_v` matchar
xlsx exakt över hela historiken (315 av 315 observationer, både sales-out och sales-in).

## 6. ~~`track_nelly_aov.py` hittade inga priser sedan 2026-07-20~~ — LÖST 2026-08-05

Scriptet var fortfarande schemalagt och körde felfritt (`continue-on-error` dolde det), men
`fetch_prices()` hittade 0 priser på samtliga 10 sidor på `nelly.com/se/topplistan/` sedan
2026-07-20. Scriptet avslutar tyst utan att skriva någon rad (varken Excel eller databas) när
`all_prices` är tom, så `data/nelly_aov.xlsx` och `nelly_aov`-tabellen saknade nya rader i över
två veckor utan någon synlig varning.

**Grundorsak:** Nelly bytte sin frontend/design-system. Prisspannet satt tidigare i
`<span class="text-sm text-darkGrey">` (med separat `<ins>`-element för rabatterat pris) —
klassen `text-sm` finns inte längre på prisspannet, som nu heter
`text-subhead leading-none text-darkGrey`. Bekräftat live genom att rendera sidan med Selenium
(precis som scriptet redan gjorde) och inspektera den faktiska DOM-strukturen: 32 produkter per
sida, alla med exakt samma nya klasskombination.

Samtidigt bekräftades att den gamla rabatt/ordinarie-pris-uppdelningen (`<ins>`/`<del>`) inte
längre existerar på listningssidan — varje produktkort visar numera bara **ett** pris (redan det
effektiva/säljande priset), ingen separat överstruken originalprissättning syns längre i rutnätet.
Sidparametern `?page=N` fungerar fortfarande som riktig sidnavigering (verifierat: sida 1 och 2
gav olika produkter), så resten av scriptets struktur var oförändrad och korrekt.

**Fix:** bytte selektor till `span.text-subhead.leading-none.text-darkGrey` och tog bort hela
den nu onödiga rabatt-vs-ordinarie-uppdelningslogiken i `fetch_prices()` (färre rader, ingen
dold sårbarhet mot en förändring i hur `<ins>` en gång användes). Körd live: alla 10 sidor gav
32 priser vardera (320 totalt), median 349 kr / snitt 423,41 kr för 2026-08-05 — ett rimligt
mönster (snitt > median, högerskev fördelning) jämfört med tidigare frisk historik
(t.ex. juni: median ~209–249, snitt ~252–288 — nivåskillnaden är trolig säsongsvariation i
"topplistan"-sortimentet, inte ett tecken på fortsatt fel).

## 1. ~~`rugvista_daily_sales_v` avviker från `data/rugvista_daily_sales.xlsx`~~ — LÖST 2026-07-29

Vyn ([sql/views/rugvista_daily_sales.sql](sql/views/rugvista_daily_sales.sql)) avvek från
`data/rugvista_daily_sales.xlsx` på 54 av 283 dagar, alltid åt samma håll (vyn räknade högre).

**Grundorsak bekräftad:** vyns 48-timmarsspärr (menad att hoppa över produkter som
tillfälligt lämnat och återvänt till topplistan, se punkt 5 nedan för samma felklass)
jämförde absolut klocktid, inte kalenderdatum. Rugvista-jobbets exakta körtid varierar
någon minut natt till natt, så ett verkligt tvådagarsgap (en produkt saknades exakt en dag
i `raw/rugvista_state/`) kunde ibland mäta strax under 48 timmar och slinka igenom spärren.
Bekräftat konkret på 2025-11-18: två artiklar (`621923`, `621992`) saknades i
2025-11-17-snapshoten men fanns 2025-11-16 och 2025-11-18 — gapet mätte 47,92 timmar,
vilket gav ett falskt delta på 2 + 35 = 37 enheter, exakt differensen mot xlsx den dagen
(841 vs 804).

Ingen av de två tidigare hypoteserna stämde: en direkt ombyggnad av
`compute_sales_from_deltas()`-logiken från `raw/rugvista_state/`-filerna gav exakt samma
siffror som xlsx (inklusive `sold_units_missing_price = 0`), vilket uteslöt
pris-hypotesen helt.

**Fix:** bytte 48-timmarsspärren mot en kalenderdagsjämförelse
(`day - prev_day <= 1`, båda i Europe/Stockholm) — immun mot klockslagsjitter eftersom den
bara bryr sig om kalenderdatum, inte exakt antal timmar. Validerat mot hela historiken
(286 dagar) efter fixen: **0 avvikelser**.

Vyn är nu godkänd för analys.

## 2. `kpi-history.xlsx` har dubbletter och luckor

`data/kpi-history.xlsx` (flik `kpi-history`) innehåller:
- Dubblettrader för **2025-10-03** och **2025-10-04** (tre identiska rader vardera).
- Saknade datum: **2025-11-16** och **2026-04-03**.

## 3. Delta-baserade script antar exakt ett dygns mellanrum mellan körningar — varning tillagd 2026-08-05

Script som beräknar sålda enheter/intäkt via lagerdifferens mellan snapshots
(`track_nelly_inventory.py`, `track_anoto_inventory.py` — båda butiker, Anoto och Neo —
`track_ahlsell_plejd_inventory.py`) antar att föregående snapshot är från gårdagen. Ingen av dem
verifierade faktisk tid sedan föregående körning — om en nattlig körning missas (t.ex. timeout,
se `continue-on-error: true` i [.github/workflows/daily.yml](.github/workflows/daily.yml)) räknas
mellanliggande dagars förändring ändå som ett enda dygns delta, vilket ger felaktigt höga
sålda-enheter-siffror för den dagen.

(`track_rvrc_sales.py` **tillhör inte denna grupp** trots att det stod med i en tidigare version
av denna punkt — det scriptet använder API:ts egna `sale_last_week`/`sale_last_days`-rullande
räknare direkt, inte en egen lagerdifferens mellan två körningar, så det är immunt mot denna
felklass redan genom sin design.)

**Åtgärdat (delvis):** ett gemensamt `core.cli.warn_if_gap()` kollar nu, i alla tre påverkade
script, kalenderdagarna mellan föregående sparade snapshot och dagens körning, och skriver en
tydlig `[VARNING]`-rad i körloggen om gapet är större än 1 dag. **Detta ändrar inte den
beräknade siffran** — samma beteende som redan validerats mot xlsx-historiken bibehålls
medvetet (jfr `ahlsell_plejd_sales_v`/`rugvista_daily_sales_v`/`anoto_daily_sales_v`, som redan
har en oberoende, korrekt kalenderdagsspärr på SQL-vy-nivå och därför inte påverkas av detta
script-sidiga problem alls). Kvarstår: `nelly_daily_summary`/`rvrc_sales_daily_summary` har
ingen motsvarande vy-nivå-skyddsmekanism ännu — de Python-beräknade siffrorna skrivs direkt till
databasen och läses direkt av Power Query, så ett missat dygn där skulle fortfarande synas som
en felaktig topp i "Daily Summary"-fliken, bara nu med en varningsrad i körloggen som förklaring.

## 4. Dashboardflikar som matas av borttagna/stoppade script visar platt linje efter 2026-06-22

Minst `data/revolutionrace_state.json` (och därmed beroende flikar/scripts) fick sin sista commit
2026-06-22 — inga nyare snapshots finns i git-historiken. Alla dashboards/flikar som bygger på
dessa filer kommer visa en platt linje efter det datumet, vilket kan misstolkas som att
verksamheten stannat av snarare än att datainsamlingen tystnat.

## 5. ~~Nollsaldon saknas i Ahlsell-lagerdata, både i Excel och i databasen~~ — LÖST 2026-08-05

`fetch_stock()` i `track_ahlsell_plejd_inventory.py` returnerade bara poster där kvantiteten var
större än noll. När ett lagersaldo nådde noll försvann alltså raden ur snapshotet i stället för
att lagras som en nolla. Detsamma gällde `ahlsell_stock_snapshot` i databasen, eftersom den fylls
från samma data. Bekräftat konkret via ett live-anrop mot API:t: ett enskilt artikelnummer gav
110 butiksposter totalt, varav 11 med kvantitet 0 — som filtret kastade bort.

Python-koden i den befintliga Excel/state-diff-logiken hanterade redan detta korrekt via
`set(prev_wh) | set(curr_wh)` och `.get(wid, 0.0)`, som behandlar en saknad post som noll — och
den redan validerade `ahlsell_plejd_sales_v` (se punkt 8) är också opåverkad, eftersom den
jämför hela kalenderdagspar via `FULL OUTER JOIN` + `COALESCE(quantity, 0)`, inte `lag()` per
artikel/lager. En framtida vy byggd med `lag()` OVER (PARTITION BY article, warehouse ORDER BY
snapshot_date) hade däremot varit sårbar — den hoppar tillbaka till senast kända rad oavsett hur
många kalenderdagar bort den är, och hade räknat en flerdagars nollperiod som en enda dags
rörelse. Grundproblemet fanns ändå kvar i rådatan: vem som helst som frågade
`ahlsell_stock_snapshot` direkt (utanför vyn) skulle fått fel resultat för "hur många artiklar
har nollsaldo idag", eftersom nollsaldon helt enkelt inte fanns som rader.

**Fix:** tog bort `if (entry.get("stock", {}).get("quantity") or 0) > 0`-filtret i
`fetch_stock()` — sparar nu alla butiksposter, inklusive nollor. Träder i kraft från nästa
schemalagda körning (dagens körning hade redan skett när fixen landade, så "redan körd idag"-
grenen byggde om dagens rader från det gamla, redan hämtade tillståndet snarare än att hämta på
nytt).
