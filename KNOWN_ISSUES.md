# Known Issues

Senast uppdaterad: 2026-08-05

## 11. Google Trends-scripten (8 st) troligen trasiga p.g.a. hårdare rate-limiting

Alla 8 `fetch_*_trends.py`-script migrerades till `google_trends_monthly` (schema +
engångsbackfill från befintlig xlsx-historik, se `scripts/tools/load_google_trends_history.py`),
och DB-skrivning kopplades in i varje scripts kod (`core/trends.py`). **Ingen av de 8 kördes
live under migreringen** — beslutat medvetet, eftersom Google Trends/pytrends är extremt
känsligt för 429-blockering och en full körning kan ta flera minuter per script. Det är alltså
inte verifierat att skrivvägen fungerar mot en riktig scrape, bara att koden är korrekt kopplad
(verifierat med syntetisk testdata mot databasen) och att bakgrundsdatan i xlsx-filerna är
korrekt inläst.

Misstanken är att scripten redan är trasiga eller att Google skärpt sina motåtgärder sedan
scripten skrevs (flera har redan omfattande backoff/retry-logik som tyder på tidigare problem).
**Måste ses över** – kör varje script manuellt och kontrollera om det fortfarande får fram data
innan de körs/schemaläggs på riktigt, eller innan man litar på att `google_trends_monthly`
kommer fyllas på med nya månader framöver.

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

## 6. `track_nelly_aov.py` hittar inga priser sedan 2026-07-20

Scriptet är fortfarande schemalagt och kör felfritt (`continue-on-error` döljer det),
men `fetch_prices()` hittar 0 priser på samtliga 10 sidor på `nelly.com/se/topplistan/`
sedan 2026-07-20 — troligen har Nelly ändrat HTML-strukturen/selektorerna
(`DISCOUNT_PRICE_SELECTOR`/`REGULAR_PRICE_SELECTOR`). Scriptet avslutar tyst utan att
skriva någon rad (varken Excel eller databas) när `all_prices` är tom, så `data/nelly_aov.xlsx`
och `nelly_aov`-tabellen har helt enkelt saknat nya rader i över en vecka utan någon synlig
varning. Behöver en uppdatering av selektorerna mot nuvarande sidstruktur.

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

## 3. Delta-baserade script antar exakt ett dygns mellanrum mellan körningar

Script som beräknar sålda enheter/intäkt via lagerdifferens mellan snapshots
(t.ex. `track_rvrc_sales.py`, `track_nelly_inventory.py`, `track_anoto_inventory.py`,
`track_ahlsell_plejd_inventory.py`) antar att föregående snapshot är från gårdagen. Ingen av dem
verifierar faktisk tid sedan föregående körning — om en nattlig körning missas (t.ex. timeout,
se `continue-on-error: true` i [.github/workflows/daily.yml](.github/workflows/daily.yml)) räknas
mellanliggande dagars förändring ändå som ett enda dygns delta, vilket ger felaktigt höga
sålda-enheter-siffror för den dagen.

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
