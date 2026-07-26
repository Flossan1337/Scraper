# Known Issues

Senast uppdaterad: 2026-07-26

## 1. `rugvista_daily_sales_v` avviker från `data/rugvista_daily_sales.xlsx`

Vyn ([sql/views/rugvista_daily_sales.sql](sql/views/rugvista_daily_sales.sql)) avviker från
`data/rugvista_daily_sales.xlsx` på **54 av 283 dagar**. Vyn räknar konsekvent **högre**
units_sold/revenue på de avvikande dagarna, aldrig lägre.

Två hypoteser, ingen bekräftad:
- `LAG()` i vyn hoppar över luckor när produkter tillfälligt lämnar och sedan återvänder till
  topplistan (Rugvista-API:et hämtas med `topSeller=true`), vilket kan ge ett annat "föregående"
  värde än vad det ursprungliga Python-scriptets state-fil hade vid samma tillfälle.
- Olika hantering av enheter utan pris: `track_rugvista_daily_sales.py` exkluderar sålda enheter
  helt ur totalen om pris saknas (`sold_units_missing_price`), medan vyns motsvarande villkor
  (`price_sek IS NOT NULL`) kan träffa andra rader om historiken i `raw/rugvista_state/` skiljer
  sig något från vad som fanns i `data/rugvista_state.json` vid respektive körning.

**Ej utrett.** Root cause är inte bekräftad.

**Vyn får inte användas för analys förrän detta är löst.**

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
