# Kodning

## 👶 Nybörjarsammanfattning
Dina Python‑scripts bor i mappen `./scripts`. När du kör allt (lokalt **eller** i GitHub)
startar `scripts/runner.py` och kör dem i tur och ordning.

Allt som scriptsen vill spara hamnar i **Excel‑filer** under `./data`.
Om filen inte finns **skapas den**. Saknas fliken **skapas den**.
Finns fliken **lägger vi till en ny rad** längst ned. Du tappar alltså aldrig gammal data.

GitHub Actions kör samma sak i molnet. Workflowfilen heter **`scrape.yml`** och ligger i
`./.github/workflows/`. Där installerar den Python, dina beroenden och kör `runner.py`.
Efter körningen kan du ladda ner resultaten som artifacts.

---

## 🎯 Syfte
Köra alla dina datainsamlings‑/analys‑scripts både lokalt och via GitHub Actions med **samma logik**
och säker Excel‑export (skapa fil → skapa flik → append rad).

## 📦 Struktur
```
/Kodning
  README.md
  requirements.txt
  /data                    # xlsx‑filer + run.log (skapas automatiskt)
  /scripts                 # alla dina scripts
    excel_utils.py         # hjälpfunktioner för Excel (append_row/append_df)
    runner.py              # kör alla scripts i ordning, migrerar CSV→XLSX
  /.github/workflows
    scrape.yml             # GitHub Actions workflow
```

### Vad gör varje del?
- **`scripts/excel_utils.py`** – återanvändbart API för att skriva till Excel enligt reglerna.
  - `append_row(xlsx_path, sheet_name, row_dict)` – lägg till **en** rad.
  - `append_df(xlsx_path, sheet_name, df)` – lägg till en **DataFrame** (fler rader).
- **`scripts/runner.py`** – hittar alla `*.py` i `./scripts` (utom `runner.py`/`excel_utils.py`) och kör dem.
  - Kör `main()` om det finns, annars kör filen som subprocess.
  - Migrerar automatiskt **CSV** som skapats till `./data/<namn>.xlsx` (flik `<namn>`).
  - Loggar till konsol och `./data/run.log`.
- **`./.github/workflows/scrape.yml`** – kör allt i GitHub Actions på `push`, `schedule` och `workflow_dispatch`.

## 🧮 Excel‑flöde (så funkar det)
1. **Skriv alltid till `.xlsx`** i `./data` (t.ex. `data/min_rapport.xlsx`).
2. Saknas filen? → Den **skapas**.
3. Saknas fliken? → Den **skapas**.
4. Finns fliken? → **Append** ny rad längst ned.
5. Om ett script fortfarande genererar `.csv` → `runner.py` kommer **migrera** den till `.xlsx`
   och append:a raderna i fliken som heter samma som csv‑filens namn.

## ▶️ Köra lokalt
```bash
# 1) Skapa och aktivera virtuell miljö (valfritt men rekommenderas)
python -m venv .venv
# Windows:
.\.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# 2) Installera beroenden
pip install -r requirements.txt

# 3) Kör alla scripts
python ./scripts/runner.py
```

> Tips: Lägg en `.env` i repo‑roten vid behov (API‑nycklar etc). `runner.py` laddar den automatiskt.

## ☁️ GitHub Actions
- Workflowfil: `./.github/workflows/scrape.yml`
- Triggers: `push` mot `main/master`, `workflow_dispatch` (manuell), samt schemalagt (dagligen 06:15 UTC).
- Jobbet:
  1. Checkar ut repo
  2. Installerar Python 3.11 och `requirements.txt`
  3. Kör `python ./scripts/runner.py`
  4. Laddar upp `./data/**` som **artifact**

**Artifacts & loggar**: Efter körning – öppna jobbets artifacts och ladda ner `data-outputs`.
Där finns dina `.xlsx`‑filer och `run.log`.

## 🛠️ Lägga till nytt script
1. Lägg filen i `./scripts` (t.ex. `01_min_scraper.py` för att styra ordningen).
2. Skriv utdata via `excel_utils`:
   ```python
   from excel_utils import append_row, append_df
   append_row("data/min_samling.xlsx", "min_flik", {"Date": "...", "Value": 123})
   ```
3. `commit` och `push` – GitHub Actions kör automatiskt.

## ❗ Vanlig felsökning
- **Saknade beroenden**: `pip install -r requirements.txt` (lokalt) eller uppdatera filen.
- **Excel‑fil låst**: Stäng filen i Excel. `runner.py` gör några omförsök, men låsta filer ger oftast `PermissionError`.
- **Fel fliknamn**: Dubbelkolla `sheet_name`. Finns den redan används append.
- **Rättigheter i CI**: Skriv endast till `./data/**`. Andra paths kan sakna rättigheter.

## 🔁 CSV → XLSX‑migrering
Om äldre scripts skriver `.csv`, behöver du **inte** ändra direkt. `runner.py` plockar upp csv:er i
repo‑roten **och** i `./data`, läser dem och append:ar till `./data/<namn>.xlsx`.
På sikt rekommenderas att uppdatera skripten att använda `excel_utils` direkt.

---

Lycka till! 🚀
