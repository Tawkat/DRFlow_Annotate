# DR Question Annotation UI

Web app for annotating DR questions with thumbs up/down. Uses **SQLite** (`annotations.db`) for persistent annotations when the DB path exists or `ANNOTATION_DB` is set; otherwise falls back to Excel.

## Run locally

```bash
# 1. Seed SQLite from Excel (one-time)
python upload_excel2sqlite.py

# 2. Start the app (uses data/annotations.db)
python app.py
```

Open http://127.0.0.1:5050

## Deploy on Railway.app (persistent annotations.db)

1. **Prepare the repo**
   - Copy the questions Excel into this app so the first deploy can auto-seed the DB:
     ```bash
     cp ../data/labeling/dr_questions.xlsx data/dr_questions.xlsx
     ```
   - Commit `data/dr_questions.xlsx` (and ensure `data/annotations.db` is **not** committed so Railway uses a fresh volume).

2. **Create a Railway project**
   - Go to [railway.app](https://railway.app), sign in, **New Project**.
   - **Deploy from GitHub repo** (or CLI): connect the repo and set **Root Directory** to `drbench/drbench_basic/dr_question_annotation_ui` (or wherever this folder lives).

3. **Add a Volume (for persistent annotations.db)**
   - In the project, open your service → **Variables** tab.
   - Go to **Volumes** (or **Settings**), click **Add Volume**, create a volume and **mount path**: `/data`.
   - Add a variable:
     - **Name:** `ANNOTATION_DB`
     - **Value:** `/data/annotations.db`
   - Redeploy so the app runs with the volume mounted.

4. **Deploy**
   - Railway will use the **Procfile** (`web: gunicorn app:app --bind 0.0.0.0:$PORT`) and install deps from `requirements.txt`.
   - On first request the app will create `/data/annotations.db` and seed it from the bundled `data/dr_questions.xlsx` if the questions table is empty.

5. **Optional: custom domain**
   - In the service, **Settings** → **Networking** → **Generate Domain** or add your own.

## Usage

1. Enter your name and click **Start / Load my annotations**.
2. Questions load with your existing annotations (if any) restored.
3. Click 👍 for +1, 👎 for -1. Click the same icon again or double-click to clear (0).
4. Annotations are stored per annotator in SQLite (or Excel when not using SQLite).

## Download annotations as Excel

**Why GitHub’s `annotations.db` is not updated:** On Railway, the database lives on a **volume** on Railway’s servers (e.g. `/data/annotations.db`). It is not in your GitHub repo, so any file you see in the repo is an old or empty copy.

**Get the current annotations as Excel from the live app:**

- Open your Railway app in the browser, then go to:  
  **`https://your-app.railway.app/api/export`**  
  (replace with your real Railway URL).  
  That endpoint returns the current questions and all annotator columns as an Excel file (`dr_questions_annotations.xlsx`).

**Locally (from a local `annotations.db`):**

```bash
python read_sql2excel.py --db data/annotations.db --output my_export.xlsx
```

## Dependencies

- Flask, pandas, openpyxl, gunicorn (see `requirements.txt`)
