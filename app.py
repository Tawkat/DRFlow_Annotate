"""
Flask web app for DR question data annotation.
Uses SQLite (persistent) when data/annotations.db exists, else Excel.
"""
import os
import re
from pathlib import Path

import pandas as pd
from io import BytesIO
from flask import Flask, jsonify, render_template, request, send_file

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR.parent / "data" / "labeling"
EXCEL_PATH = DATA_DIR / "dr_questions.xlsx"
SHEET_NAME = "dr_questions"

# All columns expected in the questions table / Excel
QUESTION_COLUMNS = [
    "task_id",
    "dr_question",
    "domain",
    "company_name",
    "company_industry",
    "company_description",
    "company_size",
    "company_employee_count",
    "company_annual_revenue",
    "company_persona",
    "company_persona_email",
    "company_persona_role",
    "company_persona_role_description",
    "user_name",
    "user_role",
    "user_email",
    "user_role_description",
    "user_company",
    "user_industry",
    "user_company_description",
    "user_company_size",
    "user_company_employee_count",
    "user_company_annual_revenue",
]
REQUIRED_COLUMNS = ["task_id", "dr_question", "domain"]

# SQLite: use if file exists or ANNOTATION_DB is set (e.g. Railway volume at /data/annotations.db)
DB_PATH = Path(os.environ.get("ANNOTATION_DB", str(APP_DIR / "data" / "annotations.db")))
# Bundled Excel for auto-seed when deploying (e.g. Railway) without running upload script
BUNDLED_EXCEL = APP_DIR / "data" / "dr_questions.xlsx"

app = Flask(__name__, template_folder="templates", static_folder="static")


def _sanitize_annotator_id(name: str) -> str:
    """Normalized annotator id (case-insensitive)."""
    if not name or not name.strip():
        return ""
    s = name.strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[-\s]+", "_", s).strip("_").lower()
    return f"Annotator_{s}" if s else ""


def _use_sqlite() -> bool:
    # Use SQLite when file exists or when explicitly set (e.g. Railway ANNOTATION_DB=/data/annotations.db)
    return DB_PATH.exists() or os.environ.get("ANNOTATION_DB") is not None


def _get_db():
    import sqlite3
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(DB_PATH))


def _build_questions_ddl() -> str:
    """Build CREATE TABLE DDL for questions with all columns."""
    col_defs = []
    for col in QUESTION_COLUMNS:
        if col == "task_id":
            col_defs.append("task_id TEXT PRIMARY KEY")
        elif col in REQUIRED_COLUMNS:
            col_defs.append(f"{col} TEXT NOT NULL")
        else:
            col_defs.append(f"{col} TEXT DEFAULT ''")
    return (
        "CREATE TABLE IF NOT EXISTS questions (\n"
        + ",\n".join(f"    {d}" for d in col_defs)
        + "\n);"
    )


def _migrate_add_columns(conn) -> None:
    """Add any new columns that don't exist yet (for existing DBs with the old 3-column schema)."""
    cur = conn.execute("PRAGMA table_info(questions)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col in QUESTION_COLUMNS:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE questions ADD COLUMN {col} TEXT DEFAULT ''")
    conn.commit()


def _ensure_sqlite_seeded() -> None:
    """If DB has no questions, create schema and seed from bundled Excel (for Railway etc.)."""
    import sqlite3
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        questions_ddl = _build_questions_ddl()
        conn.executescript(f"""
            {questions_ddl}
            CREATE TABLE IF NOT EXISTS annotations (
                annotator_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                value INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (annotator_id, task_id),
                CHECK (value IN (-1, 0, 1)),
                FOREIGN KEY (task_id) REFERENCES questions(task_id)
            );
            CREATE INDEX IF NOT EXISTS idx_annotations_annotator ON annotations(annotator_id);
        """)
        conn.commit()
        _migrate_add_columns(conn)
        row = conn.execute("SELECT COUNT(*) FROM questions").fetchone()
        if row and row[0] == 0 and BUNDLED_EXCEL.exists():
            import pandas as _pd
            df = _pd.read_excel(BUNDLED_EXCEL, engine="openpyxl", sheet_name=SHEET_NAME)
            available_cols = [c for c in QUESTION_COLUMNS if c in df.columns]
            placeholders = ", ".join("?" for _ in available_cols)
            col_names = ", ".join(available_cols)
            sql = f"INSERT OR REPLACE INTO questions ({col_names}) VALUES ({placeholders})"
            for _, r in df.iterrows():
                values = []
                for col in available_cols:
                    val = r[col]
                    if _pd.notna(val):
                        values.append(str(val).strip())
                    else:
                        values.append("")
                conn.execute(sql, values)
            conn.commit()
    finally:
        conn.close()


def _questions_from_sqlite() -> list[dict]:
    """Return all questions with ALL columns from SQLite."""
    col_list = ", ".join(QUESTION_COLUMNS)
    with _get_db() as conn:
        conn.row_factory = lambda c, r: dict(zip([x[0] for x in c.description], r))
        cur = conn.execute(
            f"SELECT {col_list} FROM questions ORDER BY task_id"
        )
        return [dict(row) for row in cur.fetchall()]


def _annotation_from_sqlite(annotator_id: str, task_id: str) -> int:
    with _get_db() as conn:
        row = conn.execute(
            "SELECT value FROM annotations WHERE annotator_id = ? AND task_id = ?",
            (annotator_id, task_id),
        ).fetchone()
        return int(row[0]) if row else 0


def _annotations_for_annotator_sqlite(annotator_id: str) -> dict[str, int]:
    with _get_db() as conn:
        cur = conn.execute(
            "SELECT task_id, value FROM annotations WHERE annotator_id = ?",
            (annotator_id,),
        )
        return {row[0]: int(row[1]) for row in cur.fetchall()}


def _set_annotation_sqlite(annotator_id: str, task_id: str, value: int) -> None:
    with _get_db() as conn:
        conn.execute(
            """INSERT INTO annotations (annotator_id, task_id, value) VALUES (?, ?, ?)
               ON CONFLICT (annotator_id, task_id) DO UPDATE SET value = ?""",
            (annotator_id, task_id, value, value),
        )
        conn.commit()


def _task_exists_sqlite(task_id: str) -> bool:
    with _get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM questions WHERE task_id = ?", (str(task_id),)
        ).fetchone()
        return row is not None


# ---- Excel path (legacy / local)
def _find_annotator_column(df: pd.DataFrame, name: str) -> str | None:
    want = _sanitize_annotator_id(name)
    if not want:
        return None
    if want in df.columns:
        return want
    for col in df.columns:
        if str(col).strip().lower() == want:
            return col
    return None


def _load_df() -> pd.DataFrame:
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"Excel file not found: {EXCEL_PATH}")
    return pd.read_excel(EXCEL_PATH, engine="openpyxl", sheet_name=SHEET_NAME)


def _save_df(df: pd.DataFrame) -> None:
    EXCEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(EXCEL_PATH, index=False, engine="openpyxl", sheet_name=SHEET_NAME)


def _combine_user_role_info(q: dict) -> str:
    """Combine user_role and user_role_description into a single display string."""
    role = str(q.get("user_role", "") or "").strip()
    desc = str(q.get("user_role_description", "") or "").strip()
    if role and desc:
        return f"{role} — {desc}"
    return role or desc or ""


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/questions", methods=["GET"])
def get_questions():
    user = request.args.get("user", "").strip()
    if _use_sqlite():
        try:
            _ensure_sqlite_seeded()
            questions = _questions_from_sqlite()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        annotator_id = _sanitize_annotator_id(user) if user else None
        annotations = _annotations_for_annotator_sqlite(annotator_id) if annotator_id else {}
        rows = []
        for i, q in enumerate(questions):
            task_id = str(q["task_id"])
            rows.append({
                "index": i,
                "task_id": task_id,
                "user_role_info": _combine_user_role_info(q),
                "domain": str(q.get("domain", "")),
                "dr_question": str(q.get("dr_question", "")),
                "annotation": annotations.get(task_id, 0),
            })
        return jsonify({"questions": rows, "annotator_column": annotator_id})
    # Excel
    try:
        df = _load_df()
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            return jsonify({"error": f"Missing column: {col}"}), 500
    annot_col = _find_annotator_column(df, user) if user else None
    rows = []
    for i, row in df.iterrows():
        q = row.to_dict()
        item = {
            "index": int(i),
            "task_id": str(q.get("task_id", "")),
            "user_role_info": _combine_user_role_info(q),
            "domain": str(q.get("domain", "")),
            "dr_question": str(q.get("dr_question", "")),
        }
        if annot_col:
            val = row.get(annot_col)
            if pd.isna(val):
                item["annotation"] = 0
            else:
                try:
                    item["annotation"] = int(float(val))
                except (TypeError, ValueError):
                    item["annotation"] = 0
        else:
            item["annotation"] = 0
        rows.append(item)
    return jsonify({"questions": rows, "annotator_column": annot_col or None})


@app.route("/api/annotate", methods=["POST"])
def annotate():
    data = request.get_json(force=True, silent=True) or {}
    user = (data.get("user") or "").strip()
    task_id = data.get("task_id")
    try:
        value = int(data.get("value", 0))
    except (TypeError, ValueError):
        value = 0
    if value not in (1, -1, 0):
        value = 0
    if not user:
        return jsonify({"error": "user is required"}), 400
    if task_id is None or task_id == "":
        return jsonify({"error": "task_id is required"}), 400

    annotator_id = _sanitize_annotator_id(user)
    if not annotator_id:
        return jsonify({"error": "invalid user name"}), 400

    if _use_sqlite():
        if not _task_exists_sqlite(task_id):
            return jsonify({"error": f"task_id not found: {task_id}"}), 404
        try:
            _set_annotation_sqlite(annotator_id, str(task_id), value)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        return jsonify({"ok": True, "task_id": task_id, "value": value})

    # Excel
    try:
        df = _load_df()
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    if "task_id" not in df.columns:
        return jsonify({"error": "task_id column not found"}), 500
    col = _find_annotator_column(df, user) or annotator_id
    if col not in df.columns:
        df[col] = 0
    mask = df["task_id"].astype(str) == str(task_id)
    if not mask.any():
        return jsonify({"error": f"task_id not found: {task_id}"}), 404
    df.loc[mask, col] = value
    _save_df(df)
    return jsonify({"ok": True, "task_id": task_id, "value": value})


@app.route("/api/export")
def export_excel():
    """Download current questions (all columns) + all annotations from SQLite as an Excel file."""
    if not _use_sqlite():
        return jsonify({"error": "Export only available when using SQLite (e.g. on Railway)"}), 400
    import sqlite3
    _ensure_sqlite_seeded()
    conn = sqlite3.connect(str(DB_PATH))
    try:
        col_list = ", ".join(QUESTION_COLUMNS)
        questions = pd.read_sql_query(
            f"SELECT {col_list} FROM questions ORDER BY task_id",
            conn,
        )
        if questions.empty:
            return jsonify({"error": "No questions in database"}), 404
        annotations = pd.read_sql_query(
            "SELECT annotator_id, task_id, value FROM annotations",
            conn,
        )
    finally:
        conn.close()
    df = questions.copy()
    if not annotations.empty:
        pivot = annotations.pivot(
            index="task_id", columns="annotator_id", values="value"
        ).fillna(0).astype(int)
        for col in pivot.columns:
            df[col] = df["task_id"].map(pivot[col]).fillna(0).astype(int)
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl", sheet_name=SHEET_NAME)
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="dr_questions_annotations.xlsx",
    )


if __name__ == "__main__":
    app.run(debug=True, port=5050)
