"""
Load dr_questions.xlsx into SQLite for persistent annotation hosting.

Creates (or overwrites) a SQLite DB with:
  - questions: all columns from the Excel (task_id, dr_question, domain, company_*, user_*, etc.)
  - annotations: annotator_id, task_id, value (+1 / -1 / 0) — filled by the app

Usage:
  python upload_excel2sqlite.py
  python upload_excel2sqlite.py --excel path/to/dr_questions.xlsx --db path/to/annotations.db
"""
import argparse
import sqlite3
from pathlib import Path

import pandas as pd


APP_DIR = Path(__file__).resolve().parent
DEFAULT_EXCEL = APP_DIR.parent / "data" / "labeling" / "dr_questions.xlsx"
DEFAULT_DB = APP_DIR / "data" / "annotations.db"
SHEET_NAME = "dr_questions"

# All columns expected in the Excel / questions table
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

# Columns beyond these three are optional TEXT with default ''
REQUIRED_COLUMNS = ["task_id", "dr_question", "domain"]


def create_schema(conn: sqlite3.Connection) -> None:
    # Build column definitions: task_id is PK, dr_question and domain are NOT NULL,
    # all other columns are TEXT DEFAULT ''
    col_defs = []
    for col in QUESTION_COLUMNS:
        if col == "task_id":
            col_defs.append("task_id TEXT PRIMARY KEY")
        elif col in REQUIRED_COLUMNS:
            col_defs.append(f"{col} TEXT NOT NULL")
        else:
            col_defs.append(f"{col} TEXT DEFAULT ''")

    questions_ddl = (
        "CREATE TABLE IF NOT EXISTS questions (\n"
        + ",\n".join(f"    {d}" for d in col_defs)
        + "\n);"
    )

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


def _migrate_add_columns(conn: sqlite3.Connection) -> None:
    """Add any new columns that don't exist yet (for existing DBs with the old 3-column schema)."""
    cur = conn.execute("PRAGMA table_info(questions)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col in QUESTION_COLUMNS:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE questions ADD COLUMN {col} TEXT DEFAULT ''")
    conn.commit()


def load_excel(excel_path: Path) -> pd.DataFrame:
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel not found: {excel_path}")
    return pd.read_excel(excel_path, engine="openpyxl", sheet_name=SHEET_NAME)


def upload(excel_path: Path, db_path: Path, replace: bool = True) -> int:
    df = load_excel(excel_path)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Excel missing required column: {col}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        create_schema(conn)
        _migrate_add_columns(conn)
        if replace:
            conn.execute("DELETE FROM questions")

        # Determine which columns from QUESTION_COLUMNS are present in the Excel
        available_cols = [c for c in QUESTION_COLUMNS if c in df.columns]
        placeholders = ", ".join("?" for _ in available_cols)
        col_names = ", ".join(available_cols)
        sql = f"INSERT OR REPLACE INTO questions ({col_names}) VALUES ({placeholders})"

        count = 0
        for _, row in df.iterrows():
            values = []
            for col in available_cols:
                val = row[col]
                if pd.notna(val):
                    values.append(str(val).strip())
                else:
                    values.append("")
            conn.execute(sql, values)
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload dr_questions.xlsx to SQLite")
    parser.add_argument("--excel", type=Path, default=DEFAULT_EXCEL, help="Path to dr_questions.xlsx")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite DB (e.g. data/annotations.db)")
    parser.add_argument("--no-replace", action="store_true", help="Do not clear questions table before insert (still upserts by task_id)")
    args = parser.parse_args()

    n = upload(args.excel, args.db, replace=not args.no_replace)
    print(f"Uploaded {n} questions to {args.db}")


if __name__ == "__main__":
    main()
