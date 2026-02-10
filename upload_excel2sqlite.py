"""
Load dr_questions.xlsx into SQLite for persistent annotation hosting.

Creates (or overwrites) a SQLite DB with:
  - questions: task_id, dr_question, domain (from Excel)
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


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS questions (
            task_id TEXT PRIMARY KEY,
            dr_question TEXT NOT NULL,
            domain TEXT NOT NULL
        );
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


def load_excel(excel_path: Path) -> pd.DataFrame:
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel not found: {excel_path}")
    return pd.read_excel(excel_path, engine="openpyxl", sheet_name=SHEET_NAME)


def upload(excel_path: Path, db_path: Path, replace: bool = True) -> int:
    df = load_excel(excel_path)
    for col in ["task_id", "dr_question", "domain"]:
        if col not in df.columns:
            raise ValueError(f"Excel missing column: {col}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        create_schema(conn)
        if replace:
            conn.execute("DELETE FROM questions")
        count = 0
        for _, row in df.iterrows():
            task_id = str(row["task_id"]).strip()
            dr_question = str(row["dr_question"]) if pd.notna(row["dr_question"]) else ""
            domain = str(row["domain"]).strip() if pd.notna(row["domain"]) else ""
            conn.execute(
                "INSERT OR REPLACE INTO questions (task_id, dr_question, domain) VALUES (?, ?, ?)",
                (task_id, dr_question, domain),
            )
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
