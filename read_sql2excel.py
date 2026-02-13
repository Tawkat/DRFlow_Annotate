"""
Export SQLite questions + annotations to an Excel sheet.

Reads from annotations.db (questions + annotations tables) and writes one Excel file:
  - Rows: one per question (all columns: task_id, dr_question, domain, company_*, user_*, etc.)
  - Columns: all question columns, then one column per annotator (Annotator_xxx) with values +1 / -1 / 0

Usage:
  python read_sql2excel.py
  python read_sql2excel.py --db path/to/annotations.db --output path/to/export.xlsx

For the live Railway DB you cannot download the file directly; use the app's export URL instead:
  https://your-app.railway.app/api/export
"""
import argparse
import os
import sqlite3
from pathlib import Path

import pandas as pd


APP_DIR = Path(__file__).resolve().parent
DEFAULT_DB = Path(os.environ.get("ANNOTATION_DB", str(APP_DIR / "data" / "annotations.db")))
DEFAULT_OUTPUT = APP_DIR / "data" / "dr_questions_annotations.xlsx"
SHEET_NAME = "dr_questions"

# All columns expected in the questions table
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


def export_sqlite_to_excel(db_path: Path, output_path: Path) -> tuple[int, int]:
    """
    Read questions and annotations from SQLite, write one Excel sheet.
    Returns (num_questions, num_annotator_columns).
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        # Determine which QUESTION_COLUMNS actually exist in the DB
        cur = conn.execute("PRAGMA table_info(questions)")
        existing_cols = {row[1] for row in cur.fetchall()}
        available_cols = [c for c in QUESTION_COLUMNS if c in existing_cols]

        col_list = ", ".join(available_cols)
        questions = pd.read_sql_query(
            f"SELECT {col_list} FROM questions ORDER BY task_id",
            conn,
        )
        if questions.empty:
            raise ValueError("No questions in database. Run upload_excel2sqlite.py first.")

        annotations = pd.read_sql_query(
            "SELECT annotator_id, task_id, value FROM annotations",
            conn,
        )
    finally:
        conn.close()

    # Pivot: one column per annotator_id, rows keyed by task_id
    df = questions.copy()
    if not annotations.empty:
        pivot = annotations.pivot(index="task_id", columns="annotator_id", values="value").fillna(0).astype(int)
        for col in pivot.columns:
            df[col] = df["task_id"].map(pivot[col]).fillna(0).astype(int)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False, engine="openpyxl", sheet_name=SHEET_NAME)
    n_annotators = len(df.columns) - len(available_cols)
    return len(df), max(0, n_annotators)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export SQLite questions + annotations to Excel (for local annotations.db). "
        "For Railway live DB use the app URL: /api/export"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to annotations.db")
    parser.add_argument("--output", "-o", type=Path, default=DEFAULT_OUTPUT, help="Output Excel path")
    args = parser.parse_args()
    n_rows, n_cols = export_sqlite_to_excel(args.db, args.output)
    print(f"Exported {n_rows} questions and {n_cols} annotator columns to {args.output}")


if __name__ == "__main__":
    main()
