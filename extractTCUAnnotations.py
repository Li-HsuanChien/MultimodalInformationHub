"""
Export TCU rows with all 'common' annotations pivoted onto the same row.

Uses a single LEFT JOIN query; Python only handles the pivot step.

Usage:
    python export_tcu_annotations.py --db path/to/database.db --out output.csv

Each annotator's columns are suffixed with their index (1-based), e.g.:
    annotator1_email, annotator1_speaker_role, annotator1_speaker_gender, ...
    annotator2_email, annotator2_speaker_role, ...
"""

import sqlite3
import csv
import argparse
from collections import OrderedDict


TCU_FIELDS = [
    "TCUID",
    "VIDEOID",
    "tcu_start",
    "tcu_end",
    "tcu_transcript",
]

ANNOTATION_FIELDS = [
    "email",
    "speaker_role",
    "speaker_gender",
    "stance",
    "vocal_tone",
    "facial_expression",
    "coder_notes",
]


def export(db_path: str, out_path: str) -> None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Single JOIN query — DB handles filtering and joining,
    # Python only needs to pivot the repeated annotation columns.
    tcu_cols = ", ".join(f't."{f}" AS "{f}"' for f in TCU_FIELDS)
    ann_cols = ", ".join(f'a."{f}" AS "ann_{f}"' for f in ANNOTATION_FIELDS)    
    query = f"""
        SELECT {tcu_cols}, {ann_cols}
        FROM TCU t
        LEFT JOIN Annotation a
            ON t.TCUID = a.TCUID AND a.annotationtype = 'common'
        ORDER BY t.TCUID, a.Email
    """

    rows = con.execute(query).fetchall()
    con.close()

    # Pivot: group annotation columns by TCUID
    # OrderedDict preserves the original TCU ordering from the query
    tcu_map: OrderedDict = OrderedDict()

    for row in rows:
        tcuid = row["TCUID"]
        if tcuid not in tcu_map:
            tcu_map[tcuid] = {
                "tcu": {f: row[f] for f in TCU_FIELDS},
                "annotations": [],
            }
        # Only append if this row actually has annotation data
        if row["ann_email"] is not None:
            tcu_map[tcuid]["annotations"].append(
                {f: row[f"ann_{f}"] for f in ANNOTATION_FIELDS}
            )
    # Find max annotators to build a fixed set of columns
    max_annotators = max(
        (len(v["annotations"]) for v in tcu_map.values()), default=0
    )

    annotator_headers = [
        f"annotator{i}_{f.lower()}"
        for i in range(1, max_annotators + 1)
        for f in ANNOTATION_FIELDS
    ]
    header = TCU_FIELDS + annotator_headers
    
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()

        for entry in tcu_map.values():
            row = dict(entry["tcu"])

            for i, ann in enumerate(entry["annotations"], start=1):
                for field, value in ann.items():
                    row[f"annotator{i}_{field.lower()}"] = value

            # Pad any missing annotator slots with empty strings
            for i in range(len(entry["annotations"]) + 1, max_annotators + 1):
                for f in ANNOTATION_FIELDS:
                    row[f"annotator{i}_{f.lower()}"] = ""

            writer.writerow(row)

    print(f"Exported {len(tcu_map)} TCUs → {out_path}")
    print(f"Max annotators per TCU: {max_annotators}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export TCU + common annotations to CSV."
    )
    parser.add_argument("--db", required=True, help="Path to the SQLite database file")
    parser.add_argument("--out", default="tcu_annotations.csv", help="Output CSV path")
    args = parser.parse_args()

    export(args.db, args.out)