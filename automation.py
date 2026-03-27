import os
import sqlite3
import csv

from helperfunctions import (
    getIndex, time_to_seconds, extract_video_id,
    build_videoseg_id, getItem, getRequiredFields,
)
from logger import get_logger, alter_text_color


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt(label: str, color: str, user_email: str | None = None, colorize: bool = True) -> str:
    """
    Build a formatted log prefix.
    colorize=True  → ANSI codes (terminal)
    colorize=False → plain text (file)  — but since we use PlainFormatter
                     in the file handlers, escape codes would be stripped
                     anyway; this keeps the message itself clean.
    """
    tag   = alter_text_color(label, color, colorize)
    email = alter_text_color(user_email, "BLUE", colorize) if user_email else None
    if email:
        return f"{tag} user_email={email}"
    return tag


# ─────────────────────────────────────────────────────────────────────────────
# Core functions  (logger passed in from process_csv / distribute_files_to_user)
# ─────────────────────────────────────────────────────────────────────────────

def check_annotation_type(user_email: str, row, logger) -> str:
    if user_email == "" or row is None:
        logger.error(f"[MISSING INFO] user_email={user_email} or row={row}")
        return "None"

    for annotationtypeOption in ["irr", "common"]:
        annotationtype = "None"

        if annotationtypeOption == "irr" and user_email == "lxb5609@psu.edu":
            continue  # Luke does not have IRR fields

        for field in getRequiredFields(user_email, annotationtypeOption):
            requiredItem = getItem(row, field, "tcucsv")
            if (requiredItem is not None) and (requiredItem != "NA") and (requiredItem != ""):
                annotationtype = annotationtypeOption
                break
        if annotationtype != "None":
            break

    return annotationtype


def check_duplicate_annotation(user_email: str, tcu_id: str, cursor, logger) -> bool:
    try:
        cursor.execute(
            "SELECT 1 FROM Annotation WHERE Email = ? AND TCUID = ?",
            (user_email, tcu_id),
        )
        if cursor.fetchone():
            return False
        return True
    except sqlite3.Error as e:
        logger.error(f"[DB ERROR - Annotation CHECK] user_email={user_email} | TCU={tcu_id} | {e}")
        return False


def validate_duration(row) -> tuple[bool, str]:
    start_sec = time_to_seconds(getItem(row, "tcu_start", "tcucsv"))
    end_sec   = time_to_seconds(getItem(row, "tcu_end",   "tcucsv"))
    if start_sec is None or end_sec is None:
        return False, "FORMAT"
    duration = end_sec - start_sec
    if duration <= 0 or duration > 60:
        return False, "DURATION"
    return True, "None"


def insert_tcu_if_not_exists(tcu_id, videoseg_id, row, cursor, user_email: str, logger) -> bool:
    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO TCU (
                TCUID, VIDEOSEGID,
                tcu_start, tcu_end, tcu_transcript, tcu_adder_email,
                video_saved, audio_saved, frames_saved
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tcu_id,
                videoseg_id,
                getItem(row, "tcu_start",     "tcucsv"),
                getItem(row, "tcu_end",       "tcucsv"),
                getItem(row, "tcu_transcript","tcucsv"),
                user_email,
                False, False, False,
            ),
        )
        return True
    except sqlite3.Error as e:
        logger.error(f"[DB ERROR - TCU INSERT] user_email={user_email} | TCU={tcu_id} | {e}")
        return False


def insert_annotation(annotation_id, tcu_id, user_email: str, row, annotationtype, cursor, logger) -> bool:
    try:
        cursor.execute(
            """
            INSERT INTO Annotation (
                AnnotationID, TCUID, Email,
                speaker_role, speaker_gender, stance,
                vocal_tone, facial_expression, coder_notes, annotationtype
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                annotation_id,
                tcu_id,
                user_email,
                getItem(row, "speaker_role",      "tcucsv"),
                getItem(row, "speaker_gender",     "tcucsv"),
                getItem(row, "stance",             "tcucsv"),
                getItem(row, "vocal_tone",         "tcucsv"),
                getItem(row, "facial_expression",  "tcucsv"),
                getItem(row, "coder_notes",        "tcucsv"),
                annotationtype,
            ),
        )
        return True
    except sqlite3.IntegrityError as e:
        logger.error(f"[DB INTEGRITY ERROR] user_email={user_email} | TCU={tcu_id} | {e}")
        return False
    except sqlite3.Error as e:
        logger.error(f"[DB ERROR] user_email={user_email} | TCU={tcu_id} | {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# process_csv
# ─────────────────────────────────────────────────────────────────────────────

def process_csv(user_email: str, alias: str, file_path: str, DB_PATH: str) -> bool:
    logger = get_logger(user_email, alias)

    if not os.path.exists(file_path):
        logger.error(f"[ERROR] File not found for user_email={user_email}: {file_path}")
        return False

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    started = False

    with open(file_path, newline="", encoding="utf-8") as f:
        reader           = csv.reader(f)
        annotation_count = 0
        video_url            = None
        ai_mention_timestamp = None

        for i, row in enumerate(reader, start=1):
            if row[0] == "original_row_number":
                started = True
                continue
            if not started:
                continue

            missing_video_data = False
            missing_tcu_data   = False

            for j in range(7):
                if row[j] is None or row[j] == "":
                    missing_video_data = True
                    break

            for j in range(7, 11):
                if row[j] is None or row[j] == "" or row[j] == "NA":
                    missing_tcu_data = True
                    break

            if video_url is None and missing_video_data:
                logger.error(f"[MISSING VIDEOSEG DATA] user_email={user_email} | row={row}")
                continue
            elif not missing_video_data:
                video_url            = getItem(row, "video_url",            "tcucsv")
                ai_mention_timestamp = getItem(row, "ai_mention_timestamp", "tcucsv")
                continue
            elif missing_video_data and missing_tcu_data:
                logger.error(f"[MISSING FULL VIDEO INFO OR MISSING TCU INFO] user_email={user_email} | row={row}")
                continue

            # 1. Check annotation type
            annotationtype = check_annotation_type(user_email, row, logger)
            if annotationtype == "None" and not missing_tcu_data:
                logger.warning(f"[MISSING ANNOTATION] user_email={user_email} | row={row}")
                continue

            tcu_id = getItem(row, "tcu_id", "tcucsv")

            # 2. Check for duplicate annotation
            if not check_duplicate_annotation(user_email, tcu_id, cursor, logger):
                continue

            # 3. Validate duration
            duration_valid, reason = validate_duration(row)
            if not duration_valid:
                if reason == "FORMAT":
                    logger.error(f"[INVALID TIME FORMAT] user_email={user_email} | row={row}")
                elif reason == "DURATION":
                    logger.error(f"[INVALID DURATION] user_email={user_email} | row={row}")
                continue

            # 4. Resolve video / segment IDs
            video_id      = extract_video_id(video_url)
            videoseg_id   = build_videoseg_id(video_id, ai_mention_timestamp)

            # 5. Ensure TCU exists
            if not insert_tcu_if_not_exists(tcu_id, videoseg_id, row, cursor, user_email, logger):
                logger.error(f"[FAILED TO INSERT TCU] user_email={user_email} | TCU={tcu_id} | row={row}")
                continue

            # 6. Insert annotation
            annotation_id = f"{tcu_id}_{user_email}"
            if not insert_annotation(annotation_id, tcu_id, user_email, row, annotationtype, cursor, logger):
                logger.error(f"[FAILED TO INSERT ANNOTATION] user_email={user_email} | TCU={tcu_id} | row={row}")
                continue

            annotation_count += 1

    conn.commit()
    conn.close()
    logger.info(f"[SUCCESS] Finished processing {file_path} for user_email={user_email}, added {annotation_count} annotations to DB.")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# get_unannotated_tcus
# ─────────────────────────────────────────────────────────────────────────────

def get_unannotated_tcus(user_email: str, conn, logger) -> list:
    """Return full rows of unannotated TCUs for a given user."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT
                vs.original_row_number, vs.video_urlID, vs.meeting_date,
                vs.ai_mention_timestamp, vs.segment_start, vs.segment_end,
                vs.segment_transcript,
                t.TCUID, t.tcu_transcript, t.tcu_start, t.tcu_end,
                NULL AS speaker_role, NULL AS speaker_gender, NULL AS stance,
                NULL AS vocal_tone,   NULL AS facial_expression, NULL AS coder_notes,
                vs.State, vs.County, t.tcu_adder_email
            FROM TCU t
            JOIN VideoSegment vs ON t.VIDEOSEGID = vs.ID
            WHERE NOT EXISTS (
                SELECT 1 FROM Annotation a
                WHERE a.TCUID = t.TCUID AND a.Email = ?
            )
            ORDER BY vs.original_row_number, t.TCUID;
            """,
            (user_email,),
        )
        rows = []
        for r in cursor.fetchall():
            row_list = list(r)
            for k in range(11, 17):
                if row_list[k] is None:
                    row_list[k] = ""
            rows.append(row_list)
        return rows
    except sqlite3.Error as e:
        logger.warning(f"[QUERY ERROR] user_email={user_email} | {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# get_alias_from_email
# ─────────────────────────────────────────────────────────────────────────────

def get_alias_from_email(email: str, conn, logger) -> tuple[str | None, str | None]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT Alias, PairEmail FROM "User" WHERE Email = ?',
            (email,),
        )
        result = cursor.fetchone()
        if result is None:
            raise ValueError(f"No user found for email: {email}")
        return result[0], result[1]
    except sqlite3.Error as e:
        logger.error(f"[DB ERROR - Get Alias] email={email} | {e}")
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
# create_file_if_not_exists
# ─────────────────────────────────────────────────────────────────────────────

def create_file_if_not_exists(path: str, logger) -> bool:
    try:
        if os.path.exists(path):
            return True

        logger.info(f"[INFO] No existing file found at {path}")
        logger.info(f"[INFO] Creating new file at {path}")

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "original_row_number", "video_url", "meeting_date",
                "ai_mention_timestamp", "segment_start", "segment_end",
                "segment_transcript", "tcu_id", "tcu_transcript",
                "tcu_start", "tcu_end", "speaker_role", "speaker_gender",
                "stance", "vocal_tone", "facial_expression", "coder_notes",
            ])
        return True
    except Exception as e:
        logger.error(f"[FILE CREATE ERROR] path={path} | {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# get_existing_tcuids_for_file
# ─────────────────────────────────────────────────────────────────────────────

def get_existing_tcuids_for_file(file_path: str, user_email: str, logger, start_row: int = 2) -> set | None:
    existing_ids = set()
    create_file_if_not_exists(file_path, logger)
    try:
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader, start=1):
                if i < start_row:
                    continue
                existing_ids.add(row[getIndex("tcu_id", "tcucsv")])
        return existing_ids
    except Exception as e:
        logger.error(f"[READ EXISTING ERROR] user_email={user_email} file={file_path} | {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# export_missing_tcus
# ─────────────────────────────────────────────────────────────────────────────

def export_missing_tcus(
    user_email: str,
    conn,
    rows: list,
    logger,
    output_sub: str = "annotation-human/version2",
) -> bool:
    try:
        if not rows:
            logger.info(f"[INFO] No missing TCUs for user_email={user_email}")
            return True

        alias, pairemail = get_alias_from_email(user_email, conn, logger)

        if alias is None or pairemail is None:
            logger.error(f"[ERROR] Could not retrieve alias/pair email for user_email={user_email}")
            return False

        user_dir = os.path.join(output_sub, alias)

        # ── No-pair branch ────────────────────────────────────────────────
        if pairemail == "" or pairemail is None:
            logger.info(f"[INFO] No pair email for user_email={user_email}, skipping IRR export")

            combined_output_file    = os.path.join(user_dir, "combined_all.csv")
            existing_tcuids_combined = get_existing_tcuids_for_file(combined_output_file, user_email, logger)

            if existing_tcuids_combined is None:
                logger.error(f"[ERROR] Could not read existing TCUIDs for user_email={user_email}")
                return False

            with open(combined_output_file, "a", newline="", encoding="utf-8") as f_combined:
                writer_combined = csv.writer(f_combined)
                new_combined = 0
                for row in rows:
                    tcu_id = row[getIndex("tcu_id", "tcucsv")]
                    if tcu_id not in existing_tcuids_combined:
                        writer_combined.writerow(row)
                        existing_tcuids_combined.add(tcu_id)
                        new_combined += 1

            logger.info(f"[SUCCESS] user_email={user_email} | appended {new_combined} new complete TCUs")
            return True

        # ── Paired branch ─────────────────────────────────────────────────
        pairs_alias, _ = get_alias_from_email(pairemail, conn, logger)
        os.makedirs(user_dir, exist_ok=True)

        combined_output_file = os.path.join(user_dir, "combined_all.csv")
        pair_output_file     = os.path.join(user_dir, f"{pairs_alias}_irr.csv")

        existing_tcuids_combined = get_existing_tcuids_for_file(combined_output_file, user_email, logger)
        existing_tcuids_irr      = get_existing_tcuids_for_file(pair_output_file,     user_email, logger)

        if existing_tcuids_combined is None:
            logger.error(f"[ERROR] Could not read existing combined TCUIDs for user_email={user_email}")
            return False
        if existing_tcuids_irr is None:
            logger.error(f"[ERROR] Could not read existing IRR TCUIDs for user_email={user_email}")
            return False

        with (
            open(combined_output_file, "a", newline="", encoding="utf-8") as f_combined,
            open(pair_output_file,     "a", newline="", encoding="utf-8") as f_pair,
        ):
            writer_combined = csv.writer(f_combined)
            writer_pair     = csv.writer(f_pair)
            new_combined = new_pair = 0

            for row in rows:
                tcu_id          = row[getIndex("tcu_id",          "tcucsv")]
                tcu_adder_email = row[getIndex("tcu_adder_email", "tcucsv")]

                if tcu_id not in existing_tcuids_combined:
                    writer_combined.writerow(row)
                    existing_tcuids_combined.add(tcu_id)
                    new_combined += 1

                if tcu_adder_email == pairemail and tcu_id not in existing_tcuids_irr:
                    writer_pair.writerow(row)
                    existing_tcuids_irr.add(tcu_id)
                    new_pair += 1

        logger.info(f"[SUCCESS] user_email={user_email} | appended {new_combined} new complete TCUs")
        logger.info(f"[SUCCESS] user_email={user_email} | appended {new_pair} new IRR TCUs")
        return True

    except Exception as e:
        logger.error(f"[EXPORT ERROR] user_email={user_email} | {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# distribute_files_to_user
# ─────────────────────────────────────────────────────────────────────────────

def distribute_files_to_user(
    user_email: str,
    alias: str,
    DB_PATH: str,
    output_sub: str = "annotation-human/version2",
) -> bool:
    logger = get_logger(user_email, alias)
    conn   = sqlite3.connect(DB_PATH)
    try:
        rows = get_unannotated_tcus(user_email, conn, logger)
        export_missing_tcus(user_email, conn, rows, logger, output_sub)
        conn.close()
        return True
    except Exception as e:
        logger.error(f"[DISTRIBUTION ERROR] user_email={user_email} | {e}")
        conn.close()
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    DB_PATH = "db/annotation.db"

    users = [
        {"email": "kkz5193@psu.edu", "alias": "Kelly"},
        {"email": "xzx5141@psu.edu", "alias": "Xinyu"},
        {"email": "sks7267@psu.edu", "alias": "Swara"},
        {"email": "lxb5609@psu.edu", "alias": "Luke"},
        {"email": "jpg6390@psu.edu", "alias": "James"},
    ]

    for user in users:
        process_csv(
            user["email"],
            user["alias"],
            f"annotation-human/version2/{user['alias']}/{user['alias']}_annotation_file.csv",
            DB_PATH,
        )

    for user in users:
        distribute_files_to_user(user["email"], user["alias"], DB_PATH)


"""Todo:
    set up globus roar to onedrive
    formatting rule
"""