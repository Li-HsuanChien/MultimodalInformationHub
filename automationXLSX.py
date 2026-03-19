import os
import sqlite3
import csv
from helperfunctions import time_to_seconds, extract_video_id, build_videoseg_id, getItem, getindex, getRequiredFields
DB_PATH = "db/annotation.db"

conn = sqlite3.connect(DB_PATH)


def check_annotation_type(user_email, row):
    if user_email == "" or row is None:
        print(f"[MISSING USER EMAIL] or row={row}")
        return "None"
    for annotationtypeOption in ["irr", "common"]:
        annotationtype = "None"

        if annotationtypeOption == "irr" and user_email == "lxb5609@psu.edu":
            continue  # Luke does not have IRR fields, so skip IRR check for him

        for field in getRequiredFields(user_email, annotationtypeOption):
            # print(f"Checking field '{field}' for user '{user_email}' and annotation type '{annotationtypeOption}'")
            if (getItem(row, field) is not None )and (getItem(row, field) != "NA") and (getItem(row, field) != ""):
                annotationtype = annotationtypeOption
                break
        if annotationtype != "None":
            break
                

    return annotationtype

def check_duplicate_annotation(user_email, tcu_id, cursor):
    try:
        cursor.execute("""
            SELECT 1 FROM Annotation
            WHERE Email = ? AND TCUID = ?
        """, (user_email, tcu_id))

        if cursor.fetchone():
            return False
        return True
    except sqlite3.Error as e:
        print(f"[DB ERROR - Annotation CHECK] {user_email} | TCU={tcu_id} | {e}")
        return False
    
def validate_duration(user_email, row):
    start_sec = time_to_seconds(getItem(row, "tcu_start"))
    end_sec = time_to_seconds(getItem(row, "tcu_end"))

    if start_sec is None or end_sec is None:
        print(f"[INVALID TIME FORMAT] {user_email} | row={row}")
        return False

    duration = end_sec - start_sec

    if duration <= 0 or duration > 60:
        print(f"[INVALID DURATION] {user_email} | duration={duration} | row={row}")
        return False
    return True

def insert_tcu_if_not_exists(tcu_id, videoseg_id, row, cursor, user_email):
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO TCU (
                TCUID, VIDEOSEGID,
                tcu_start, tcu_end, tcu_transcript, video_saved, audio_saved, frames_saved
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tcu_id,
            videoseg_id,
            getItem(row, "tcu_start"),
            getItem(row, "tcu_end"),
            getItem(row, "tcu_transcript"),
            False,  # video_saved
            False,  # audio_saved   
            False   # frames_saved
        ))
        return True
    except sqlite3.Error as e:
        print(f"[DB ERROR - TCU INSERT] {user_email} | TCU={tcu_id} | {e}")
        return False
    
def insert_annotation(annotation_id, tcu_id, user_email, row, annotationtype, cursor):
    try:
        cursor.execute("""
            INSERT INTO Annotation (
                AnnotationID,
                TCUID,
                Email,
                speaker_gender,
                stance,
                vocal_tone,
                facial_expression,
                coder_notes,
                annotationtype       
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            annotation_id,
            tcu_id,
            user_email,
            getItem(row, "speaker_gender"),
            getItem(row, "stance"),
            getItem(row, "vocal_tone"),
            getItem(row, "facial_expression"),
            getItem(row, "coder_notes"),
            annotationtype
        ))

    except sqlite3.IntegrityError as e:
        print(f"[DB INTEGRITY ERROR] {user_email} | TCU={tcu_id} | {e}")

    except sqlite3.Error as e:
        print(f"[DB ERROR] {user_email} | TCU={tcu_id} | {e}")

def process_csv(file_path, user_email, conn, start_row=25):
    cursor = conn.cursor()
    
    with open(file_path, newline='', encoding='cp1252') as f:
        reader = csv.reader(f)
        
        #check annotation type based on required fields for the user

        
        for i, row in enumerate(reader, start=1):
            if i < start_row:
                continue
            
            # 1. Check Annotation
            # note for IRR 

            annotationtype = check_annotation_type(user_email, row)
            
            if annotationtype == "None":
                print(f"[MISSING ANNOTATION] {user_email} | row={row}")
                continue
          
            tcu_id = getItem(row, "tcu_id")

            # 2. Check duplicate Annotation (Email, TCUID)
            
            if not check_duplicate_annotation(user_email, tcu_id, cursor):
                print(f"[DUPLICATE ANNOTATION] {user_email} | TCU={tcu_id} | row={row}")
                continue
            
            # 3. Validate duration (0 < duration <= 60)

            if not validate_duration(user_email, row):
                continue

            # 4. Ensure VideoSegment exists Ignored since we populate videos first now

            video_id = extract_video_id(getItem(row, "video_url"))
            mention_timestamp = getItem(row, "ai_mention_timestamp")
            videoseg_id = build_videoseg_id(video_id, mention_timestamp)
            # try: 
            #     cursor.execute("""
            #         INSERT OR IGNORE INTO VideoSegment (
            #             ID,
            #             video_urlID,
            #             meeting_date,
            #             State,
            #             County,
            #             original_row_number,
            #             ai_mention_timestamp,
            #             segment_start,
            #             segment_end,
            #             segment_transcript
            #         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            #     """, (
            #         videoseg_id,
            #         video_id,
            #         getItem(row, "meeting_date"),
            #         getItem(row, "State"),
            #         getItem(row, "County"),
            #         int(getItem(row, "original_row_number")),
            #         getItem(row, "ai_mention_timestamp"),
            #         getItem(row, "actual_segment_start_time"),
            #         getItem(row, "actual_segment_end_time"),
            #         getItem(row, "transcript_for_actual_segment")
            #     ))
            # except sqlite3.Error as e:
            #     print(f"[DB ERROR - VideoSegment INSERT] {user_email} | VideoSegment={videoseg_id} | {e}")
            #     continue
            
            # 5. Ensure TCU exists
            if not insert_tcu_if_not_exists(tcu_id, videoseg_id, row, cursor, user_email):
                continue
            # 6. Insert Annotation (ONLY annotation fields)
            annotation_id = f"{tcu_id}_{user_email}"
            insert_annotation(annotation_id, tcu_id, user_email, row, annotationtype, cursor)            
    conn.commit()


def get_unannotated_tcus(user_email, conn):
    """
    Returns full rows of unannotated TCUs for a given user,
    including associated VideoSegment info.
    """
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT
                vs.original_row_number,
                vs.video_urlID,
                vs.meeting_date,
                vs.ai_mention_timestamp,
                vs.segment_start,
                vs.segment_end,
                vs.segment_transcript,
                t.TCUID,
                t.tcu_transcript,
                t.tcu_start,
                t.tcu_end,
                NULL AS speaker_role,
                NULL AS speaker_gender,
                NULL AS stance,
                NULL AS vocal_tone,
                NULL AS facial_expression,
                NULL AS coder_notes,
                vs.State,
                vs.County
            FROM TCU t
            JOIN VideoSegment vs ON t.VIDEOSEGID = vs.ID
            LEFT JOIN Annotation a
                ON t.TCUID = a.TCUID
                AND a.Email = ?
                AND t.annotationtype != 'irr'
            WHERE a.TCUID IS NULL
            ORDER BY vs.original_row_number, t.TCUID
        """, (user_email,))

        rows = []
        for r in cursor.fetchall():
            row_list = list(r)
            # replace None with empty string for annotation fields
            for i in range(11, 17):
                if row_list[i] is None:
                    row_list[i] = ""
            rows.append(row_list)

        return rows

    except sqlite3.Error as e:
        print(f"[QUERY ERROR] user={user_email} | {e}")
        return []
    
def get_alias_from_email(email, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT Alias, PairEmail
            FROM "User"
            WHERE Email = ?
        """, (email,))

        result = cursor.fetchone()

        if result is None:
            raise ValueError(f"No user found for email: {email}")

        return result[0], result[1]
    except sqlite3.Error as e:
        print(f"[DB ERROR - Get Alias] email={email} | {e}")
        return None, None
def create_file_if_not_exists(path):
    file_exists = os.path.exists(path)
    if not file_exists:
        print("No existing " + path + " found")
        print("Creating new " + path)
        with open(path, "w", newline='', encoding="cp1252") as f:
            writer = csv.writer(f)
            writer.writerow([
                "original_row_number", "video_url", "meeting_date",
                "ai_mention_timestamp", "segment_start", "segment_end",
                "segment_transcript", "tcu_id", "tcu_transcript",
                "tcu_start", "tcu_end", "speaker_role", "speaker_gender",
                "stance", "vocal_tone", "facial_expression", "coder_notes"
            ])

def get_existing_tcuids_for_file(file_path, user_email):
    exisiting_id = set()
    create_file_if_not_exists(file_path)
    try:
        with open(file_path, newline='', encoding="cp1252") as f:
            reader = csv.reader(f)
            for row in reader:
                exisiting_id.add(row[getindex("tcu_id")])
        return exisiting_id
    except Exception as e:
        print(f"[READ EXISTING ERROR] user={user_email} file={file_path} | {e}")
        return None
    
def export_missing_tcus(user_email, conn, output_sub = "annotation-human/version2"):
    try:
        rows = get_unannotated_tcus(user_email, conn)

        
        if not rows:
            print(f"[INFO] No missing TCUs for {user_email}")
            return

        alias, pairemail = get_alias_from_email(user_email, conn)
        if alias is None or pairemail is None:
            print(f"[ERROR] Could not retrieve alias/pair email for {user_email}")
            return

        user_dir = os.path.join(output_sub, alias)
        os.makedirs(user_dir, exist_ok=True)

        combined_output_file = os.path.join(user_dir, "combined_all.csv")
        pair_output_file = os.path.join(user_dir, f"{pairemail}_irr.csv")

        existing_tcuids_combined = get_existing_tcuids_for_file(combined_output_file, user_email)
        existing_tcuids_irr = get_existing_tcuids_for_file(pair_output_file, user_email)

        if existing_tcuids_combined is None or existing_tcuids_irr is None:
            print(f"[ERROR] Could not read existing TCUIDs for {user_email}")
            return
        
        
        # 2. Append only NEW rows
        with open(combined_output_file, "a", newline='', encoding="cp1252") as f_combined, \
     open(pair_output_file, "a", newline='', encoding="cp1252") as f_pair:

            writer_combined = csv.writer(f_combined)
            writer_pair = csv.writer(f_pair)

            new_combined = 0
            new_pair = 0

            for row in rows:
                tcu_id = row[getindex("tcu_id")]
                email = row[getindex("email")]

                # write to combined
                if tcu_id not in existing_tcuids_combined:
                    writer_combined.writerow(row)
                    existing_tcuids_combined.add(tcu_id)
                    new_combined += 1

                # write to pair file
                if email == pairemail and tcu_id not in existing_tcuids_irr:
                    writer_pair.writerow(row)
                    existing_tcuids_irr.add(tcu_id)
                    new_pair += 1

        print(f"[INFO] {user_email} | appended {new_combined} new complete TCUs")
        print(f"[INFO] {user_email} | appended {new_pair} new IRR TCUs")

    except Exception as e:
        print(f"[EXPORT ERROR] user={user_email} | {e}")



"""Todo: 
    Test each module function
    activate sql db
    implement csv file to data branch
    implement data to sql branch
    implement main function and do integration test
    set up cloud retrieval of csv and write
    youtube link to id
"""