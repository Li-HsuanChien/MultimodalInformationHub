import os
import sqlite3
import csv
from helperfunctions import time_to_seconds, extract_video_id, build_videoseg_id, getItem, getindex, getRequiredFields
DB_PATH = "annotation.db"

conn = sqlite3.connect(DB_PATH)


def process_csv(file_path, user_email, conn, start_row=25):
    cursor = conn.cursor()
    """row structure: 
            original_row_number	
            video_url	
            meeting_date
            ai_mention_timestamp
            segment_start
            segment_end
            segment_transcript
            tcu_id
            tcu_transcript
            tcu_start
            tcu_end
            speaker_role
            speaker_gender
            stance
            vocal_tone
            facial_expression"""

    
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        

        for i, row in enumerate(reader, start=1):
            if i < start_row:
                continue
            
            # 1. Check Annotation
            for field in getRequiredFields(user_email):
                if getItem(row, field) is None:
                    print(f"[MISSING FIELD] {user_email} | field={field} | row={row}")
                    continue
          
            tcu_id = getItem(row, "tcu_id")
            # 2. Check duplicate Annotation (Email, TCUID)
            try:
                    
                cursor.execute("""
                    SELECT 1 FROM Annotation
                    WHERE Email = ? AND TCUID = ?
                """, (user_email, tcu_id))

                if cursor.fetchone():
                    continue
            except sqlite3.Error as e:
                print(f"[DB ERROR - Annotation CHECK] {user_email} | TCU={tcu_id} | {e}")
                continue
             
            
            # 3. Validate duration (0 < duration <= 60)

            start_sec = time_to_seconds(getItem(row, "tcu_start"))
            end_sec = time_to_seconds(getItem(row, "tcu_end"))

            if start_sec is None or end_sec is None:
                print(f"[INVALID TIME FORMAT] {user_email} | row={row}")
                continue

            duration = end_sec - start_sec

            if duration <= 0 or duration > 60:
                print(f"[INVALID DURATION] {user_email} | duration={duration} | row={row}")
                continue

            # 4. Ensure VideoSegment exists

            video_id = extract_video_id(getItem(row, "video_url"))
            mention_timestamp = getItem(row, "ai_mention_timestamp")
            videoseg_id = build_videoseg_id(video_id, mention_timestamp)
            try: 
                cursor.execute("""
                    INSERT OR IGNORE INTO VideoSegment (
                        ID,
                        video_urlID,
                        meeting_date,
                        State,
                        County,
                        original_row_number,
                        ai_mention_timestamp,
                        segment_start,
                        segment_end,
                        segment_transcript
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    videoseg_id,
                    video_id,
                    getItem(row, "meeting_date"),
                    getItem(row, "State"),
                    getItem(row, "County"),
                    int(getItem(row, "original_row_number")),
                    getItem(row, "ai_mention_timestamp"),
                    getItem(row, "actual_segment_start_time"),
                    getItem(row, "actual_segment_end_time"),
                    getItem(row, "transcript_for_actual_segment")
                ))
            except sqlite3.Error as e:
                print(f"[DB ERROR - VideoSegment INSERT] {user_email} | VideoSegment={videoseg_id} | {e}")
                continue
            
            # 5. Ensure TCU exists
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO TCU (
                        TCUID, VIDEOSEGID,
                        tcu_start, tcu_end, tcu_transcript
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    tcu_id,
                    videoseg_id,
                    getItem(row, "tcu_start"),
                    getItem(row, "tcu_end"),
                    getItem(row, "tcu_transcript")
                ))

            except sqlite3.Error as e:
                print(f"[DB ERROR - TCU INSERT] {user_email} | TCU={tcu_id} | {e}")
                continue
            # 6. Insert Annotation (ONLY annotation fields)
            
            annotation_id = f"{tcu_id}_{user_email}"
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
                        coder_notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    annotation_id,
                    tcu_id,
                    user_email,
                    getItem(row, "speaker_gender"),
                    getItem(row, "stance"),
                    getItem(row, "vocal_tone"),
                    getItem(row, "facial_expression"),
                    getItem(row, "coder_notes")
                ))
  
            except sqlite3.IntegrityError as e:
                print(f"[DB INTEGRITY ERROR] {user_email} | TCU={tcu_id} | {e}")

            except sqlite3.Error as e:
                print(f"[DB ERROR] {user_email} | TCU={tcu_id} | {e}")
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

    cursor.execute("""
        SELECT Alias
        FROM "User"
        WHERE Email = ?
    """, (email,))

    result = cursor.fetchone()

    if result is None:
        raise ValueError(f"No user found for email: {email}")

    return result[0]


import os
import csv

def export_missing_tcus(user_email, conn, output_sub = "annotation-human/version2"):
    try:
        rows = get_unannotated_tcus(user_email, conn)

        
        if not rows:
            print(f"[INFO] No missing TCUs for {user_email}")
            return

        alias = get_alias_from_email(user_email, conn)
        user_dir = os.path.join(output_sub, alias)
        os.makedirs(user_dir, exist_ok=True)

        combined_output_file = os.path.join(user_dir, "combined_all.csv")

        # 1. Load existing TCUIDs (ONLY ONCE)
        existing_tcuids = set()

        if os.path.exists(combined_output_file):
            try:
                with open(combined_output_file, newline='', encoding="utf-8") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        existing_tcuids.add(row[getindex("tcu_id")])
            except Exception as e:
                print(f"[READ EXISTING ERROR] user={user_email} | {e}")

        # 2. Append only NEW rows
        file_exists = os.path.exists(combined_output_file)
        if not file_exists:
            print("No existing combined_all.csv found")
            print("[EXPORT ERROR] No existing combined_all.csv found ")
        
        with open(combined_output_file, "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)

            new_count = 0

            for i, row in enumerate(rows, start=1):
                if i < 25:
                    continue
                tcu_id = row[getindex("tcu_id")] 

                if tcu_id in existing_tcuids:
                    continue  

                try:
                    writer.writerow(row)
                    existing_tcuids.add(tcu_id)
                    new_count += 1
                except Exception as e:
                    print(f"[CSV WRITE ERROR] user={user_email} | row={row} | {e}")

        print(f"[INFO] {user_email} | appended {new_count} new TCUs")

    except Exception as e:
        print(f"[EXPORT ERROR] user={user_email} | {e}")



