import os
import sqlite3
import csv
 
from helperfunctions import getIndex, time_to_seconds, extract_video_id, build_videoseg_id, getItem, getIndex, getRequiredFields


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
            if (getItem(row, field, "tcucsv") is not None )and (getItem(row, field, "tcucsv") != "NA") and (getItem(row, field, "tcucsv") != ""):
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
    
def validate_duration(row):
    start_sec = time_to_seconds(getItem(row, "tcu_start", "tcucsv"))
    end_sec = time_to_seconds(getItem(row, "tcu_end", "tcucsv"))
    if start_sec is None or end_sec is None:
        return False, "FORMAT"

    duration = end_sec - start_sec

    if duration <= 0 or duration > 60:
        print(f"[INVALID DURATION] | duration={duration} | row={row}")
        return False, "DURATION"
    return True, "None"

def insert_tcu_if_not_exists(tcu_id, videoseg_id, row, cursor, user_email):
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO TCU (
                TCUID, VIDEOSEGID,
                tcu_start, tcu_end, tcu_transcript, tcu_adder_email, video_saved, audio_saved, frames_saved
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tcu_id,
            videoseg_id,
            getItem(row, "tcu_start", "tcucsv"),
            getItem(row, "tcu_end", "tcucsv"),
            getItem(row, "tcu_transcript", "tcucsv"),
            user_email,
            False,  # video_saved
            False,  # audio_saved   
            False   # frames_saved
        ))
        return True
    except sqlite3.Error as e:
        print(f"[DB ERROR - TCU INSERT]  | TCU={tcu_id} | {e}")
        return False
    
def insert_annotation(annotation_id, tcu_id, user_email, row, annotationtype, cursor):
    try:
        cursor.execute("""
            INSERT INTO Annotation (
                AnnotationID,
                TCUID,
                Email,
                speaker_role,
                speaker_gender,
                stance,
                vocal_tone,
                facial_expression,
                coder_notes,
                annotationtype       
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            annotation_id,
            tcu_id,
            user_email,
            getItem(row, "speaker_role", "tcucsv"),
            getItem(row, "speaker_gender", "tcucsv"),
            getItem(row, "stance", "tcucsv"),
            getItem(row, "vocal_tone", "tcucsv"),
            getItem(row, "facial_expression", "tcucsv"),
            getItem(row, "coder_notes", "tcucsv"),
            annotationtype
        ))
        return True

    except sqlite3.IntegrityError as e:
        print(f"[DB INTEGRITY ERROR] {user_email} | TCU={tcu_id} | {e}")
        return False

    except sqlite3.Error as e:
        print(f"[DB ERROR] {user_email} | TCU={tcu_id} | {e}")
        return False

def process_csv(user_email, file_path, DB_PATH):
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found for {user_email}: {file_path}")
        return False
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    started = False
    # try:
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        annotation_count = 0
        
        
        #check annotation type based on required fields for the user
        for i, row in enumerate(reader, start=1):
            if row[0] == "original_row_number":
                started = True
                continue  # skip header
            if not started:
                continue
            
            for i in range(11):
                if row[i] is None or row[i] == "":
                    print(f"[MISSING REQUIRED FIELD] {user_email} | row={row}")
                    continue
            
            
            # 1. Check Annotation 
            # note for IRR 

            annotationtype = check_annotation_type(user_email, row)
            
            if annotationtype == "None":
                # print(f"[MISSING ANNOTATION] {user_email} | row={row}")
                continue
                
            tcu_id = getItem(row, "tcu_id", "tcucsv")

            # 2. Check duplicate Annotation (Email, TCUID)
            
            if not check_duplicate_annotation(user_email, tcu_id, cursor):
                # print(f"[DUPLICATE ANNOTATION] {user_email} | TCU={tcu_id} | row={row}")
                continue
            
            # 3. Validate duration (0 < duration <= 60)
            duration_valid, reason = validate_duration(row)
            if not duration_valid:
                if reason == "FORMAT":
                    print(f"[INVALID TIME FORMAT] {user_email} | row={row}")
                elif reason == "DURATION":
                    print(f"[INVALID DURATION] {user_email} | row={row}")
                # TODO: automate invalid input notification to user
                continue

            # 4. Ensure VideoSegment exists Ignored since we populate videos first now

            video_id = extract_video_id(getItem(row, "video_url", "tcucsv"))
            mention_timestamp = getItem(row, "ai_mention_timestamp", "tcucsv")
            videoseg_id = build_videoseg_id(video_id, mention_timestamp)
        
            
            # 4. Ensure TCU exists
            if not insert_tcu_if_not_exists(tcu_id, videoseg_id, row, cursor, user_email):
                print(f"[FAILED TO INSERT TCU] {user_email} | TCU={tcu_id} | row={row}")
                continue
            # 5. Insert Annotation (ONLY annotation fields)
            
            annotation_id = f"{tcu_id}_{user_email}"
            if not insert_annotation(annotation_id, tcu_id, user_email, row, annotationtype, cursor):
                
                print(f"[FAILED TO INSERT ANNOTATION] {user_email} | TCU={tcu_id} | row={row}")
                continue
            
        conn.commit()
        print(f"[SUCCESS] Finished processing {file_path} for {user_email}, added {annotation_count} annotations to DB.")
        conn.close()
        return True
    # except Exception as e:
    #     print(f"[PROCESSING ERROR] {user_email} | file={file_path} | {e}")
    #     conn.close()
    #     return False

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
                vs.County,
                t.tcu_adder_email
            FROM TCU t
            JOIN VideoSegment vs ON t.VIDEOSEGID = vs.ID
            WHERE NOT EXISTS (
                SELECT 1 
                FROM Annotation a 
                WHERE a.TCUID = t.TCUID 
                AND a.Email = ?
            )
            ORDER BY vs.original_row_number, t.TCUID;
        """, (user_email,))
        rows = []
        for r in cursor.fetchall():
            row_list = list(r)
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
    try:
        if os.path.exists(path):
            return True  

        print(f"No existing {path} found")
        print(f"Creating new {path}")

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "original_row_number", "video_url", "meeting_date",
                "ai_mention_timestamp", "segment_start", "segment_end",
                "segment_transcript", "tcu_id", "tcu_transcript",
                "tcu_start", "tcu_end", "speaker_role", "speaker_gender",
                "stance", "vocal_tone", "facial_expression", "coder_notes"
            ])

        return True  # successfully created

    except Exception as e:
        print(f"[FILE CREATE ERROR] path={path} | {e}")
        return False
    
def get_existing_tcuids_for_file(file_path, user_email,  start_row=2):
    exisiting_id = set()
    create_file_if_not_exists(file_path)
    try:
        with open(file_path, newline='', encoding="utf-8") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader, start=1):
                if i < start_row:  
                    continue
                exisiting_id.add(row[getIndex("tcu_id", "tcucsv")])
        return exisiting_id
    except Exception as e:
        print(f"[READ EXISTING ERROR] user={user_email} file={file_path} | {e}")
        return None
    
# rows = get_unannotated_tcus(user_email, conn)

def export_missing_tcus(user_email, conn, rows, output_sub = "annotation-human/version2"):
    try:        
        if not rows:
            print(f"[INFO] No missing TCUs for {user_email}")
            return

        alias, pairemail = get_alias_from_email(user_email, conn)
        user_dir = os.path.join(output_sub, alias)
        
        if alias is None or pairemail is None:
            print(f"[ERROR] Could not retrieve alias/pair email for {user_email}")
            return
        if pairemail == "" or pairemail is None:
            print(f"[INFO] No pair email for {user_email}, skipping IRR export")
            combined_output_file = os.path.join(user_dir, "combined_all.csv")
            existing_tcuids_combined = get_existing_tcuids_for_file(combined_output_file, user_email)
            if existing_tcuids_combined is None:
                print(f"[ERROR] Could not read existing TCUIDs for {user_email}")
                return
            with open(combined_output_file, "a", newline='', encoding="utf-8") as f_combined:
                writer_combined = csv.writer(f_combined)
                new_combined = 0
                for row in rows:
                    tcu_id = row[getIndex("tcu_id", "tcucsv")]
                    if tcu_id not in existing_tcuids_combined:
                        writer_combined.writerow(row)
                        existing_tcuids_combined.add(tcu_id)
                        new_combined += 1
            print(f"[SUCCESS] {user_email} | appended {new_combined} new complete TCUs")
            return True
        
        pairs_alias, _ = get_alias_from_email(pairemail, conn)
        os.makedirs(user_dir, exist_ok=True)

        combined_output_file = os.path.join(user_dir, "combined_all.csv")
        pair_output_file = os.path.join(user_dir, f"{pairs_alias}_irr.csv")

        existing_tcuids_combined = get_existing_tcuids_for_file(combined_output_file, user_email)
        existing_tcuids_irr = get_existing_tcuids_for_file(pair_output_file, user_email)

        if existing_tcuids_combined is None:
            print(f"[ERROR] Could not read existing combined TCUIDs for {user_email}")
            return
           
        if existing_tcuids_irr is None:
            print(f"[ERROR] Could not read existing IRR TCUIDs for {user_email}")
            return
        
        # 2. Append only NEW rows
        with open(combined_output_file, "a", newline='', encoding="utf-8") as f_combined, \
     open(pair_output_file, "a", newline='', encoding="utf-8") as f_pair:

            writer_combined = csv.writer(f_combined)
            writer_pair = csv.writer(f_pair)

            new_combined = 0
            new_pair = 0

            for row in rows:
                tcu_id = row[getIndex("tcu_id", "tcucsv")]
                tcu_adder_email = row[getIndex("tcu_adder_email", "tcucsv")]

                # write to combined
                if tcu_id not in existing_tcuids_combined:
                    writer_combined.writerow(row)
                    existing_tcuids_combined.add(tcu_id)
                    new_combined += 1

                # write to pair file
                if tcu_adder_email == pairemail and tcu_id not in existing_tcuids_irr:
                    writer_pair.writerow(row)
                    existing_tcuids_irr.add(tcu_id)
                    new_pair += 1

        print(f"[SUCCESS] {user_email} | appended {new_combined} new complete TCUs")
        print(f"[SUCCESS] {user_email} | appended {new_pair} new IRR TCUs")
        return True

    except Exception as e:
        print(f"[EXPORT ERROR] user={user_email} | {e}")
        return False
        

def distribute_files_to_user(user_email, DB_PATH, output_sub = "annotation-human/version2"):
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = get_unannotated_tcus(user_email, conn)
        export_missing_tcus(user_email, conn, rows, output_sub)
        conn.close()
        return True
    except Exception as e:
        print(f"[DISTRIBUTION ERROR] user={user_email} | {e}")
        conn.close()
        return False
    

if __name__ == "__main__":
    ## retrieve all user annotations and it goes to db
    ## iterate through each user and populate files for each user based on their unannotated TCUs in db
    DB_PATH = "db/annotation.db"
    users = [   {"email": "kkz5193@psu.edu", "alias": "Kelly", "pair_email": "xzx5141@psu.edu"},
            {"email": "xzx5141@psu.edu", "alias": "Xinyu", "pair_email": "sks7267@psu.edu"},
            {"email": "sks7267@psu.edu", "alias": "Swara", "pair_email": "kkz5193@psu.edu"},
            {"email": "lxb5609@psu.edu", "alias": "Luke", "pair_email": ""},
            {"email": "jpg6390@psu.edu", "alias": "James", "pair_email": ""}
    ]
    for user in users:
       process_csv(user["email"], f"annotation-human/version2/{user['alias']}/{user['alias']}_annotation_file.csv", DB_PATH)
    
    for user in users:
        distribute_files_to_user(user["email"], DB_PATH)
    


"""Todo: 
    set up globus roar to onedrive
    
"""