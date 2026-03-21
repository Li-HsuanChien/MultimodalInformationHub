import csv
import os
import sqlite3
from helperfunctions import extract_video_id, build_videoseg_id

DB_PATH = "db/annotation.db"
conn = sqlite3.connect(DB_PATH)



def read_csv_insert_videoseg_no_header(file_path = "annotation-human/version2/training_meetings_annotation_state&county.csv", conn=conn):
    cursor = conn.cursor()
    file_path = os.path.normpath(file_path)  
    with open(file_path, "r", encoding="cp1252") as f:
        reader = csv.reader(f)

        for i, row in enumerate(reader, start=1):
            if i < 2:  # start reading from row 2
                continue

            original_row_number = i
            video_url = row[0]
            state = row[1]
            county = row[2]
            meeting_date = row[3]
            ai_mention_timestamp = row[4]
            segment_start = row[5]
            segment_end = row[6]
            segment_transcript = row[7]
            

            video_id = extract_video_id(video_url)
            videoseg_id = build_videoseg_id(video_id, ai_mention_timestamp)
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO VideoSegment (
                        ID,
                        video_urlID,
                        meeting_date,
                        "State",
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
                    meeting_date,
                    state,
                    county,
                    int(original_row_number),
                    ai_mention_timestamp,
                    segment_start,
                    segment_end,
                    segment_transcript
                ))
            except sqlite3.Error as e:
                print(f"[DB ERROR - VideoSegment INSERT] row={row} | {e}")
                continue

    conn.commit()
    print("Video segments inserted successfully.")
    conn.close()

if __name__ == "__main__":
    read_csv_insert_videoseg_no_header(conn=conn)